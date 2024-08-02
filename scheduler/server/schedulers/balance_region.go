// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	maxStoreDownTime := cluster.GetMaxStoreDownTime()
	stores := []*core.StoreInfo{}
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < maxStoreDownTime {
			stores = append(stores, store)
		}
	}
	if len(stores) < 2 {
		return nil
	}
	//给store从小到大排序
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})
	var region *core.RegionInfo
	var originStore *core.StoreInfo
	for _, store := range stores {
		cluster.GetPendingRegionsWithLock(store.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})
		if region != nil {
			originStore = store
			break
		}
		cluster.GetFollowersWithLock(store.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})
		if region != nil {
			originStore = store
			break
		}
		cluster.GetLeadersWithLock(store.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})
		if region != nil {
			originStore = store
			break
		}
	}
	if region == nil {
		return nil
	}
	if len(region.GetPeers()) < cluster.GetMaxReplicas() {
		//没必要balance了,没达到最大副本数
		return nil
	}

	var targetStore *core.StoreInfo
	for i := len(stores) - 1; i >= 0; i-- {
		store := stores[i]
		if region.GetStorePeer(store.GetID()) == nil {
			targetStore = store
			break
		}
		//按size从小到大找store,如果一个store中包含所转移region的peer,那么略过.
		//因为同一个store中包含两个相同的peer是没有意义的
	}
	if targetStore == nil {
		return nil
	}
	//如果region在targetStore上的regionSize大于region在store上的regionSize
	diff := originStore.GetRegionSize() - targetStore.GetRegionSize()
	if diff > 2*region.GetApproximateSize() {
		peer, err := cluster.AllocPeer(targetStore.GetID())
		if err != nil {
			return nil
		}
		op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, originStore.GetID(), targetStore.GetID(), peer.GetId())
		if err != nil {
			return nil
		}
		return op
	}
	return nil
}
