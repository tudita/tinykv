package raftstore

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	//处理raft的ready()
	//初步判断就是调用2ac里实现的那些函数
	if d.stopped {
		return
	}
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()
		ApplySnapResult, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			return
		}
		if ApplySnapResult != nil {
			//prevRegion换成Region
			if !reflect.DeepEqual(ApplySnapResult.PrevRegion, ApplySnapResult.Region) {
				d.SetRegion(ApplySnapResult.Region)
				d.ctx.storeMeta.Lock()
				d.ctx.storeMeta.regions[ApplySnapResult.Region.Id] = ApplySnapResult.Region
				d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: ApplySnapResult.PrevRegion})
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: ApplySnapResult.Region})
				d.ctx.storeMeta.Unlock()
			}

		}
		if len(ready.Messages) != 0 {
			d.Send(d.ctx.trans, ready.Messages)
		}
		//执行待apply的log
		for _, entry := range ready.CommittedEntries {
			if entry.EntryType == eraftpb.EntryType_EntryNormal {
				d.applyEntry(&entry)
			} else {
				d.applyEntryConfChange(&entry)
			}

		}

		d.RaftGroup.Advance(ready)
	}
	// Your Code Here (2B).
}
func (d *peerMsgHandler) applyEntryConfChange(entry *eraftpb.Entry) {
	var confChange eraftpb.ConfChange
	err := confChange.Unmarshal(entry.Data)
	if err != nil {
		log.Panic(err)
		return
	}
	//confChange的Context还不知道干嘛的
	//用来存储raft_cmdpb.RaftCmdRequest的
	//TODO
	kvWb := new(engine_util.WriteBatch)
	switch confChange.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		d.execAddNode(entry, &confChange, kvWb)
	case eraftpb.ConfChangeType_RemoveNode:
		d.execRemoveNode(entry, &confChange, kvWb)
	}

	if d.stopped {
		return
	}
	// 写入状态
	err = kvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	if err != nil {
		log.Panic(err)
	}

	engines := d.peerStorage.Engines
	txn := engines.Kv.NewTransaction(false)
	regionId := d.peerStorage.Region().GetId()
	regionState := new(rspb.RegionLocalState)
	kvWb.MustWriteToDB(d.peerStorage.Engines.Kv)
	err = engine_util.GetMetaFromTxn(txn, meta.RegionStateKey(regionId), regionState)

	if err != nil {
		log.Panic(err)
	}
}

func (d *peerMsgHandler) applyEntry(entry *eraftpb.Entry) {
	d.peerStorage.applyState.AppliedIndex = entry.Index
	kvWb := new(engine_util.WriteBatch)
	if entry.EntryType == eraftpb.EntryType_EntryNormal {
		msg := &raft_cmdpb.RaftCmdRequest{}
		err := msg.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		if msg.Header != nil {
			from := msg.GetHeader().GetRegionEpoch()
			if from != nil {
				region := d.Region()
				if util.IsEpochStale(from, region.RegionEpoch) {
					resp := ErrResp(&util.ErrStaleCommand{})
					d.callBack(entry, resp, false)
					return
				}
			}
		}
		// AdminRequests
		if msg.AdminRequest != nil {
			switch msg.AdminRequest.CmdType {
			case raft_cmdpb.AdminCmdType_CompactLog:
				d.execCompactLog(entry, msg.AdminRequest, kvWb)
			case raft_cmdpb.AdminCmdType_Split:
				if msg.Header.RegionId != d.regionId {
					resp := ErrResp(&util.ErrRegionNotFound{RegionId: msg.Header.RegionId})
					d.callBack(entry, resp, false)
					return
				}
				//确保版本没有落后
				err := util.CheckRegionEpoch(msg, d.Region(), true)
				if err != nil {
					resp := ErrResp(err)
					d.callBack(entry, resp, false)
					return
				}
				//实现split
				d.execSplit(entry, msg.AdminRequest, kvWb)
			}

		}
		// CommonRequests
		if len(msg.Requests) > 0 {
			req := msg.Requests[0]
			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				d.execGet(entry, req)
			case raft_cmdpb.CmdType_Put:
				d.execPut(entry, req, kvWb)
			case raft_cmdpb.CmdType_Delete:
				d.execDelete(entry, req, kvWb)
			case raft_cmdpb.CmdType_Snap:
				d.execSnap(entry, req)
			}
		}
		if d.stopped {
			return
		}
		err = kvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
		}

		engines := d.peerStorage.Engines
		txn := engines.Kv.NewTransaction(false)
		regionId := d.peerStorage.Region().GetId()
		regionState := new(rspb.RegionLocalState)
		kvWb.MustWriteToDB(d.peerStorage.Engines.Kv)
		err = engine_util.GetMetaFromTxn(txn, meta.RegionStateKey(regionId), regionState)

		if err != nil {
			log.Panic(err)
		}
	}
}
func (d *peerMsgHandler) execGet(entry *eraftpb.Entry, req *raft_cmdpb.Request) {
	value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
	response := newCmdResp()
	response.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: value}}}
	d.callBack(entry, response, false)
}
func (d *peerMsgHandler) execPut(entry *eraftpb.Entry, req *raft_cmdpb.Request, kvWb *engine_util.WriteBatch) {
	kvWb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	d.SizeDiffHint += uint64(len(req.Put.Key) + len(req.Put.Value))
	response := newCmdResp()
	response.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}}}
	d.callBack(entry, response, false)
}
func (d *peerMsgHandler) execDelete(entry *eraftpb.Entry, req *raft_cmdpb.Request, kvWb *engine_util.WriteBatch) {
	kvWb.DeleteCF(req.Delete.Cf, req.Delete.Key)
	d.SizeDiffHint -= uint64(len(req.Delete.Key))
	response := newCmdResp()
	response.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}}}
	d.callBack(entry, response, false)
}
func (d *peerMsgHandler) execSnap(entry *eraftpb.Entry, msg *raft_cmdpb.Request) {
	response := newCmdResp()
	//必须复制一份，因为scan请求中会反复CheckKeyInRegion(key, region),而region是指针,直接传入split后会报错
	region := *d.Region()
	response.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: &region}}}
	d.callBack(entry, response, true)
}
func (d *peerMsgHandler) execCompactLog(entry *eraftpb.Entry, req *raft_cmdpb.AdminRequest, kvWb *engine_util.WriteBatch) {
	compactLog := req.GetCompactLog()
	if compactLog.CompactIndex >= d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index = compactLog.CompactIndex
		d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm
		err := kvWb.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
			return
		}
		d.ScheduleCompactLog(compactLog.CompactIndex)
	}
	response := newCmdResp()
	response.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_CompactLog, CompactLog: &raft_cmdpb.CompactLogResponse{}}
	d.callBack(entry, response, false)
}
func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}
func (d *peerMsgHandler) execSplit(entry *eraftpb.Entry, req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) {

	err := util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
	if err != nil {
		resp := ErrResp(err)
		d.callBack(entry, resp, false)
		return
	}
	if len(req.Split.NewPeerIds) != len(d.Region().Peers) {
		resp := ErrResp(errors.Errorf("length of NewPeerIds != length of Peers"))
		d.callBack(entry, resp, false)
		return
	}

	log.Infof("region %d peer %d begin to split", d.regionId, d.PeerId())

	leftRegion := d.Region()
	rightRegion := &metapb.Region{}
	util.CloneMsg(leftRegion, rightRegion)

	rightRegion.Id = req.Split.NewRegionId
	rightRegion.StartKey = req.Split.SplitKey

	for i := range rightRegion.Peers {
		rightRegion.Peers[i].Id = req.Split.NewPeerIds[i]
	}

	d.ctx.storeMeta.Lock()
	leftRegion.RegionEpoch.Version++
	rightRegion.RegionEpoch.Version++
	leftRegion.EndKey = req.Split.SplitKey
	meta.WriteRegionState(kvWB, leftRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, rightRegion, rspb.PeerState_Normal)
	d.ctx.storeMeta.regions[rightRegion.Id] = rightRegion
	d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: d.Region()})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})
	d.ctx.storeMeta.Unlock()

	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, rightRegion)
	if err != nil {
		log.Panic(err)
	}
	newPeer.peerStorage.SetRegion(rightRegion)
	d.ctx.router.register(newPeer)
	startMsg := message.Msg{
		RegionID: req.Split.NewRegionId,
		Type:     message.MsgTypeStart,
	}
	err = d.ctx.router.send(req.Split.NewRegionId, startMsg)
	if err != nil {
		log.Panic(err)
	}
	// clear region size
	d.SizeDiffHint = 0
	d.ApproximateSize = new(uint64)

	response := newCmdResp()
	response.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_Split, Split: &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{leftRegion, rightRegion}}}
	d.callBack(entry, response, false)
	d.notifyHeartbeatScheduler(leftRegion, d.peer)
	d.notifyHeartbeatScheduler(rightRegion, newPeer)
}
func (d *peerMsgHandler) execAddNode(entry *eraftpb.Entry, confChange *eraftpb.ConfChange, kvWB *engine_util.WriteBatch) {
	//TODO
	//可能有重复命令
	d.RaftGroup.ApplyConfChange(*confChange)
	req := new(raft_cmdpb.RaftCmdRequest)
	err := req.Unmarshal(confChange.Context)
	if err != nil {
		log.Panic(err)
		return
	}
	nodeId := confChange.NodeId
	if req.Header != nil {
		fromEpoch := req.GetHeader().GetRegionEpoch()
		if fromEpoch != nil {
			if util.IsEpochStale(fromEpoch, d.Region().RegionEpoch) {
				resp := ErrResp(&util.ErrEpochNotMatch{})
				d.callBack(entry, resp, false)
				return
			}
		}
	}
	//更改RegionLocalState
	if !d.nodeExits(nodeId) {
		log.Infof("添加node %d to region %d", nodeId, d.regionId)
		d.ctx.storeMeta.Lock()
		region := d.Region() //该函数返回指针
		peer := &metapb.Peer{Id: nodeId, StoreId: req.AdminRequest.ChangePeer.Peer.StoreId}
		region.Peers = append(region.Peers, peer)
		region.RegionEpoch.ConfVer++
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		d.insertPeerCache(peer)
		d.ctx.storeMeta.Unlock()
	}
	//response
	response := newCmdResp()
	response.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer, ChangePeer: &raft_cmdpb.ChangePeerResponse{}}
	d.callBack(entry, response, false)
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
}
func (d *peerMsgHandler) execRemoveNode(entry *eraftpb.Entry, confChange *eraftpb.ConfChange, kvWB *engine_util.WriteBatch) {
	//TODO
	d.RaftGroup.ApplyConfChange(*confChange)
	req := new(raft_cmdpb.RaftCmdRequest)
	err := req.Unmarshal(confChange.Context)
	if err != nil {
		log.Panic(err)
		return
	}
	nodeId := confChange.NodeId
	if req.Header != nil {
		fromEpoch := req.GetHeader().GetRegionEpoch()
		if fromEpoch != nil {
			if util.IsEpochStale(fromEpoch, d.Region().RegionEpoch) {
				resp := ErrResp(&util.ErrEpochNotMatch{})
				d.callBack(entry, resp, false)
				return
			}
		}
	}
	//更改RegionLocalState
	if nodeId == d.PeerId() {
		d.startToDestroyPeer()
		kvWB.DeleteMeta(meta.ApplyStateKey(d.regionId))
	} else if d.nodeExits(nodeId) {
		region := d.Region() //该函数返回指针
		d.ctx.storeMeta.Lock()
		region.RegionEpoch.ConfVer++

		//从peers中删除
		for i, p := range region.Peers {
			//fmt.Printf("---Loop %d---\n", i)
			if p.Id == nodeId {
				region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
				break
			}
		}
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		d.removePeerCache(nodeId)
		d.ctx.storeMeta.Unlock()
	}

	//reponse
	response := newCmdResp()
	response.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer, ChangePeer: &raft_cmdpb.ChangePeerResponse{}}
	d.callBack(entry, response, false)
	d.notifyHeartbeatScheduler(d.Region(), d.peer)

}
func (d *peerMsgHandler) callBack(entry *eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse, isSnap bool) {
	//callback用来断定命令是否被执行,当执行之后,就需要从proposal中调用这些callback(cb.Done())进行回应,回应完后删除proposal
	//callback中response类型里,Get的response有value字段,推断response可用于返回阶段,通过callback进行值的传递.

	//清理过时的proposal
	if len(d.proposals) > 0 {
		index := 0
		for _, proposal := range d.proposals {
			if proposal.index < entry.Index {
				//过时了
				proposal.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				index++
			} else {
				break
			}
		}
		if index == len(d.proposals) {
			d.proposals = make([]*proposal, 0)
			return
		} else {
			d.proposals = d.proposals[index:]
		}
	}
	if len(d.proposals) == 0 {
		return
	}
	//现在,proposals中至少有一个proposal,且里面全是"不比entry旧"的proposal
	proposal := d.proposals[0]
	if proposal.index <= entry.Index { //为什么是小于等于呢？
		if proposal.term != entry.Term {
			NotifyStaleReq(entry.Term, proposal.cb)
			d.proposals = d.proposals[1:]
		} else {
			if isSnap {
				proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
			proposal.cb.Done(resp)
			d.proposals = d.proposals[1:]
		}
	}
}
func (d *peerMsgHandler) nodeExits(nodeId uint64) bool {
	region := d.Region()
	for _, peer := range region.Peers {
		if peer.Id == nodeId {
			return true
		}
	}
	return false
}
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	//callback用来断定命令是否被执行,当执行之后,就需要从proposal中调用这些callback(cb.Done())进行回应,回应完后删除proposal
	//callback中response类型里,Get的response有value字段,推断response可用于返回阶段,通过callback进行值的传递.
	//或许此处不需要管response？
	if len(msg.Requests) > 0 {
		for len(msg.Requests) > 0 {
			req := msg.Requests[0]
			var Key []byte
			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				Key = req.Get.Key
			case raft_cmdpb.CmdType_Put:
				Key = req.Put.Key
			case raft_cmdpb.CmdType_Delete:
				Key = req.Delete.Key
			case raft_cmdpb.CmdType_Snap:
				//TODO
				//No key?
			}
			err = util.CheckKeyInRegion(Key, d.Region())
			if err != nil && req.CmdType != raft_cmdpb.CmdType_Snap {
				cb.Done(ErrResp(err))
				msg.Requests = msg.Requests[1:]
				continue
			}
			data, errr := msg.Marshal()
			if errr != nil {
				log.Panic(errr)
			}
			d.peer.proposals = append(d.peer.proposals, &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb})
			msg.Requests = msg.Requests[1:]
			d.RaftGroup.Propose(data)
		}
	} //真不知道下面的是干嘛的,proposal在raft里都在propose里处理
	//TODO——v
	if msg.AdminRequest != nil {
		req := msg.AdminRequest
		switch req.GetCmdType() {
		case raft_cmdpb.AdminCmdType_ChangePeer:
			if len(d.Region().Peers) == 2 && msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode && msg.AdminRequest.ChangePeer.Peer.Id == d.PeerId() {
				for _, p := range d.Region().Peers {
					if p.Id != d.PeerId() {
						d.RaftGroup.TransferLeader(p.Id)
						break
					}
				}
				err := fmt.Sprintf("%s return corner case\n", d.Tag)
				cb.Done(ErrResp(errors.New(err)))
				return
			}
			data, err := msg.Marshal()
			if err != nil {
				log.Panic(err)
				return
			}
			confChange := eraftpb.ConfChange{
				ChangeType: req.ChangePeer.ChangeType,
				NodeId:     req.ChangePeer.Peer.Id,
				Context:    data,
			}
			err = d.RaftGroup.ProposeConfChange(confChange)
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.peer.proposals = append(d.peer.proposals, &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb})
		case raft_cmdpb.AdminCmdType_CompactLog:
			data, err := msg.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.peer.proposals = append(d.peer.proposals, &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb})
			d.RaftGroup.Propose(data)
		case raft_cmdpb.AdminCmdType_TransferLeader:
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
			response := newCmdResp()
			response.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_TransferLeader, TransferLeader: &raft_cmdpb.TransferLeaderResponse{}}
			cb.Done(response)
		case raft_cmdpb.AdminCmdType_Split:
			err = util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
			if err != nil {
				log.Panic(err)
				cb.Done(ErrResp(err))
				return
			}
			data, err := msg.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.peer.proposals = append(d.peer.proposals, &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb})
			d.RaftGroup.Propose(data)
		}
	}

	// Your Code Here (2B).
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
func (d *peerMsgHandler) startToDestroyPeer() {
	if len(d.Region().Peers) == 2 && d.IsLeader() {
		var targetPeer uint64 = 0
		for _, peer := range d.Region().Peers {
			if peer.Id != d.PeerId() {
				targetPeer = peer.Id
				break
			}
		}
		m := []eraftpb.Message{{
			To:      targetPeer,
			MsgType: eraftpb.MessageType_MsgHeartbeat,
			Commit:  d.peerStorage.raftState.HardState.Commit,
		}}
		for i := 0; i < 10; i++ {
			d.Send(d.ctx.trans, m)
			time.Sleep(100 * time.Millisecond)
		}
	}
	d.destroyPeer()
}
func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected, isInitialized is " + strconv.FormatBool(isInitialized))
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
