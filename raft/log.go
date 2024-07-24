// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//snapshot, _ := storage.Snapshot() //初始化时不需要创建快照
	HardState, _, _ := storage.InitialState()

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	log := &RaftLog{
		storage:   storage,
		entries:   entries,
		committed: HardState.Commit,
		applied:   firstIndex - 1,
		stabled:   lastIndex, //stabled应该是开的右区间,不知道要不要+1，先这么写
		//pendingSnapshot: &snapshot,
	}
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	index, err := l.storage.FirstIndex()
	//FirstIndex 返回可能通过 Entries 方法可用的第一个日志条目的索引（较旧的条目已被合并到最新的快照中
	if err != nil {
		panic(err)
	}
	if len(l.entries) > 0 {
		if index > l.LastIndex() {
			l.entries = nil
		} else if index >= l.FirstIndex() {
			l.entries = l.entries[index-l.FirstIndex():]
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	var validEntries []pb.Entry
	for _, entry := range l.entries {
		if entry.Data == nil {
			validEntries = append(validEntries, entry)
		}
	}
	return validEntries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).

	if len(l.entries) > 0 {
		if l.stabled <= l.LastIndex()-1 {
			//防止越界
			return l.entries[l.stabled-l.FirstIndex()+1:]
		}
	}
	return make([]pb.Entry, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		if l.applied >= l.FirstIndex()-1 && l.committed >= l.FirstIndex()-1 && l.applied <= l.committed && l.committed <= l.LastIndex() {
			//防止越界
			return l.entries[l.applied-l.FirstIndex()+1 : l.committed-l.FirstIndex()+1]
		}
	}
	return make([]pb.Entry, 0)
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		index, _ := l.storage.FirstIndex()
		return index
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		index, _ := l.storage.LastIndex()
		return index
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		lastIndex := l.LastIndex()
		if firstIndex <= i && i <= lastIndex {
			return l.entries[i-firstIndex].Term, nil
		}
	}
	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}
	return 0, err
}
func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}
func (l *RaftLog) committedTo(i uint64) {
	if i == 0 {
		return
	}
	if i < l.committed {
		log.Panicf("committed(%d) is out of range [prevCommitted(%d), inf]", i, l.applied)
	}
	l.committed = i
}
