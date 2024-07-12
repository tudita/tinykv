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
	"errors"

	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	for _, peer := range c.peers {
		prs[peer] = new(Progress)
	}
	return &Raft{
		RaftLog:          newLog(c.Storage),
		id:               c.ID,
		Prs:              prs,
		State:            StateFollower,
		heartbeatElapsed: c.HeartbeatTick,
		electionElapsed:  c.ElectionTick,
	}
	//利用config中的信息初始化raft结构体
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	Ents := r.RaftLog.allEntries()
	var ents []*eraftpb.Entry
	next := r.Prs[to].Next
	for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
		ents = append(ents, &Ents[i])
	}
	message := eraftpb.Message{
		MsgType: eraftpb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,                            //发送者ID(leader)
		Term:    r.Term,                          //发送者任期(leader)
		LogTerm: r.RaftLog.entries[next-1].Term,  //紧邻着上一个日志条目的任期
		Index:   r.RaftLog.entries[next-1].Index, //紧邻着上一个日志条目的索引
		Commit:  r.RaftLog.committed,             //current commit index
		Entries: ents,
	}
	r.msgs = append(r.msgs, message)
	return true
	//什么情况下会sent失败？
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	next := r.Prs[to].Next
	message := eraftpb.Message{
		MsgType: eraftpb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,                            //发送者ID(leader)
		Term:    r.Term,                          //发送者任期(leader)
		LogTerm: r.RaftLog.entries[next-1].Term,  //紧邻着上一个日志条目的任期
		Index:   r.RaftLog.entries[next-1].Index, //紧邻着上一个日志条目的索引
		Commit:  r.RaftLog.committed,             //current commit index
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	message := eraftpb.Message{
		MsgType: eraftpb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,                                           //候选人id
		Term:    r.Term,                                         //候选人任期号
		Index:   r.RaftLog.entries[r.RaftLog.LastIndex()].Index, //最后一个日志条目的索引
		LogTerm: r.RaftLog.entries[r.RaftLog.LastIndex()].Term,  //最后一个日志条目的任期
	}
	r.msgs = append(r.msgs, message)
}
func (r *Raft) sendRequestVote2All() {
	for id := range r.Prs {
		if id != r.id {
			r.sendRequestVote(id)
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	//Raft中有两种tick, 一种是heartbeat, 一种是electionElapsed，
	//heartbeatElapsed只有在r为leader时有效,每达到一次heartbeatTimeout就向follower发送一次heartbeat
	//electionElapsed无论对谁都有效。对于leader和candidate,每间隔一个electionTimeout就清空
	//对于follower,如果收到了leader的消息就清空,否则,若达到了electionTimeout,说明leader寄了,转化为candidate参与选举
	//此时记得重置elctionTimeout
	r.electionElapsed++
	if r.State == StateLeader { //若为领导人，发送heartbeat
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			for id := range r.Prs {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		}
		//leader宕机后变为follower或许应该在异步接受到heartbeat后处理？
	}
	if r.State == StateFollower {
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			//这个地方按理说应该随机化electionTimeout，但我先不写————————————————————
			r.becomeCandidate()
			//发起投票请求
			r.sendRequestVote2All()
		}
		//follower接受heartbeat后清空electionElapsed应该也是异步进行的？
	}
	if r.State == StateCandidate {
		//选票瓜分后需要随机重置超时时间,先不写————————————————————
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			//随机
			//重新发起选举

		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		}
	case StateCandidate:
		switch m.MsgType {
		}
	case StateLeader:
		switch m.MsgType {
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
