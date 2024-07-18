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
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sort"

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
	PendingConfIndex    uint64
	electionTimeoutbase int
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
	hardState, _, _ := c.Storage.InitialState()
	return &Raft{
		RaftLog:             newLog(c.Storage),
		id:                  c.ID,
		Prs:                 prs,
		State:               StateFollower,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		electionTimeoutbase: c.ElectionTick,
		votes:               make(map[uint64]bool),
		Vote:                hardState.Vote,
		Term:                hardState.Term,
	}
	//利用config中的信息初始化raft结构体
	//还要用到hardState
}
func (r *Raft) reset() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Vote = None
	r.votes = make(map[uint64]bool)
}
func (r *Raft) append(entries []*pb.Entry) {
	for i, entry := range entries {
		ent := pb.Entry{
			Term:  r.Term,
			Index: r.RaftLog.LastIndex() + 1 + uint64(i),
			Data:  entry.Data,
		}
		r.RaftLog.entries = append(r.RaftLog.entries, ent)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var ents []*eraftpb.Entry
	pr, ok := r.Prs[to]
	if !ok {
		return false
	}
	firstIndex := r.RaftLog.FirstIndex()
	for i := pr.Next; i <= r.RaftLog.LastIndex(); i++ {
		ents = append(ents, &r.RaftLog.entries[i-firstIndex])
	}

	prevLogIndex := pr.Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	message := eraftpb.Message{
		MsgType: eraftpb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,                //发送者ID(leader)
		Term:    r.Term,              //发送者任期(leader)
		LogTerm: prevLogTerm,         //紧邻着上一个日志条目的任期
		Index:   prevLogIndex,        //紧邻着上一个日志条目的索引
		Commit:  r.RaftLog.committed, //current commit index
		Entries: ents,
	}
	r.msgs = append(r.msgs, message)

	return true
	//什么情况下会sent失败？
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	message := eraftpb.Message{
		MsgType: eraftpb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,                //发送者ID(leader)
		Term:    r.Term,              //发送者任期(leader)
		Commit:  r.RaftLog.committed, //current commit index
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	//无条目时,term为0,last也为0,好像没问题
	message := eraftpb.Message{
		MsgType: eraftpb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,      //候选人id
		Term:    r.Term,    //候选人任期号
		Index:   lastIndex, //最后一个日志条目的索引
		LogTerm: lastTerm,  //最后一个日志条目的任期
	}
	r.msgs = append(r.msgs, message)
}
func (r *Raft) sendAppend2All() {
	cnt := 0
	//fmt.Printf("%d\n",len(r.Prs))
	for pr := range r.Prs {
		cnt++
		if pr != r.id {
			r.sendAppend(pr)
		}
	}
	r.updateCommited()
}
func (r *Raft) sendRequestVote2All() {
	for id := range r.Prs {
		if id != r.id {
			r.sendRequestVote(id)
		}
	}
}
func (r *Raft) sendHeartbeat2All() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}
func (r *Raft) RandomizeTimeout() {
	rr, _ := rand.Int(rand.Reader, big.NewInt(int64(r.electionTimeoutbase)))
	r.electionTimeout += int(rr.Int64())
	for r.electionTimeout >= 2*r.electionTimeoutbase {
		r.electionTimeout -= r.electionTimeoutbase
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
			r.sendHeartbeat2All()
		}
		//leader宕机后变为follower或许应该在异步接受到heartbeat后处理？
	}
	if r.State == StateFollower {
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
		//follower接受heartbeat后清空electionElapsed应该也是异步进行的？
	}
	if r.State == StateCandidate {

		//选票瓜分后需要随机重置超时时间,先不写———————————————写了
		if r.electionElapsed >= r.electionTimeout {
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
			r.electionElapsed = 0
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.reset()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {

	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term += 1
	r.reset()
	r.Vote = r.id //是投票的对象
	r.votes[r.id] = true
	//r.sendRequestVote2All()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.reset()
	//成为leader后初始化prs
	for id := range r.Prs {
		r.Prs[id].Match = 0
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		//nextindex初始化为领导人最后的日志条目索引+1
		//matchindex为已知的已经复制到该服务器的最高日志条目的索引,初始值为0
	}
	//明明要求新leader要附加一个noop entry，但是我注释掉这一行之后反而pass掉了？
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	//r.updateCommited()
}
func (r *Raft) elect() {
	//fmt.Printf("%s %d start elect The term is %d \n", r.State.String(), r.id, r.Term)
	r.RandomizeTimeout()
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	fmt.Printf("")
	//fmt.Printf("%d start elect The term is %d \n", r.id, r.Term)
	//发起投票请求
	r.sendRequestVote2All()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.Vote = None
		r.becomeFollower(m.Term, None)
	} //如果发送者term比较大,变为follower
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case eraftpb.MessageType_MsgHup:
			r.elect()
		case eraftpb.MessageType_MsgBeat:
		case eraftpb.MessageType_MsgPropose:
			return ErrProposalDropped
		case eraftpb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case eraftpb.MessageType_MsgAppendResponse:
		case eraftpb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case eraftpb.MessageType_MsgRequestVoteResponse:
		case eraftpb.MessageType_MsgSnapshot:
		case eraftpb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case eraftpb.MessageType_MsgHeartbeatResponse:
		case eraftpb.MessageType_MsgTransferLeader:
		case eraftpb.MessageType_MsgTimeoutNow:
		}
	case StateCandidate:
		switch m.MsgType {
		case eraftpb.MessageType_MsgHup:
			r.elect()
		case eraftpb.MessageType_MsgBeat:
		case eraftpb.MessageType_MsgPropose:
			return ErrProposalDropped
		case eraftpb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case eraftpb.MessageType_MsgAppendResponse:
		case eraftpb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case eraftpb.MessageType_MsgRequestVoteResponse:
			//TestLeaderElectionInOneRoundRPC2AA中只有一个节点的情况根本无法触发该message的处理
			//考虑一下也十分合理，因为也只有自己会投票,发起选举时特判
			reject := m.Reject
			r.votes[m.From] = !reject
			agreeCount := 0
			objectionCount := 0
			for _, value := range r.votes {
				if value {
					agreeCount++
				} else {
					objectionCount++
				}
			}
			//fmt.Printf("%d agreeCount %d objectionCount %d\n", r.id, agreeCount, objectionCount)
			if agreeCount*2 > len(r.Prs) {
				r.becomeLeader()
			}
			if objectionCount*2 >= len(r.Prs) {
				r.becomeFollower(r.Term, None)
			}

		case eraftpb.MessageType_MsgSnapshot:
		case eraftpb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case eraftpb.MessageType_MsgHeartbeatResponse:
		case eraftpb.MessageType_MsgTransferLeader:
		case eraftpb.MessageType_MsgTimeoutNow:
		}
	case StateLeader:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		} //如果发送者term比较大,变为follower
		switch m.MsgType {
		case eraftpb.MessageType_MsgHup:
		case eraftpb.MessageType_MsgBeat:
			r.sendHeartbeat2All()
		case eraftpb.MessageType_MsgPropose:
			r.handlePropose(m)
		case eraftpb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case eraftpb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case eraftpb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case eraftpb.MessageType_MsgRequestVoteResponse:
		case eraftpb.MessageType_MsgSnapshot:
		case eraftpb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case eraftpb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case eraftpb.MessageType_MsgTransferLeader:
		case eraftpb.MessageType_MsgTimeoutNow:
		}
	}
	return nil
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}
	if m.Reject {
		r.Prs[m.From].Next = min(r.Prs[m.From].Next-1, m.Index+1)
		r.sendAppend(m.From)
		return //拒绝则重发
	}

	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	if r.updateCommited() {
		r.sendAppend2All()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	response := pb.Message{
		MsgType: eraftpb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}

	if r.Term <= m.Term {
		r.becomeFollower(m.Term, m.From)
	} //候选人竞选期间若收到appendEntries请求,且任期大于等于自己,变为follower
	if r.State == StateLeader {
		return //leader收到appendEntries请求,忽略
	}
	if m.Term < r.Term {
		response.Reject = true
		r.msgs = append(r.msgs, response)
		return
	}

	if m.Index > r.RaftLog.LastIndex() {
		response.Reject = true
		r.msgs = append(r.msgs, response)
		return
	} else if t, _ := r.RaftLog.Term(m.Index); t != m.LogTerm {
		response.Reject = true
		r.msgs = append(r.msgs, response)
		return
	}

	for _, ent := range m.Entries {
		index := ent.Index
		oldTerm, err := r.RaftLog.Term(index) //报错说明
		//如果有冲突往后全都删了
		//在r.raftlog.entries范围外,存入
		if index-r.RaftLog.FirstIndex() > uint64(len(r.RaftLog.entries)) || index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		} else if oldTerm != ent.Term || err != nil { //不相符就删,往后全删了
			if index < r.RaftLog.FirstIndex() { //全都得删
				r.RaftLog.entries = make([]pb.Entry, 0)
			} else { //删一部分
				r.RaftLog.entries = r.RaftLog.entries[:index-r.RaftLog.FirstIndex()]
			}
			r.RaftLog.stabled = min(r.RaftLog.stabled, index-1) //index以及后面的都删了，要更新
			r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		}

	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committedTo(min(m.Commit, m.Index+uint64(len(m.Entries))))
		//fmt.Printf("lastIndex: %d ,m.Index: %d ,m.Index+len: %d\n", r.RaftLog.LastIndex(), m.Index, m.Index+uint64(len(m.Entries)))
		//这个和lastIndex区别在哪里呢？
		//如果传入的entries是空的,节点里多的未commit的条目不会被删除,但他们也未被commit,所以要亲手算出来
	}
	response.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, response)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	response := pb.Message{
		MsgType: eraftpb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	if r.Term <= m.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if r.Lead != m.From {
		r.Lead = m.From
	}
	r.electionElapsed = 0
	r.msgs = append(r.msgs, response)
}
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}
func (r *Raft) handleRequestVote(m pb.Message) {
	// 自己加的, 用于处理投票请求
	response := pb.Message{
		MsgType: eraftpb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true, //是否拒绝投票
	}
	//fmt.Printf("r.term:%d m.term%d\n", r.Term, m.Term)
	if r.Term < m.Term {
		r.Vote = None
		r.becomeFollower(m.Term, None)
	}
	if m.Term < r.Term {
		response.Reject = true
	}

	if r.Vote == None || r.Vote == m.From {
		last := r.RaftLog.LastIndex()
		term, _ := r.RaftLog.Term(last)
		//fmt.Printf("%d handlefrom %d---m.LogTerm %d term %d last %d m.Index %d \n", r.id, m.From, m.LogTerm, term, last, m.Index)
		if (m.LogTerm > term) || (m.LogTerm == term && (m.Index >= last)) {
			r.Vote = m.From
			response.Reject = false
			//fmt.Printf("%x vote to %x at term %d\n", r.id, m.From, r.Term)
		}
	}
	//fmt.Println("Reject: ", response.Reject)
	r.msgs = append(r.msgs, response)
}

// 更新了返回true,没更新返回false,更新leader的commited
func (r *Raft) updateCommited() bool {
	//假设存在 N 满足N > commitIndex，使得大多数的
	//matchIndex[i] ≥ N以及log[N].term == currentTerm
	//成立，则令 commitIndex = N
	//如果只有一个节点，什么时候更新commit呢
	match := make([]uint64, 0)
	for _, pr := range r.Prs {
		match = append(match, pr.Match)
	}
	sort.Slice(match, func(i, j int) bool { //    1 2 3 4 5 6
		return match[i] < match[j] //小的在前面   1 2 3 4 5 6 7
	})
	majority := match[(len(r.Prs)-1)/2]
	for ; majority > r.RaftLog.committed; majority-- {
		if term, _ := r.RaftLog.Term(majority); term == r.Term {
			break
		}
	}
	//输出match
	//fmt.Printf("%d match", r.id)
	//for _, m := range match {
	//	fmt.Printf(" %d  ", m)
	//}
	//fmt.Printf("\n")
	if majority > r.RaftLog.committed {
		r.RaftLog.committedTo(majority)
		//fmt.Printf("%d updateCommited to %d\n", r.id, majority)
		return true
	}
	return false
}

func (r *Raft) handlePropose(m pb.Message) {
	r.append(m.Entries)
	r.sendAppend2All()
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
