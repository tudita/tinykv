// 版权所有 2015 年 etcd 作者
//
// 根据 Apache 许可证第 2 版（“许可证”）授权;
// 除非符合许可证的规定，否则您不得使用此文件。
// 您可以在以下网址获取许可证的副本
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// 除非适用法律要求或书面同意，否则在许可证下分发的软件
// 是基于“按原样”分发的，不附带任何明示或暗示的保证或条件。
// 有关详细信息，请参阅 Diego Ongaro 和 John Ousterhout 的《寻找可理解的共识算法》
// （https://ramcloud.stanford.edu/raft.pdf）。

/*
raft 包以 Protocol Buffer 格式发送和接收消息，
该格式在 eraftpb 包中定义。

Raft 是一种协议，通过该协议，一组节点可以维护一个复制状态机。
通过使用复制日志，状态机保持同步。
有关 Raft 的更多详细信息，请参见 Diego Ongaro 和 John Ousterhout 的《寻找可理解的共识算法》
（https://ramcloud.stanford.edu/raft.pdf）。

# 用法

raft 中的主要对象是 Node。您可以使用 raft.StartNode 从头开始启动一个 Node，
或者使用 raft.RestartNode 从某个初始状态启动一个 Node。

raft.StartNode
raft.RestartNode

从头开始启动一个节点：

	storage := raft.NewMemoryStorage()
	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	}
	n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

To restart a node from previous state:

	storage := raft.NewMemoryStorage()

	// recover the in-memory storage from persistent
	// snapshot, state and entries.
	storage.ApplySnapshot(snapshot)
	storage.SetHardState(state)
	storage.Append(entries)

	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	  MaxInflightMsgs: 256,
	}

	// restart raft without peer information.
	// peer information is already included in the storage.
	n := raft.RestartNode(c)

Now that you are holding onto a Node you have a few responsibilities:

First, you must read from the Node.Ready() channel and process the updates
it contains. These steps may be performed in parallel, except as noted in step
2.

1. Write HardState, Entries, and Snapshot to persistent storage if they are
not empty. Note that when writing an Entry with Index i, any
previously-persisted entries with Index >= i must be discarded.

2. Send all Messages to the nodes named in the To field. It is important that
no messages be sent until the latest HardState has been persisted to disk,
and all Entries written by any previous Ready batch (Messages may be sent while
entries from the same batch are being persisted).

Note: Marshalling messages is not thread-safe; it is important that you
make sure that no new entries are persisted while marshalling.
The easiest way to achieve this is to serialize the messages directly inside
your main raft loop.

3. Apply Snapshot (if any) and CommittedEntries to the state machine.
If any committed Entry has Type EntryType_EntryConfChange, call Node.ApplyConfChange()
to apply it to the node. The configuration change may be cancelled at this point
by setting the NodeId field to zero before calling ApplyConfChange
(but ApplyConfChange must be called one way or the other, and the decision to cancel
must be based solely on the state machine and not external information such as
the observed health of the node).

4. Call Node.Advance() to signal readiness for the next batch of updates.
This may be done at any time after step 1, although all updates must be processed
in the order they were returned by Ready.

Second, all persisted log entries must be made available via an
implementation of the Storage interface. The provided MemoryStorage
type can be used for this (if you repopulate its state upon a
restart), or you can supply your own disk-backed implementation.

Third, when you receive a message from another node, pass it to Node.Step:

	func recvRaftRPC(ctx context.Context, m eraftpb.Message) {
		n.Step(ctx, m)
	}

Finally, you need to call Node.Tick() at regular intervals (probably
via a time.Ticker). Raft has two important timeouts: heartbeat and the
election timeout. However, internally to the raft package time is
represented by an abstract "tick".

The total state machine handling loop will look something like this:

	for {
	  select {
	  case <-s.Ticker:
	    n.Tick()
	  case rd := <-s.Node.Ready():
	    saveToStorage(rd.State, rd.Entries, rd.Snapshot)
	    send(rd.Messages)
	    if !raft.IsEmptySnap(rd.Snapshot) {
	      processSnapshot(rd.Snapshot)
	    }
	    for _, entry := range rd.CommittedEntries {
	      process(entry)
	      if entry.Type == eraftpb.EntryType_EntryConfChange {
	        var cc eraftpb.ConfChange
	        cc.Unmarshal(entry.Data)
	        s.Node.ApplyConfChange(cc)
	      }
	    }
	    s.Node.Advance()
	  case <-s.done:
	    return
	  }
	}

To propose changes to the state machine from your node take your application
data, serialize it into a byte slice and call:

	n.Propose(data)

If the proposal is committed, data will appear in committed entries with type
eraftpb.EntryType_EntryNormal. There is no guarantee that a proposed command will be
committed; you may have to re-propose after a timeout.

To add or remove a node in a cluster, build ConfChange struct 'cc' and call:

	n.ProposeConfChange(cc)

After config change is committed, some committed entry with type
eraftpb.EntryType_EntryConfChange will be returned. You must apply it to node through:

	var cc eraftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)

Note: An ID represents a unique node in a cluster for all time. A
given ID MUST be used only once even if the old node has been removed.
This means that for example IP addresses make poor node IDs since they
may be reused. Node IDs must be non-zero.

# Implementation notes

This implementation is up to date with the final Raft thesis
(https://ramcloud.stanford.edu/~ongaro/thesis.pdf), although our
implementation of the membership change protocol differs somewhat from
that described in chapter 4. The key invariant that membership changes
happen one node at a time is preserved, but in our implementation the
membership change takes effect when its entry is applied, not when it
is added to the log (so the entry is committed under the old
membership instead of the new). This is equivalent in terms of safety,
since the old and new configurations are guaranteed to overlap.

To ensure that we do not attempt to commit two membership changes at
once by matching log positions (which would be unsafe since they
should have different quorum requirements), we simply disallow any
proposed membership change while any uncommitted change appears in
the leader's log.

This approach introduces a problem when you try to remove a member
from a two-member cluster: If one of the members dies before the
other one receives the commit of the confchange entry, then the member
cannot be removed any more since the cluster cannot make progress.
For this reason it is highly recommended to use three or more nodes in
every cluster.

# MessageType

Package raft sends and receives message in Protocol Buffer format (defined
in eraftpb package). Each state (follower, candidate, leader) implements its
own 'step' method ('stepFollower', 'stepCandidate', 'stepLeader') when
advancing with the given eraftpb.Message. Each step is determined by its
eraftpb.MessageType. Note that every step is checked by one common method
'Step' that safety-checks the terms of node and incoming message to prevent
stale log entries:

	'MessageType_MsgHup' is used for election. If a node is a follower or candidate, the
	'tick' function in 'raft' struct is set as 'tickElection'. If a follower or
	candidate has not received any heartbeat before the election timeout, it
	passes 'MessageType_MsgHup' to its Step method and becomes (or remains) a candidate to
	start a new election.

	'MessageType_MsgBeat' is an internal type that signals the leader to send a heartbeat of
	the 'MessageType_MsgHeartbeat' type. If a node is a leader, the 'tick' function in
	the 'raft' struct is set as 'tickHeartbeat', and triggers the leader to
	send periodic 'MessageType_MsgHeartbeat' messages to its followers.

	'MessageType_MsgPropose' proposes to append data to its log entries. This is a special
	type to redirect proposals to the leader. Therefore, send method overwrites
	eraftpb.Message's term with its HardState's term to avoid attaching its
	local term to 'MessageType_MsgPropose'. When 'MessageType_MsgPropose' is passed to the leader's 'Step'
	method, the leader first calls the 'appendEntry' method to append entries
	to its log, and then calls 'bcastAppend' method to send those entries to
	its peers. When passed to candidate, 'MessageType_MsgPropose' is dropped. When passed to
	follower, 'MessageType_MsgPropose' is stored in follower's mailbox(msgs) by the send
	method. It is stored with sender's ID and later forwarded to the leader by
	rafthttp package.

	'MessageType_MsgAppend' contains log entries to replicate. A leader calls bcastAppend,
	which calls sendAppend, which sends soon-to-be-replicated logs in 'MessageType_MsgAppend'
	type. When 'MessageType_MsgAppend' is passed to candidate's Step method, candidate reverts
	back to follower, because it indicates that there is a valid leader sending
	'MessageType_MsgAppend' messages. Candidate and follower respond to this message in
	'MessageType_MsgAppendResponse' type.

	'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend'). When
	'MessageType_MsgAppend' is passed to candidate or follower's Step method, it responds by
	calling 'handleAppendEntries' method, which sends 'MessageType_MsgAppendResponse' to raft
	mailbox.

	'MessageType_MsgRequestVote' requests votes for election. When a node is a follower or
	candidate and 'MessageType_MsgHup' is passed to its Step method, then the node calls
	'campaign' method to campaign itself to become a leader. Once 'campaign'
	method is called, the node becomes candidate and sends 'MessageType_MsgRequestVote' to peers
	in cluster to request votes. When passed to the leader or candidate's Step
	method and the message's Term is lower than leader's or candidate's,
	'MessageType_MsgRequestVote' will be rejected ('MessageType_MsgRequestVoteResponse' is returned with Reject true).
	If leader or candidate receives 'MessageType_MsgRequestVote' with higher term, it will revert
	back to follower. When 'MessageType_MsgRequestVote' is passed to follower, it votes for the
	sender only when sender's last term is greater than MessageType_MsgRequestVote's term or
	sender's last term is equal to MessageType_MsgRequestVote's term but sender's last committed
	index is greater than or equal to follower's.

	'MessageType_MsgRequestVoteResponse' contains responses from voting request. When 'MessageType_MsgRequestVoteResponse' is
	passed to candidate, the candidate calculates how many votes it has won. If
	it's more than majority (quorum), it becomes leader and calls 'bcastAppend'.
	If candidate receives majority of votes of denials, it reverts back to
	follower.

	'MessageType_MsgSnapshot' requests to install a snapshot message. When a node has just
	become a leader or the leader receives 'MessageType_MsgPropose' message, it calls
	'bcastAppend' method, which then calls 'sendAppend' method to each
	follower. In 'sendAppend', if a leader fails to get term or entries,
	the leader requests snapshot by sending 'MessageType_MsgSnapshot' type message.

	'MessageType_MsgHeartbeat' sends heartbeat from leader. When 'MessageType_MsgHeartbeat' is passed
	to candidate and message's term is higher than candidate's, the candidate
	reverts back to follower and updates its committed index from the one in
	this heartbeat. And it sends the message to its mailbox. When
	'MessageType_MsgHeartbeat' is passed to follower's Step method and message's term is
	higher than follower's, the follower updates its leaderID with the ID
	from the message.

	'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'. When 'MessageType_MsgHeartbeatResponse'
	is passed to the leader's Step method, the leader knows which follower
	responded.
*/
package raft

// 版权所有 2015 年 etcd 作者
//
// 根据 Apache 许可证第 2 版（“许可证”）授权;
// 除非符合许可证的规定，否则您不得使用此文件。
// 您可以在以下网址获取许可证的副本
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// 除非适用法律要求或书面同意，否则在许可证下分发的软件
// 是基于“按原样”分发的，不附带任何明示或暗示的保证或条件。
// 有关详细信息，请参阅 Diego Ongaro 和 John Ousterhout 的《寻找可理解的共识算法》
// （https://ramcloud.stanford.edu/raft.pdf）。

/*
raft 包以 Protocol Buffer 格式发送和接收消息，
该格式在 eraftpb 包中定义。

Raft 是一种协议，通过该协议，一组节点可以维护一个复制状态机。
通过使用复制日志，状态机保持同步。
有关 Raft 的更多详细信息，请参见 Diego Ongaro 和 John Ousterhout 的《寻找可理解的共识算法》
（https://ramcloud.stanford.edu/raft.pdf）。

# 用法

raft 中的主要对象是 Node。您可以使用 raft.StartNode 从头开始启动一个 Node，
或者使用 raft.RestartNode 从某个初始状态启动一个 Node。

raft.StartNode
raft.RestartNode

从头开始启动一个节点：

	storage := raft.NewMemoryStorage()
	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	}
	n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

从先前的状态重新启动一个节点：

	storage := raft.NewMemoryStorage()

	// 从持久化的快照、状态和条目中恢复内存中的存储。
	storage.ApplySnapshot(snapshot)
	storage.SetHardState(state)
	storage.Append(entries)

	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	  MaxInflightMsgs: 256,
	}

	// 重新启动 raft，不包含对等节点信息。
	// 对等节点信息已包含在存储中。
	n := raft.RestartNode(c)

现在，您需要承担一些责任：

首先，您必须从 Node.Ready() 通道中读取并处理其中包含的更新。
这些步骤可以并行执行，除非在步骤 2 中另有说明。

1. 如果 HardState、Entries 和 Snapshot 不为空，则将其写入持久化存储。
   注意，当写入索引为 i 的 Entry 时，必须丢弃索引大于等于 i 的任何先前持久化的条目。

2. 将所有 Messages 发送到 To 字段中指定的节点。
   重要的是，在最新的 HardState 已经持久化到磁盘之前，不要发送任何消息，
   并且在任何先前的 Ready 批次中写入的所有 Entries（在写入 Entries 时，可以同时发送 Messages）都被持久化。

注意：消息的编组不是线程安全的；确保在编组时没有持久化新条目非常重要。
最简单的方法是在主 raft 循环中直接序列化消息。

3. 将 Snapshot（如果有）和 CommittedEntries 应用于状态机。
   如果任何已提交的 Entry 的类型为 EntryType_EntryConfChange，请调用 Node.ApplyConfChange() 将其应用于节点。
   在调用 ApplyConfChange 之前，可以通过将 NodeId 字段设置为零来取消配置更改
   （但是必须以状态机为基础，而不是外部信息，如节点的观察到的健康状况，来决定是否取消）。

4. 调用 Node.Advance() 以表示准备好处理下一批更新。
   可以在步骤 1 之后的任何时间执行此操作，尽管所有更新都必须按照 Ready 返回的顺序进行处理。

其次，必须通过 Storage 接口的实现来提供所有持久化的日志条目。
可以使用提供的 MemoryStorage 类型（如果在重新启动时重新填充其状态），
或者可以提供自己的基于磁盘的实现。

第三，当您从另一个节点接收到消息时，请将其传递给 Node.Step：

	func recvRaftRPC(ctx context.Context, m eraftpb.Message) {
		n.Step(ctx, m)
	}

最后，您需要定期调用 Node.Tick()（可能通过 time.Ticker）。
Raft 有两个重要的超时时间：心跳超时和选举超时。
但是，在 raft 包内部，时间由抽象的“tick”表示。

总的状态机处理循环将如下所示：

	for {
	  select {
	  case <-s.Ticker:
		n.Tick()
	  case rd := <-s.Node.Ready():
		saveToStorage(rd.State, rd.Entries, rd.Snapshot)
		send(rd.Messages)
		if !raft.IsEmptySnap(rd.Snapshot) {
		  processSnapshot(rd.Snapshot)
		}
		for _, entry := range rd.CommittedEntries {
		  process(entry)
		  if entry.Type == eraftpb.EntryType_EntryConfChange {
			var cc eraftpb.ConfChange
			cc.Unmarshal(entry.Data)
			s.Node.ApplyConfChange(cc)
		  }
		}
		s.Node.Advance()
	  case <-s.done:
		return
	  }
	}

要向节点的状态机提议更改，请将应用程序数据序列化为字节切片，并调用：

	n.Propose(data)

如果提议被提交，数据将以类型为 eraftpb.EntryType_EntryNormal 的已提交条目的形式出现。
不能保证提议的命令将被提交；您可能需要在超时后重新提议。

要在集群中添加或删除节点，请构建 ConfChange 结构 'cc' 并调用：

	n.ProposeConfChange(cc)

配置更改提交后，将返回某个类型为 eraftpb.EntryType_EntryConfChange 的已提交条目。
您必须通过以下方式将其应用于节点：

	var cc eraftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)

注意：ID 表示集群中的唯一节点，一直保持不变。
给定的 ID 必须仅使用一次，即使旧节点已被删除。
这意味着例如 IP 地址不适合作为节点 ID，因为它们可能会被重用。
节点 ID 必须是非零的。

# 实现说明

此实现与最终的 Raft 论文（https://ramcloud.stanford.edu/~ongaro/thesis.pdf）保持最新，
尽管我们的成员更改协议的实现与第 4 章中描述的有所不同。
保持的关键不变条件是，成员更改一次只发生一个节点，但在我们的实现中，
成员更改在其条目应用时生效，而不是在将其添加到日志中时生效
（因此，在旧成员身份下提交该条目，而不是在新成员身份下提交）。
从安全性的角度来看，这是等效的，因为旧配置和新配置都保证重叠。

为了确保我们不会通过匹配日志位置尝试同时提交两个成员更改
（这是不安全的，因为它们应具有不同的法定要求），我们只是禁止
在领导者的日志中出现任何未提交的更改时，禁止任何提议的成员更改。

当您尝试从两个成员的集群中删除一个成员时，这种方法会引入一个问题：
如果其中一个成员在另一个成员接收到 confchange 条目的提交之前死亡，
那么该成员将无法再被删除，因为集群无法取得进展。
因此，强烈建议在每个集群中使用三个或更多节点。

# MessageType

raft 包以 Protocol Buffer 格式（在 eraftpb 包中定义）发送和接收消息。
每个状态（follower、candidate、leader）在前进时都会实现自己的 'step' 方法
（'stepFollower'、'stepCandidate'、'stepLeader'），该方法根据给定的 eraftpb.Message 进行操作。
每个步骤都由其 eraftpb.MessageType 确定。请注意，每个步骤都由一个公共方法 'Step' 检查，
该方法会检查节点和传入消息的术语，以防止陈旧的日志条目：

'MessageType_MsgHup' 用于选举。如果节点是 follower 或 candidate，则在 raft 结构中的 'tick' 函数设置为 'tickElection'。
如果 follower 或 candidate 在选举超时之前没有收到任何心跳，则将 'MessageType_MsgHup' 传递给其 Step 方法，并成为（或保持）candidate 以开始新的选举。

'MessageType_MsgBeat' 是一个内部类型，用于向 leader 发送 'MessageType_MsgHeartbeat' 类型的心跳。
如果节点是 leader，则在 raft 结构中的 'tick' 函数设置为 'tickHeartbeat'，并触发 leader 定期向其 followers 发送 'MessageType_MsgHeartbeat' 消息。

'MessageType_MsgPropose' 用于将数据附加到其日志条目。这是一种特殊类型，用于将提议重定向到 leader。
因此，send 方法会将 eraftpb.Message 的术语替换为其 HardState 的术语，以避免将其本地术语附加到 'MessageType_MsgPropose'。
当 'MessageType_MsgPropose' 传递给 leader 的 Step 方法时，leader 首先调用 appendEntry 方法将条目附加到其日志中，
然后调用 bcastAppend 方法将这些条目发送给其 peers。当传递给 candidate 时，'MessageType_MsgPropose' 被丢弃。
当传递给 follower 时，'MessageType_MsgPropose' 通过 send 方法存储在 follower 的邮箱（msgs）中。
它与发送者的 ID 一起存储，并稍后由 rafthttp 包转发给 leader。

'MessageType_MsgAppend' 包含要复制的日志条目。leader 调用 bcastAppend，bcastAppend 调用 sendAppend，
sendAppend 以 'MessageType_MsgAppend' 类型发送即将复制的日志。当 'MessageType_MsgAppend' 传递给 candidate 的 Step 方法时，
candidate 会恢复为 follower，因为它表示有一个有效的 leader 发送 'MessageType_MsgAppend' 消息。
candidate 和 follower 以 'MessageType_MsgAppendResponse' 类型响应此消息。

'MessageType_MsgAppendResponse' 是对日志复制请求（'MessageType_MsgAppend'）的响应。
当 'MessageType_MsgAppend' 传递给 candidate 或 follower 的 Step 方法时，它会通过调用 'handleAppendEntries' 方法响应，
该方法将 'MessageType_MsgAppendResponse' 发送到 raft 邮箱。

'MessageType_MsgRequestVote' 请求选举的投票。当节点是 follower 或 candidate，并且 'MessageType_MsgHup' 传递给其 Step 方法时，
节点调用 'campaign' 方法以将自身竞选为 leader。一旦调用了 'campaign' 方法，节点就成为 candidate，并向集群中的 peers 发送 'MessageType_MsgRequestVote' 请求投票。
当传递给 leader 或 candidate 的 Step 方法，并且消息的 Term 低于 leader 或 candidate 的 Term 时，
'MessageType_MsgRequestVote' 将被拒绝（返回带有 Reject 为 true 的 'MessageType_MsgRequestVoteResponse'）。
如果 leader 或 candidate 收到更高 term 的 'MessageType_MsgRequestVote'，它将恢复为 follower。
当 'MessageType_MsgRequestVote' 传递给 follower 时，只有当发送者的最后一个 term 大于 MessageType_MsgRequestVote 的 term，
或者发送者的最后一个 term 等于 MessageType_MsgRequestVote 的 term，但发送者的最后一个已提交索引大于等于 follower 的索引时，follower 才会为发送者投票。

'MessageType_MsgRequestVoteResponse' 包含来自投票请求的响应。
当 'MessageType_MsgRequestVoteResponse' 传递给 candidate 时，candidate 计算自己赢得了多少票。
如果票数超过半数（法定要求），则它成为 leader，并调用 'bcastAppend'。
如果 candidate 收到了大多数否决票，它将恢复为 follower。

'MessageType_MsgSnapshot' 请求安装快照消息。当节点刚刚成为 leader 或 leader 接收到 'MessageType_MsgPropose' 消息时，
它调用 'bcastAppend' 方法，然后调用 'sendAppend' 方法向每个 follower 发送 'MessageType_MsgAppend' 类型的日志。
在 'sendAppend' 中，如果 leader 无法获取术语或条目，则会通过发送 'MessageType_MsgSnapshot' 类型的消息请求快照。

'MessageType_MsgHeartbeat' 从 leader 发送心跳。当 'MessageType_MsgHeartbeat' 传递给 candidate，并且消息的 term 高于 candidate 的 term 时，
candidate 会恢复为 follower，并从此心跳中更新其已提交索引。它还将消息发送到其邮箱中。
当 'MessageType_MsgHeartbeat' 传递给 follower 的 Step 方法，并且消息的 term 高于 follower 的 term 时，
follower 将其 leaderID 更新为消息中的 ID。

'MessageType_MsgHeartbeatResponse' 是对 'MessageType_MsgHeartbeat' 的响应。
当 'MessageType_MsgHeartbeatResponse' 传递给 leader 的 Step 方法时，leader 知道哪个 follower 做出了响应。
*/
