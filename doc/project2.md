- [Project2 RaftKV](#project2-raftkv)
  - [2A](#2a)
    - [RaftLog](#raftlog)
    - [Raft](#raft)
      - [MessageType](#messagetype)
        - [MessageType\_MsgHup](#messagetype_msghup)
        - [MessageType\_MsgBeat](#messagetype_msgbeat)
        - [MessageType\_MsgSnapshot](#messagetype_msgsnapshot)
        - [MessageType\_MsgPropose](#messagetype_msgpropose)
        - [MessageType\_MsgAppend](#messagetype_msgappend)
        - [MsgAppendResponse](#msgappendresponse)
        - [MessageType\_MsgRequestVote](#messagetype_msgrequestvote)
        - [MessageType\_MsgRequestVoteResponse](#messagetype_msgrequestvoteresponse)
        - [MessageType\_MsgHeartbeat](#messagetype_msgheartbeat)
        - [MessageType\_MsgHeartbeatResponse](#messagetype_msgheartbeatresponse)
        - [MessageType\_MsgTransferLeader](#messagetype_msgtransferleader)
        - [MessageType\_MsgTimeoutNow](#messagetype_msgtimeoutnow)
    - [RawNode](#rawnode)
    - [疑难和感想](#疑难和感想)
  - [2B](#2b)
    - [PeerMsgHandler](#peermsghandler)
      - [proposeRaftCommand](#proposeraftcommand)
      - [HandleRaftReady](#handleraftready)
    - [PeerStorage](#peerstorage)
      - [SaveReadyState](#savereadystate)
      - [Append](#append)
    - [疑难和感想](#疑难和感想-1)
  - [2C](#2c)
    - [日志压缩](#日志压缩)
    - [发送快照](#发送快照)
    - [疑难和感想](#疑难和感想-2)


# Project2 RaftKV

这一部分需要我们学习Raft算法并以给出的框架为基础，实现Raft算法。

Raft算法在很多地方都可以找到很详细的论述，此处不再赘述。

## 2A

此处我们需要实现 `rawnode.go`，`raft.go`和 `log.go`。`raft.go`对应`raft`算法，`log.go`负责日志相关，而`rawnode.go`是`raft`的上层封装。

- **RawNode**：该模块用来接收上层传来的信息，将信息下传给 Raft 模块。同时还会将raft处理的结果返回给上层(以Ready的形式)。总的来说RawNode对raft层的封装， 是 raft 层暴露给上层的一个模块，用来进行信息的传递。
- **Raft**：Raft 算法的核心模块。raft算法的各种操作都在这里实现。
- **RaftLog**：存储raft的状态信息，里面的数据不会持久化。

### RaftLog

```
// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
```

如图所示，不难理解各字段的含义。注意每个entry都有自己的index，这个index与下标并不一定相同。

### Raft

本部分的核心是Msg的收发

- `Step(m pb.Message)` ，上层 RawNode 通过该函数，将Msg传给Raft，我们需要在函数内部，根据不同消息做出不同的处理。
- `Tick()`，同样会被上层的 RawNode 调用，为Raft算法的逻辑时钟，每调用一次增加自己的 `r.heartbeatElapsed` 和 `r.electionElapsed`，并作出相应的处理，如发起选举，发送心跳等。

外部消息通过Step传入，要发送给其他节点信息需要存放在`r.msgs`中,`rawnode` 会在生成Ready时取走并发送给别的节点。

#### MessageType

Msg分为Local和普通两种，前者是本地发起的，后者是节点之间发送的。下面根据Msg类型来介绍：

##### MessageType_MsgHup

Local Message，收到后自己直接开始发起选举，但是要先判断自身的条件是否满足选举条件。

| 所需字段 | 作用     |
| -------- | -------- |
| MsgType  | 标注类别 |

##### MessageType_MsgBeat

Local Message，收到后，如果自己是 Leader，则广播 heartbeat。

| 所需字段 | 作用     |
| -------- | -------- |
| MsgType  | 标注类别 |

##### MessageType_MsgSnapshot

当 Leader 发现目标节点所需的日志已经被 compact 的时候，则发送 Snapshot。

| 所需字段 | 作用                                                   |
| -------- | ------------------------------------------------------ |
| MsgType  | 标注类别                                               |
| Term     | Leader 的 term                                         |
| Snapshot | Snapshot 数据，通过`r.RaftLog.storage.Snapshot()` 获取 |
| To       | 目标节点                                               |

##### MessageType_MsgPropose

Local Message，propose 数据。注意只有 Leader 才能处理 propose 数据，如果不是 leader，直接返回 `ErrProposalDropped` 

1. 判断 r.leadTransferee 是否等于 None，如果不是，返回 ErrProposalDropped，因为此时集群正在转移 Leader。如果是，往下；

2. 把 m.Entries 追加到自己的 Entries 中；

3. 向其他所有节点发送追加日志 RPC，即 MsgAppend，用于集群同步；

4. 如果集群中只有自己一个节点，则直接更新自己的 committedIndex；

| 所需字段 | 作用                     |
| -------- | ------------------------ |
| MsgType  | 标注类别                 |
| Entries  | Propose 的数据           |
| To       | 转发到别的 Leader 时需要 |

##### MessageType_MsgAppend

用于 Leader 向其他节点同步数据。

| 所需字段 | 作用                  |
| -------- | --------------------- |
| MsgType  | 标注类别              |
| Index    | 论文中的 prevLogIndex |
| Term     | Leader 的 term        |
| LogTerm  | 论文中的 prevLogTerm  |
| Entries  | Append 的数据         |
| Commit   | Leader 的 committed   |
| To       | 目标节点              |

发送：

1. 若发送的 Index 已经被压缩了，转为发送快照，否则继续

2. 根据Prs中的next，添加entries发送MsgAppend 

接收与处理：

1. 判断 Msg 的 Term 是否大于等于自己的 Term，是则更新，否则拒绝

2. 如果 prevLogIndex > r.RaftLog.LastIndex()，拒绝。否则往下

3. 如果接收者日志中没有包含这样一个条目：即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上。拒绝。否则往下

4. 追加新条目，同时删除冲突条目

5. 接受；

其中，不管是接受还是拒绝，都要返回一个 MsgAppendResponse 给 Leader，让 Leader 知道追加是否成功并且更新相关信息。 

##### MsgAppendResponse

Common Msg，用于节点告诉 Leader 日志同步是否成功

| 字段    | 值                                                         |
| ------- | ---------------------------------------------------------- |
| MsgType | pb.MessageType_MsgAppendResponse                           |
| Term    | 当前节点的 Term                                            |
| To      | to                                                         |
| Reject  | 是否拒绝                                                   |
| From    | 当前节点的 Id                                              |
| Index   | r.RaftLog.LastIndex()；该字段用于 Leader 更快地去更新 next |

发送：

1. 不管节点接受或是拒绝，都要发送一个 MsgAppendResponse 给 Leader，调整 Reject 字段即可，其他字段固定；

接收与处理：

1. 只有 Leader 会处理该 Msg，其余角色直接忽略
2. 如果被 reject 了，更新next为 min(r.Prs[m.From].Next-1, m.Index+1)
3. 如果没有 reject，则更新 match为m.Index ，next为m.Index + 1。
4. 更新 commit。 这里我还设置如果commit被更新，就会向其他节点发送一遍Append用以同步。

##### MessageType_MsgRequestVote

Candidate 用于发送投票请求。

| 所需字段 | 作用                  |
| -------- | --------------------- |
| MsgType  | 标注类别              |
| Term     | 节点当前 term         |
| Index    | 论文中的 lastLogIndex |
| LogTerm  | 论文中的 lastLogTerm  |
| To       | 目标节点              |

##### MessageType_MsgRequestVoteResponse

回复 `MessageType_MsgRequestVote` 。如果当前 Term 还没有给其他节点投过票，那么可以投票给它，且将自己的 `r.Vote = m.From`。当然如果选举者日志落后肯定也要拒绝。

| 所需字段 | 作用                  |
| -------- | --------------------- |
| MsgType  | 标注类别              |
| Term     | 节点当前 term         |
| Reject   | 不同意给其投票为 true |
| To       | 目标节点              |

##### MessageType_MsgHeartbeat

Leader 发送的 Heartbeat。

| 所需字段 | 作用                  |
| -------- | --------------------- |
| MsgType  | 标注类别              |
| Term     | Leader 的 term        |
| Commit   | util.RaftInvalidIndex |
| To       | 目标节点              |

##### MessageType_MsgHeartbeatResponse

| 所需字段 | 作用          |
| -------- | ------------- |
| MsgType  | 标注类别      |
| Term     | 节点当前 term |
| To       | 目标节点      |

Leader收到后若发现commit落后，应发送 Append 进行同步。

##### MessageType_MsgTransferLeader

Local message，发起 Leader Transfer。

| 所需字段 | 作用                |
| -------- | ------------------- |
| MsgType  | 标注类别            |
| From     | Transfer 的目标节点 |

Leader 收到该请求后，设置 `r.leadTransferee = m.From`，同时检查目标节点是否已经包含了自己所有的日志，如果不包含，则启动 Append 流程进行同步，同步的过程中不再接收任何新的 propose 请求。同步完成后发送 `pb.MessageType_MsgTimeoutNow` 到目标节点。

##### MessageType_MsgTimeoutNow

目标节点收到该消息，即刻自增 term 发起选举。

| 所需字段 | 作用                |
| -------- | ------------------- |
| MsgType  | 标注类别            |
| Term     | 节点当前 term       |
| To       | Transfer 的目标节点 |

记得重置各类数据。

### RawNode

RawNode 负责raft与上层信息的交互。

RawNode 通过 HasReady() 来判断 Raft 模块是否已经有同步完成并且需要上层处理的信息。

如果 HasReady() 返回 true，那么上层就会调用 Ready() 来获取需要更新的信息。

当上层处理完 Ready 后，调用 Advance() 来推进整个状态机。

### 疑难和感想

- Raftlog中的index包括了storage中的和entries中的，是共用的index,所以访问entries中的数据时需要根据index和firstindex等计算出正确的下标

- 一开始commit在handleAppendEntriesResponse时处理的，但是这样单个节点时无法更新commit,就特殊处理了一下,在sendAppend2All中处理了

- newRaft里初始化时还要用到HardState里的信息

- 多轮选举,vote相关的数据没有清空,由于统计选票是否过半的操作是在响应voteResponse时实现的,残留的数据会影响下一轮的选举

## 2B

到这里引入了`Store` 、 `Peer`和`Region`的概念

- **Store**：每一个节点叫做一个 store，也就是一个节点上面只有一个 Store。代码里面叫 RaftStore，后面统一使用 RaftStore 代称。
- **Peer**：一个 RaftStore 里面会包含多个 peer，一个 RaftStore 里面的所有 peer 公用同一个底层存储，也就是多个 peer 公用同一个 badger 实例。
- **Region**：一个 Region 叫做一个 Raft group，即同属一个 raft 集群，一个 region 包含多个 peer，这些 peer 散落在不同的 RaftStore 上。Region 处理的是某一个范围的数据。比如 Region A 处理 $0<key<split$，Region B 处理 $split\le key < {MAX}$，两个 Region 各司其职，互不干涉，均有自己的 Leader。

一个Region在一个RaftStore中最多只有一个Peer，因为多了没有意义。一个region的所有peer中数据一致，分布在不同RaftStore是为了增加容灾性。

RaftStore 会在节点启动的时候被创建，它负责维护在该节点上所有的 region 和对应的 peer。RaftStore 使用 `storeMeta`维护该节点所有的元数据。

### PeerMsgHandler

#### proposeRaftCommand

将client的请求包装成entry发送给raft层，会传入一个`raft_cmdpb.RaftCmdRequest`,req分为Requests 和 AdminRequest 两种，此处暂时不涉及AdminRequest。

1. 通过序列化操作将请求包装为字节流通过`d.RaftGroup.Propose()`包装为entry传递给下层。

2. 同时对于每个msg创建一个proposal，`proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}`，存入proposals中。这个操作的关键在于cb，即callback。callback用于断定请求是否被执行，执行后要从proposal中调用cb.Done()响应，然后将proposal从proposals中删除。

#### HandleRaftReady

该函数用于处理rawNode传递过来的Ready。

1. 判断是否有新的 Ready，没有就什么都不处理
2. 调用 SaveReadyState 将 Ready 中需要持久化的内容保存到 badger。如果 Ready 中存在 snapshot，则应用它
3. 然后调用 d.Send() 方法将 Ready 中的 msg 发送出去
4. 应用 Ready 中的 CommittedEntries
5. 调用 d.RaftGroup.Advance() 推进 RawNode

### PeerStorage

提供了两个badger实例。

raftDB 存储：

- raftLog
- RaftLocalState

kvDB 存储：

- key-value
- RaftApplyState
- RegionLocalState

| Key              | KeyFormat                        | Value            | DB   |
| ---------------- | -------------------------------- | ---------------- | ---- |
| raft_log_key     | 0x01 0x02 region_id 0x01 log_idx | Entry            | raft |
| raft_state_key   | 0x01 0x02 region_id 0x02         | RaftLocalState   | raft |
| apply_state_key  | 0x01 0x02 region_id 0x03         | RaftApplyState   | kv   |
| region_state_key | 0x01 0x03 region_id 0x01         | RegionLocalState | kv   |

#### SaveReadyState

用于将ready中的数据持久化，函数内部需要调用下面Append函数。

#### Append

用于将Ready中的entires持久化到raftDB中，同时更新RaftLocalState

### 疑难和感想

- 报错` panic: find no region for 30203030303030303030`

    2A中newRaft初始化存在问题，`Prs` 必须从 `confState` 中取，不然根本不知道集群中有哪些 `peer`,更改后`panic`消失

- handleAppendEntries中的response中index没有完全初始化,2A中应当只包含正常返回的情况

- append之后还要更新peerstorage中的raftState数据,而且要记得`SetMeta`写进去

## 2C

在前面我们一直忽略了snapshot，本部分就要将其完成。

### 日志压缩

1. 满足压缩条件时，创建一个`CompactLogRequest`通过`d.proposeRaftCommand()`提交

2. 像普通请求一样，将 `CompactLogRequest` 请求 propose 到 Raft 中，等待 Raft Group 确认。

3. Commit 后，在 `HandleRaftReady()` 中开始 apply `CompactLogRequest` 请求。这里需要修改应用entries的部分，对`raft_cmdpb.AdminCmdType_CompactLog`进行处理。

### 发送快照

1. 当leader发现发送给follower的entries已经被压缩，就会生成一份snapshot并发送给对应节点。

2. 每个节点有pendingSnapshot字段，若发快照时发现该字段有值，直接发送pendingSnapshot。

3. `HandleRaftReady()`中应用snapshot。

4. 最后Advance()中清除pendingSnapshot。

### 疑难和感想

- `proposeRaftCommand`序列化和`HandleRaftReady`反序列化的时候两边类型没对上,序列化的是req,反序列化的是msg,一直panic,看了好久才发现