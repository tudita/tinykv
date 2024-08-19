- [3A](#3a)
  - [疑难和感想](#疑难和感想)
- [3B](#3b)
  - [LeaderTransfer](#leadertransfer)
  - [Add/Remove Node](#addremove-node)
  - [Split](#split)
    - [触发 Split 流程](#触发-split-流程)
    - [Apply split](#apply-split)
  - [疑难和感想](#疑难和感想-1)
- [3C](#3c)
  - [processRegionHeartbeat()](#processregionheartbeat)
  - [Schedule()](#schedule)
  - [疑难和感想](#疑难和感想-2)

## 3A

本部分需要在raft层支持LeaderTransfer和Add/Remove node操作。

LeaderTransfer有关的消息在之前介绍各类Msg时已经提到过。

对于Add/Remove 操作，只需要操作r.Prs[]，较为简单，唯一需要注意的是删除节点后可能会导致“大多数”发生变化，需要同步一下commit。

### 疑难和感想

- LeaderTransfer不为None时,说明还处于领导者变更阶段，不能接受任何propose;同时要记得重置leadTransferee,否则永远不会接受propose

- MessageType_MsgTransferLeader可能会发给follower,要处理这种情况，把message重新发给leader

- 发现在测试中会有一个raft的peers中不包含自己的情况，想象不出来为啥会有这种情况，只是添加了对这个情况的处理。

## 3B

先说一下RegionEpoch是什么。

RegionEpoch本质就是一个版本号，由ConfVer和Version组成。他的更新规则如下：

1. 配置变更的时候， `conf_ver` + 1。
2. Split 的时候，原 region 与新 region 的 `version` 均等于原 region 的 `version` + 新 region 个数。
3. Merge 的时候，两个 region 的 `version` 均等于这两个 region 的 `version` 最大值 + 1。

### LeaderTransfer

这是一种Admin指令,直接调用d.RaftGroup.TransferLeader() 方法即可，它会通过 Step() 传递一个 MsgTransferLeader 到 raft 层中。

### Add/Remove Node

与其他propose不同，这两个指令要通过`d.RaftGroup.ProposeConfChange()`提交

在Apply时：

1. 调用 `d.Region()读取原有的 region
2. 修改 `region.Peers`，是删除就删除，是增加就增加一个 peer。如果删除的目标节点正好是自己本身，那么直接调用 `d.destroyPeer()` 方法销毁自己，并直接 return。
3. 设置 `region.RegionEpoch.ConfVer++`。
4. 持久化修改后的 Region，写到 kvDB 里面。使用 `meta.WriteRegionState()` 方法。注意使用的是 `rspb.PeerState_Normal`，因为其要正常服务请求的。
5. 调用 `d.insertPeerCache()` 或 `d.removePeerCache()` 方法，这决定了你的消息是否能够正常发送，`peer.go` 里面的 `peerCache`注释上说明了为什么这么做。
6. 调用 `d.RaftGroup.ApplyConfChange()` 方法，因为刚刚修改的是 RawNode 上层的 peers 信息，Raft 内部的 peers 还没有修改。
7. 调用 Raft 的 `addNode()` 或 `removeNode()` 方法
8. 调用 `notifyHeartbeatScheduler()` 方法，该方法用于快速刷新 scheduler 那里的 region 缓存

### Split

当单个region容量超出阈值时，就要触发split操作，降低单个regiond存储压力。按splitKey将region一分为二。

#### 触发 Split 流程

1. `peer_msg_handler.go` 中的 `onTick()` 定时检查，调用 `onSplitRegionCheckTick()` 方法，它会生成一个 `SplitCheckTask` 任务发送到 `split_checker.go` 中。
2. 检查时如果发现满足 split 要求，则生成一个 `MsgTypeSplitRegion` 请求。
3. 在 `peer_msg_handler.go` 中的 `HandleMsg()` 方法中调用 `onPrepareSplitRegion()`，发送 `SchedulerAskSplitTask` 请求到 `scheduler_task.go` 中，申请其分配新的 region id 和 peer id。申请成功后其会发起一个 `AdminCmdType_Split` 的 AdminRequest 到 region 中。
4. 之后就和接收普通 AdminRequest 一样，propose 等待 apply。注意 propose 的时候检查 splitKey 是否在目标 region 中和 regionEpoch 是否为最新，因为目标 region 可能已经产生了分裂。

#### Apply split

1. 基于原来的 region clone 一个新 region，这里把原来的 region 叫 leftRegion，新 region 叫 rightRegion。
2. 更新两个region的版本号,Key等等信息。
3. 持久化 leftRegion 和 rightRegion 的信息。
4. 通过 `createPeer()` 方法创建新的 peer 并注册进 router，同时发送 `message.MsgTypeStart` 启动 peer。
5. 更新 storeMeta 里面的 regionRanges，同时使用 `storeMeta.setRegion()` 进行设置。注意加锁。
6. 调用 `d.notifyHeartbeatScheduler()`

### 疑难和感想

- Transfer不需要propose,直接调用TransferLeader后`cb.Done`就行

- 要更改`RegionLocalState` ，包括`RegionEpoch`和`Region`中的`Peers`,修改后通过kvWB写入
  
  还要更新`GlobalContext`的`storeMeta`中的区域状态,这个是Global的,操作前要上锁.

- confChange通过`raft.RawNode.ProposeConfChange`来propose,需要输入一个`pb.ConfChange`,`req`可以序列化后存入`Context`

- `BasicConfChange`中一直Timeout,发现是`sendHeartBeat`实现有问题,`MsgHeartBeat`的`commit`字段必须为`util.RaftInvalidIndex`,否则无法通过初始化检查。
  
  ```go
  func IsInitialMsg(msg *eraftpb.Message) bool {
  	return msg.MsgType == eraftpb.MessageType_MsgRequestVote ||
  		// the peer has not been known to this leader, it may exist or not.
  		(msg.MsgType == eraftpb.MessageType_MsgHeartbeat && msg.Commit == RaftInvalidIndex)
  }
  ```
  
  

- 若干个测试均出现` x meta corruption detected `,查看报错发现问题出现在`destroyPeer`
  
  ```go
  if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
      panic(d.Tag + " meta corruption detected, isInitialized is " + strconv.FormatBool(isInitialized))
  }
  ```
  
  怀疑是`addNode`时`regionRanges`未更新,检查后发现`addNode`实现没有问题，于是在执行`addNode`时输出日志，结果发现每次addnode时都连续两次输出日志。回头检查发现raft.go中`PendingConfIndex`字段没有用上，填写后此类错误消失。

- 执行完confChange后要判断d.stopped，因为有可能把自己删了。

- apply`raft_cmdpb.CmdType_Snap`时，response中返回的region不能直接返回指针`d.region()`,需要拷贝一份。因为scan请求中会反复CheckKeyInRegion(key, region),而region是指针,直接传入的话，如果中途发生了split，会报错

## 3C

这部分实现一个上层的调度器。

### processRegionHeartbeat()

在 `processRegionHeartbeat()` 收到汇报来的心跳，先检查一下 RegionEpoch 是否是最新的，如果是新的则调用 `c.putRegion()` 和 `c.updateStoreStatusLocked()` 进行更新。

不然就看情况返回err

### Schedule()

这一部分主要负责 region 的调度，从 region size 最大的 store 中取出一个 region 放到 region size 最小的 store 中。

具体流程提供的官方文档中写得十分清楚。

### 疑难和感想

- 文档中提到用`RaftCluster.core.PutRegion`等函数更新本地储存,实际上`RaftCluster`的成员函数中又对这些函数的包装,这些函数进行了上锁,可以直接使用.

- `processRegionHeartbeat`中获取region的RegionEpoch,有为nil的情况.开始因为找没找到合适的err,这种情况我随便返回了一个`errors.Errorf`,结果运行五十次有六次一个测试点FAIL;改为返回nil后,五十次测试全部通过.
  
  这背后的意义我还不是很明确

- Balance操作
  
  - 如果找到的待移动的`region`小于`GetMaxReplicas`,不必balance,没有必要
  - 按size从小到大找targetStore时,如果一个store中包含所转移region的peer,那么略过.因为同一个store中包含两个相同的peer是没有意义的