- [Project1  StandaloneKV](#project1--standalonekv)
  - [实现独立存储引擎](#实现独立存储引擎)
  - [实现服务器处理函数](#实现服务器处理函数)
  - [疑难及感想](#疑难及感想)

  
# Project1  StandaloneKV

要求我们基于`badger`实现一个支持CF的单节点键值存储

分为两个部分：

- 实现单节点的引擎（实现kv/storage/standalone_storage/standalone_storage.go）

- 实现键值存储的处理函数（需要实现kv/Server/Server.go的四个基本的函数）

## 实现独立存储引擎

使用`engine_util`中提供的函数来实现`standalone_storage.go`即可

## 实现服务器处理函数

基于上一部分中编写的StandAloneStorage来实现`raw_api.go`

- RawGet： 如果获取不到，返回时要标记 `reply.NotFound=true`。
- RawPut：把单一的 put 请求用 `storage.Modify` 包装成一个数组，一次性写入。
- RawDelete：和 RawPut 同理。
- RawScan：通过 reader 获取 iter，从 StartKey 开始，同时注意 limit。

## 疑难及感想

一开始写的时候在函数内部调用了`server.storage.start`和`server.storage.close`.但是结果不对，想了一下这个应该在函数外部调用。不是功能的一部分

刚开始不熟悉GO语言,写代码效率很低,需要慢慢熟悉