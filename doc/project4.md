# Project 4

提供了三种CF：`default`：记录values  `write`：记录操作  `lock`：记录锁

每个事务以开始时间戳唯一标识（唯一的）

每个事务和一个primary key挂钩，事务如何处理取决于primary key如何处理。其他key参照primary key的操作来进行。

- txn储存完所有的修改之后记得将修改写入storage

- `KvCheckTxnStatus`的几种情况：（结合前面的`prewrite`和`commit`）
  
  - 已经commit（能查询到非rollback的write），NoAction
  
  - 无锁，已经回滚，NoAction
  
  - 无锁，未回滚，LockNotExistRollback
  
  - 有锁，锁超时，TTLExpireRollback

- `getvalue`之前要查询这之前的`write`，如果是`put`操作才有值

- 记得在每次rollback后将修改写入storage,或者独立成一个函数比较好