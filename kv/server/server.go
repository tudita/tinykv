package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	keys = append(keys, req.Key)
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	response := &kvrpcpb.GetResponse{
		NotFound: true,
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return response, err
	}
	if lock != nil && lock.Ts <= txn.StartTS { //假如这个req被锁住了
		response.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return response, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return response, err
	}
	if value != nil {
		response.Value = value
		response.NotFound = false
	}
	return response, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	response := &kvrpcpb.PrewriteResponse{}
	mutations := req.Mutations
	var keys [][]byte
	for _, mutation := range mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	for _, key := range keys {
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			return response, err
		}
		if write != nil && commitTs >= req.GetStartVersion() {
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					Key:        key,
					StartTs:    req.GetStartVersion(),
					ConflictTs: commitTs,
					Primary:    req.PrimaryLock,
				},
			})
			return response, nil
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return response, err
		}
		if lock != nil {
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			})
			return response, nil
		}

	}
	for _, mutation := range mutations {
		lock := mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.GetStartVersion(),
			Ttl:     req.LockTtl,
		}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.Key, mutation.Value)
			lock.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.Key)
			lock.Kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Lock:
		case kvrpcpb.Op_Rollback:
			lock.Kind = mvcc.WriteKindRollback

		}
		txn.PutLock(mutation.Key, &lock)
	}
	server.storage.Write(req.Context, txn.Writes())
	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	response := &kvrpcpb.CommitResponse{}
	keys := req.Keys
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return response, err
		}
		if lock == nil {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return response, nil
			}
			if write == nil {
				return response, nil
			}
			if write.Kind == mvcc.WriteKindRollback {
				response.Error = &kvrpcpb.KeyError{
					Retryable: "true",
				}
				return response, nil
			}
			return response, nil
		}
		if lock.Ts != txn.StartTS {
			response.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return response, nil
		}
	}
	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return response, err
		}
		write := mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		}
		txn.PutWrite(key, req.CommitVersion, &write)
		txn.DeleteLock(key)
	}
	server.storage.Write(req.Context, txn.Writes())
	return response, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	if err != nil {
		return response, err
	}
	scan := mvcc.NewScanner(req.StartKey, txn)
	defer scan.Close()
	for i := 0; i < int(req.GetLimit()); i++ {
		if !scan.Valid() {
			break
		}
		key, value, err := scan.Next()
		if err != nil {
			return response, err
		}
		if key == nil {
			continue
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return response, err
		}
		if lock != nil && lock.Ts < txn.StartTS {
			response.Pairs = append(response.Pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			})
			continue
		}
		if value != nil {
			response.Pairs = append(response.Pairs, &kvrpcpb.KvPair{
				Key:   key,
				Value: value})
		}
	}
	return response, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).

	response := &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_NoAction}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return response, err
	}
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		//已经commit
		response.CommitVersion = commitTs
		response.Action = kvrpcpb.Action_NoAction
		return response, nil
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return response, err
	}
	//没commit
	if lock == nil { //被回滚了但是没记录
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			response.Action = kvrpcpb.Action_NoAction
			response.CommitVersion = 0
			response.LockTtl = 0
			return response, nil
		} else {
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			txn.DeleteValue(req.PrimaryKey)
			err := server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				return response, err
			}
			response.Action = kvrpcpb.Action_LockNotExistRollback
			response.CommitVersion = 0
			response.LockTtl = 0
			return response, nil
		}
	} else if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		//lock过期了
		//fmt.Printf("lock.ttl:%d\n", lock.Ttl)
		//fmt.Printf("lock.ts:%d\n", lock.Ts)
		//fmt.Printf("current.ts:%d\n", req.CurrentTs)
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return response, err
		}
		response.Action = 1
		response.CommitVersion = 0
		response.LockTtl = 0
		return response, nil

	}
	response.LockTtl = lock.Ttl
	return response, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).

	response := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, err
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {

		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return response, err
		}
		//已经回滚了
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}
		//已经commit了
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			response.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			return response, nil
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return response, err
		}

		if lock != nil && lock.Ts != txn.StartTS { //该key被其他事务的lock锁住,中止操作,可以以后重试
			//依然要给回滚标记
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}

		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())

	if err != nil {
		return response, err
	}
	return response, nil
}

// 处理StartVersion这个事务(时间戳唯一标识一个事务)的的锁冲突情况,commit or rollback
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).

	var keys [][]byte
	response := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := txn.Reader.IterCF(engine_util.CfLock)
	for ; iter.Valid(); iter.Next() {
		//为什么要满足时间戳相等
		item := iter.Item()
		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			return response, err
		}
		lock, err := mvcc.ParseLock(val)
		if err != nil {
			return response, err
		}
		if lock.Ts == txn.StartTS {
			//lock存储的key不附带时间戳
			keys = append(keys, key)
		}
	}
	if req.GetCommitVersion() == 0 {
		//primaryKey滚了,应该全部回滚
		res, err := server.KvBatchRollback(context.Background(), &kvrpcpb.BatchRollbackRequest{StartVersion: req.StartVersion, Keys: keys, Context: req.Context})
		if err != nil {
			return response, err
		}
		response.Error = res.Error
		response.RegionError = res.RegionError
		return response, nil
	} else {
		//提交了,那就提交
		res, err := server.KvCommit(context.Background(), &kvrpcpb.CommitRequest{StartVersion: req.StartVersion, Keys: keys, Context: req.Context, CommitVersion: req.GetCommitVersion()})
		if err != nil {
			return response, err
		}
		response.Error = res.Error
		response.RegionError = res.RegionError
		return response, nil
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
