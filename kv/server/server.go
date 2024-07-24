package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
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
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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
