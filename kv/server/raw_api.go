package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, _ := server.storage.Reader(req.Context)
	value, error := reader.GetCF(req.GetCf(), req.GetKey())
	res := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: false,
	}
	if value == nil {
		res.NotFound = true
	}
	if error != nil {
		res.Error = error.Error()
	}
	return res, error
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	//函数中不需要调用server.storage.start和close
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	mod := storage.Modify{Data: put}
	res := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(req.Context, []storage.Modify{mod})
	if err != nil {
		res.Error = err.Error()
	}
	return res, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	del := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	mod := storage.Modify{Data: del}
	err := server.storage.Write(req.Context, []storage.Modify{mod})
	res := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		res.Error = err.Error()
	}
	return res, err

}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	reader, error := server.storage.Reader(req.GetContext())
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())
	var pairs []*kvrpcpb.KvPair
	for i := 0; i < int(req.GetLimit()) && iter.Valid(); i++ {
		item := iter.Item()
		value, _ := item.Value()
		pairs = append(pairs, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: pairs,
	}, error

}
