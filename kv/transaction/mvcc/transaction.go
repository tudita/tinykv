package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}


type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

//The lock CF is accessed using the user key;
//The default CF is accessed using the user key and the start timestamp of the transaction in which it was written;
//The write CF is accessed using the user key and the commit timestamp of the transaction in which it was written;

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   EncodeKey(key, ts),
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	byte, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	lock, err := ParseLock(byte)
	if err != nil {
		return nil, nil
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   key,
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})

}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	del := storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: del})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	//key升序(1,2,3),ts降序(3,2,1)
	iter.Seek(EncodeKey(key, txn.StartTS)) //企图找到事务开始前的第一个write
	if !iter.Valid() {
		return nil, nil
	}
	item := iter.Item()
	writekey := DecodeUserKey(item.KeyCopy(nil))
	if !bytes.Equal(writekey, key) {
		return nil, nil
	}
	writeval, err := item.ValueCopy(nil)
	write, err2 := ParseWrite(writeval)
	if err != nil {
		return nil, err
	}
	if err2 != nil {
		return nil, err2
	}
	if write.Kind != WriteKindPut {
		return nil, nil
	}
	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   EncodeKey(key, txn.StartTS),
		Value: value,
		Cf:    engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	del := storage.Delete{
		Key: EncodeKey(key, txn.StartTS),
		Cf:  engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: del})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	for iter.Seek(EncodeKey(key, TsMax)); iter.Valid(); iter.Next() { //从这个该key的第一个write开始找
		writekey := DecodeUserKey(iter.Item().KeyCopy(nil))
		if !bytes.Equal(writekey, key) {
			//说明该key里没有这个时间戳
			return nil, 0, nil
		}
		writeval, err := iter.Item().ValueCopy(nil)
		if err != nil || writeval == nil {
			return nil, 0, err
		}
		write, err := ParseWrite(writeval)
		if err != nil || write == nil {
			return nil, 0, err
		}
		if write.StartTS == txn.StartTS {
			//满足函数要求的时间戳条件
			return write, decodeTimestamp(iter.Item().KeyCopy(nil)), nil
		}
		if write.StartTS < txn.StartTS {
			break
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(key, TsMax))
	if !iter.Valid() {
		return nil, 0, nil
	}
	item := iter.Item()
	if !bytes.Equal(DecodeUserKey(item.KeyCopy(nil)), key) {
		return nil, 0, nil
	}
	ts := decodeTimestamp(item.KeyCopy(nil))
	writeval, err := item.ValueCopy(nil)
	if writeval == nil || err != nil {
		return nil, 0, err
	}
	write, err := ParseWrite(writeval)
	if err != nil || write == nil {
		return nil, 0, err
	}
	return write, ts, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
