package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	nextKey []byte
	iter    engine_util.DBIterator
	txn     *MvccTxn
}

//Like KvGet, it does so at a single point in time
//所以应该是要求或许单个时间点中的所有数据,yes

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	scan := &Scanner{
		nextKey: startKey,
		txn:     txn,
		iter:    iter,
	}
	return scan
}
func (scan *Scanner) Valid() bool {
	return scan.iter.Valid()
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	//本次函数执行过程中返回的val一定对应开头的key值
	iter := scan.iter
	txn := scan.txn
	key := scan.nextKey
	iter.Seek(EncodeKey(scan.nextKey, txn.StartTS))
	if !iter.Valid() {
		return nil, nil, nil
	}
	item := iter.Item()
	userKey := DecodeUserKey(item.KeyCopy(nil))
	if !bytes.Equal(userKey, key) {
		scan.nextKey = userKey
		return scan.Next()
	} else {
		//找到下一个key的开头,所以要不停next,直到decode出来的userKey不等于key
		for {
			iter.Next()
			if !iter.Valid() {
				scan.nextKey = nil
				break
			}
			userKey2 := DecodeUserKey(iter.Item().KeyCopy(nil))
			if !bytes.Equal(userKey2, key) {
				scan.nextKey = userKey2
				break
			}
		}
	}
	//更新完了nextKey
	wVal, err := item.ValueCopy(nil)
	if err != nil {
		return key, nil, err
	}
	write, err := ParseWrite(wVal)
	if err != nil {
		return key, nil, err
	}
	if write.Kind == WriteKindDelete {
		return key, nil, nil
	}
	val, err := txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	if err != nil {
		return key, nil, err
	}
	return key, val, nil
}
