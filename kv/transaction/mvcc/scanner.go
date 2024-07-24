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
	startKey []byte
	iter     engine_util.DBIterator
	txn      MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scan := &Scanner{
		startKey: startKey,
	}
	scan.iter = txn.Reader.IterCF(engine_util.CfWrite)
	scan.iter.Seek(EncodeKey(startKey, TsMax))
	if !scan.iter.Valid() || !bytes.Equal(DecodeUserKey(scan.iter.Item().KeyCopy(nil)), startKey) {
		scan.Close()
	}
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	key := scan.iter.Item().KeyCopy(nil)
	val, err := scan.iter.Item().ValueCopy(nil)

	return nil, nil, nil
}
