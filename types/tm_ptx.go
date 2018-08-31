package types

import (
	"bytes"
	"sync/atomic"
	"io"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/common"
)

type VNode struct {
	Data interface{} //common.Hash
}

type Dag struct {
	Nodes [][]*VNode
}

// ParalleledTransaction.
type ParalleledTransaction struct {
	Data            ParalleledTransactionData

	// cache
	hash            atomic.Value
	size            atomic.Value
}

type ParalleledTransactionData struct {
	TxIds       [][32]byte
	Txs         [][]byte
	Dag         *Dag
}

// DecodeRLP implements rlp.Decoder
func (tx *ParalleledTransaction) DecodeRLP(s *rlp.Stream) error {
	_, size, _ := s.Kind()
	err := s.Decode(&tx.Data)
	if err == nil {
		tx.size.Store(common.StorageSize(rlp.ListSize(size)))
	}

	return err
}

func (tx *ParalleledTransaction) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &tx.Data)
}

func DecodePtx(txBytes []byte) (*ParalleledTransaction, error, int) {
	ptx := new(ParalleledTransaction)
	rlpStream := rlp.NewStream(bytes.NewBuffer(txBytes), 0)
	if err := ptx.DecodeRLP(rlpStream); err != nil {
		return nil, err, 0
	}
	txLen := 0
	if (len(ptx.Data.TxIds) > 0) {
		txLen = len(ptx.Data.TxIds)
	} else {
		txLen = len(ptx.Data.Txs)
	}
	return ptx, nil, txLen
}

// covert ptx in tx hash to ptx in raw tx
func EncodePtx(txBytes []byte, ptxs [][]byte) ([]byte, error) {
	ptx := new(ParalleledTransaction)
	rlpStream := rlp.NewStream(bytes.NewBuffer(txBytes), 0)
	if err := ptx.DecodeRLP(rlpStream); err != nil {
		return nil, err
	}
	ptx.Data.TxIds = nil
	ptx.Data.Txs = ptxs
	buf := new(bytes.Buffer)
	if err := ptx.EncodeRLP(buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}