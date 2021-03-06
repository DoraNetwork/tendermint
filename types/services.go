package types

import (
	abci "github.com/tendermint/abci/types"
)

// NOTE/XXX: all type definitions in this file are considered UNSTABLE

//------------------------------------------------------
// blockchain services types
// NOTE: Interfaces used by RPC must be thread safe!
//------------------------------------------------------

//------------------------------------------------------
// mempool

// Mempool defines the mempool interface as used by the ConsensusState.
// Updates to the mempool need to be synchronized with committing a block
// so apps can reset their transient state on Commit
// UNSTABLE
type Mempool interface {
	Lock()
	Unlock()

	Size() int
	CheckTx(Tx, CommonHash, int32, bool, func(*abci.Response)) error
	Reap(int) Txs
	Update(height int64, txs Txs) error
	Restore(height int64)
	Flush()

	TxsAvailable() <-chan int64
	EnableTxsAvailable()

	TxResponsed() <-chan int64

	GetTx(height int64, tx []byte, from int32, to int32) (bool, Tx)
	TxsFetching() <-chan RequestTxMsg
}

// MockMempool is an empty implementation of a Mempool, useful for testing.
// UNSTABLE
type MockMempool struct {
}

func (m MockMempool) Lock()                                                   {}
func (m MockMempool) Unlock()                                                 {}
func (m MockMempool) Size() int                                               { return 0 }
func (m MockMempool) CheckTx(tx Tx, hash CommonHash, flag int32, local bool, cb func(*abci.Response)) error { return nil }
func (m MockMempool) Reap(n int) Txs                                          { return Txs{} }
func (m MockMempool) Update(height int64, txs Txs) error                      { return nil }
func (m MockMempool) Restore(height int64)                      			  {}
func (m MockMempool) Flush()                                                  {}
func (m MockMempool) TxsAvailable() <-chan int64                              { return make(chan int64) }
func (m MockMempool) EnableTxsAvailable()                                     {}
func (m MockMempool) TxResponsed() <-chan int64                               { return make(chan int64) }
func (m MockMempool) GetTx(height int64, tx []byte, from int32, to int32) (bool, Tx)        { return true, nil }
func (m MockMempool) TxsFetching() <-chan RequestTxMsg                        { return make(chan RequestTxMsg) }

//------------------------------------------------------
// blockstore

// BlockStoreRPC is the block store interface used by the RPC.
// UNSTABLE
type BlockStoreRPC interface {
	Height() int64

	LoadBlockMeta(height int64) *BlockMeta
	LoadBlock(height int64) *Block
	LoadBlockPart(height int64, index int) *Part

	LoadBlockCommit(height int64) *Commit
	LoadSeenCommit(height int64) *Commit
}

// BlockStore defines the BlockStore interface used by the ConsensusState.
// UNSTABLE
type BlockStore interface {
	BlockStoreRPC
	SaveBlock(block *Block, blockParts *PartSet, seenCommit *Commit)
}

//------------------------------------------------------
// evidence pool

// EvidencePool defines the EvidencePool interface used by the ConsensusState.
// UNSTABLE
type EvidencePool interface {
	PendingEvidence() []Evidence
	AddEvidence(Evidence) error
	Update(*Block)
}

// MockMempool is an empty implementation of a Mempool, useful for testing.
// UNSTABLE
type MockEvidencePool struct {
}

func (m MockEvidencePool) PendingEvidence() []Evidence { return nil }
func (m MockEvidencePool) AddEvidence(Evidence) error  { return nil }
func (m MockEvidencePool) Update(*Block)               {}
