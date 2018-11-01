package mempool

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	abci "github.com/tendermint/abci/types"
	wire "github.com/tendermint/go-wire"
	"github.com/tendermint/tmlibs/clist"
	"github.com/tendermint/tmlibs/log"

	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/types"
	"gopkg.in/fatih/set.v0"
	// "github.com/ethereum/go-ethereum/common"
)

const (
	MempoolChannel = byte(0x30)

	maxKnownTxs      = 32768
	maxMempoolMessageSize      = 1048576 // 1MB TODO make it configurable
	peerCatchupSleepIntervalMS = 100     // If peer is behind, sleep this amount
)

// MempoolReactor handles mempool tx broadcasting amongst peers.
type MempoolReactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	Mempool *Mempool
	// requestingTx [][]byte
}

// NewMempoolReactor returns a new MempoolReactor with the given config and mempool.
func NewMempoolReactor(config *cfg.MempoolConfig, mempool *Mempool) *MempoolReactor {
	memR := &MempoolReactor{
		config:  config,
		Mempool: mempool,
	}
	// memR.requestingTx = make([][]byte, 0)
	memR.BaseReactor = *p2p.NewBaseReactor("MempoolReactor", memR)
	return memR
}

// SetLogger sets the Logger on the reactor and the underlying Mempool.
func (memR *MempoolReactor) SetLogger(l log.Logger) {
	memR.Logger = l
	memR.Mempool.SetLogger(l)
}

// GetChannels implements Reactor.
// It returns the list of channels for this reactor.
func (memR *MempoolReactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:       MempoolChannel,
			Priority: 5,
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (memR *MempoolReactor) AddPeer(peer p2p.Peer) {
	memR.Logger.Info("*************************************")
	memR.Logger.Info("************Add peer ************")
	peerSet := set.New()
	peer.Set(types.PeerMempoolChKey, peerSet)
	memR.Logger.Info("*************************************")
	go memR.broadcastTxRoutine(peer)
}

// RemovePeer implements Reactor.
func (memR *MempoolReactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	// broadcast routine checks if peer is gone and returns
	// TODO: remove the peer in knownTxs or not
}

// SendTxMessage response peer GetTxMessage and send back to peer
func (memR *MempoolReactor) SendTxMessage(hash []byte, tx types.Tx, src p2p.Peer) {
	msg := &TxMessage{Hash: hash, Tx: tx}
	success := src.Send(MempoolChannel, struct{ MempoolMessage }{msg})
	if !success {
		memR.Logger.Error("Send tx", tx, "to peer", src, "not success")
	}
}

// MarkTransaction marks a transaction as known for the peer, ensuring that it
// will never be propagated to this particular peer.
func (memR *MempoolReactor) MarkTransaction(peerSet *set.Set, tx []byte) {
	// If we reached the memory allowance, drop a previously known transaction hash
	for peerSet.Size() >= maxKnownTxs {
		peerSet.Pop()
	}
	peerSet.Add(string(tx))
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *MempoolReactor) Receive(chID byte, src p2p.Peer, msgBytes []byte) {
	_, msg, err := DecodeMessage(msgBytes)
	if err != nil {
		memR.Logger.Error("Error decoding message", "err", err)
		return
	}
	memR.Logger.Debug("Receive", "src", src, "chId", chID, "msg", msg)
	peerSet := src.Get(types.PeerMempoolChKey).(*set.Set)

	switch msg := msg.(type) {
	case *TxMessage:
		memR.MarkTransaction(peerSet, msg.Tx)
		// hash := types.BytesToHash(msg.Hash)
		err := memR.Mempool.CheckTx(msg.Tx, msg.Hash, types.RawTx, false, nil)
		if err != nil {
			memR.Logger.Info("Could not check tx", "tx", msg.Tx, "err", err)
		}
		// broadcasting happens from go routines per peer
	case *GetTxMessage:
		for _, hash := range msg.Hash {
			_, tx := memR.Mempool.GetTx(hash, types.RawTxHash, types.RawTx)
			if (tx == nil) {
				txHash := types.BytesToHash(hash)
				memR.Logger.Error("GetTxMessage:Can not find tx", "hash", txHash)
				continue
			}
			// txs = append(txs, tx)
			// TODO: send one by one or send together
			go memR.SendTxMessage(hash, tx, src)
		}
	default:
		memR.Logger.Error(fmt.Sprintf("Unknown message type %v", reflect.TypeOf(msg)))
	}
}

// BroadcastTx is an alias for Mempool.CheckTx. Broadcasting itself happens in peer routines.
func (memR *MempoolReactor) BroadcastTx(tx types.Tx, txhash types.CommonHash, cb func(*abci.Response)) error {
	return memR.Mempool.CheckTx(tx, txhash, types.RawTx, false, cb)
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

// Send new mempool txs to peer.
// TODO: Handle mempool or reactor shutdown?
// As is this routine may block forever if no new txs come in.
func (memR *MempoolReactor) broadcastTxRoutine(peer p2p.Peer) {
	if !memR.config.Broadcast {
		return
	}
	peerSet := peer.Get(types.PeerMempoolChKey).(*set.Set)

	var next *clist.CElement
	for {
		if !memR.IsRunning() || !peer.IsRunning() {
			return // Quit!
		}
		if next == nil {
			// This happens because the CElement we were looking at got
			// garbage collected (removed).  That is, .NextWait() returned nil.
			// Go ahead and start from the beginning.
			next = memR.Mempool.TxsFrontWait() // Wait until a tx is available
		}
		memTx := next.Value.(*mempoolTx)
		// make sure the peer is up to date
		height := memTx.Height()
		if peerState_i := peer.Get(types.PeerStateKey); peerState_i != nil {
			peerState := peerState_i.(PeerState)
			if peerState.GetHeight() < height-1 { // Allow for a lag of 1 block
				time.Sleep(peerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}
		}
		if (peerSet.Has(string(memTx.tx))) {
			memR.Logger.Debug("*************************************")
			memR.Logger.Debug("peer have the same tx", memTx.tx, "dont need send")
			memR.Logger.Debug("*************************************")
			time.Sleep(peerCatchupSleepIntervalMS * time.Millisecond)
			next = next.NextWait()
			continue
		} else {
			memR.Logger.Debug("*************************************")
			memR.Logger.Debug("peer dont have the same tx", memTx.tx, "send it")
			memR.Logger.Debug("*************************************")
		}
		// send memTx
		msg := &TxMessage{Hash: nil, Tx: memTx.tx}
		success := peer.Send(MempoolChannel, struct{ MempoolMessage }{msg})
		if !success {
			time.Sleep(peerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		next = next.NextWait()
		continue
	}
}

//-----------------------------------------------------------------------------
// Messages

const (
	msgTypeTx = byte(0x01)
	msgTypeGetTx = byte(0x02)
)

// MempoolMessage is a message sent or received by the MempoolReactor.
type MempoolMessage interface{}

var _ = wire.RegisterInterface(
	struct{ MempoolMessage }{},
	wire.ConcreteType{&TxMessage{}, msgTypeTx},
	wire.ConcreteType{&GetTxMessage{}, msgTypeGetTx},
)

// DecodeMessage decodes a byte-array into a MempoolMessage.
func DecodeMessage(bz []byte) (msgType byte, msg MempoolMessage, err error) {
	msgType = bz[0]
	n := new(int)
	r := bytes.NewReader(bz)
	msg = wire.ReadBinary(struct{ MempoolMessage }{}, r, maxMempoolMessageSize, n, &err).(struct{ MempoolMessage }).MempoolMessage
	return
}

//-------------------------------------

// TxMessage is a MempoolMessage containing a transaction.
type TxMessage struct {
	Hash []byte
	Tx types.Tx
}

// GetTxMessage is a MempoolMessage containing a transaction hash.
type GetTxMessage struct {
	Hash [][]byte
}

// String returns a string representation of the TxMessage.
func (m *TxMessage) String() string {
	return fmt.Sprintf("[TxMessage %v]", m.Tx)
}
