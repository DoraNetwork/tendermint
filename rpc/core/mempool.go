package core

import (
	"context"
	"fmt"
	"time"
	"bytes"

	"github.com/pkg/errors"

	abci "github.com/tendermint/abci/types"
	data "github.com/tendermint/go-wire/data"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	"github.com/tendermint/tendermint/types"
	wire "github.com/tendermint/go-wire"
)

type MemAppTxMessage types.AppTxMessage

// DecodeMessage decodes a byte-array into a AppTxMessage.
func DecodeMessage(bz []byte) (msgType byte, msg MemAppTxMessage, err error) {
	msgType = bz[0]
	n := new(int)
	r := bytes.NewReader(bz)
	// TODO: define max tx size
	msg = wire.ReadBinary(struct{ MemAppTxMessage }{}, r, 10240, n, &err).(struct{ MemAppTxMessage }).MemAppTxMessage
	return
}

//-----------------------------------------------------------------------------
// NOTE: tx should be signed, but this is only checked at the app level (not by Tendermint!)

// Returns right away, with no response
//
// ```shell
// curl 'localhost:46657/broadcast_tx_async?tx="123"'
// ```
//
// ```go
// client := client.NewHTTP("tcp://0.0.0.0:46657", "/websocket")
// result, err := client.BroadcastTxAsync("123")
// ```
//
// > The above command returns JSON structured like this:
//
// ```json
// {
// 	"error": "",
// 	"result": {
// 		"hash": "E39AAB7A537ABAA237831742DCE1117F187C3C52",
// 		"log": "",
// 		"data": "",
// 		"code": 0
// 	},
// 	"id": "",
// 	"jsonrpc": "2.0"
// }
// ```
//
// ### Query Parameters
//
// | Parameter | Type | Default | Required | Description     |
// |-----------+------+---------+----------+-----------------|
// | tx        | Tx   | nil     | true     | The transaction |
func BroadcastTxAsync(tx types.Tx) (*ctypes.ResultBroadcastTx, error) {
	// _, msg, err := DecodeMessage(tx)
	// if err != nil {
	// 	fmt.Errorf("Error decoding message", "err", err)
	// 	return nil, nil
	// }
	// switch msg := msg.(type) {
	// 	case *(types.TxMessage):
	// 		err = mempool.CheckTx(msg.Tx, types.RawTx, nil)
	// 	case *(types.ParallelTxMessage):
	// 		err = mempool.CheckTx(msg.Tx, types.ParallelTx, nil)
	// 	case *(types.ParallelTxHashMessage):
	// 		err = mempool.CheckTx(msg.Tx, types.ParallelTxHash, nil)
	// }
	err := mempool.CheckTx(tx, nil, types.RawTx, true, nil)
	if err != nil {
		return nil, fmt.Errorf("Error broadcasting transaction: %v", err)
	}
	return &ctypes.ResultBroadcastTx{Hash: tx.Hash()}, nil
}

// Returns with the response from CheckTx.
//
// ```shell
// curl 'localhost:46657/broadcast_tx_sync?tx="456"'
// ```
//
// ```go
// client := client.NewHTTP("tcp://0.0.0.0:46657", "/websocket")
// result, err := client.BroadcastTxSync("456")
// ```
//
// > The above command returns JSON structured like this:
//
// ```json
// {
// 	"jsonrpc": "2.0",
// 	"id": "",
// 	"result": {
// 		"code": 0,
// 		"data": "",
// 		"log": "",
// 		"hash": "0D33F2F03A5234F38706E43004489E061AC40A2E"
// 	},
// 	"error": ""
// }
// ```
//
// ### Query Parameters
//
// | Parameter | Type | Default | Required | Description     |
// |-----------+------+---------+----------+-----------------|
// | tx        | Tx   | nil     | true     | The transaction |
func BroadcastTxSync(tx types.Tx, txtype int32) (*ctypes.ResultBroadcastTx, error) {
	resCh := make(chan *abci.Response, 1)
	// _, msg, err := DecodeMessage(tx)
	// if err != nil {
	// 	fmt.Errorf("Error decoding message", "err", err)
	// 	return nil, nil
	// }
	// switch msg := msg.(type) {
	// 	case *(types.TxMessage):
	// 		txType = types.RawTx
	// 		tx = msg.Tx
	// 	case *(types.ParallelTxMessage):
	// 		txType = types.ParallelTx
	// 		tx = msg.Tx
	// 	case *(types.ParallelTxHashMessage):
	// 		txType = types.ParallelTxHash
	// 		tx = msg.Tx
	// }
	err := mempool.CheckTx(tx, nil, txtype, true, func(res *abci.Response) {
		resCh <- res
	})
	if err != nil {
		return nil, fmt.Errorf("Error broadcasting transaction: %v", err)
	}
	if (txtype != types.RawTx) {
		return &ctypes.ResultBroadcastTx{
			Code: 0,
			Data: nil,
			Log:  "",
			Hash: tx.Hash(),
		}, nil		// TODO: check return value if ok
	}
	res := <-resCh
	r := res.GetCheckTx()
	return &ctypes.ResultBroadcastTx{
		Code: r.Code,
		Data: r.Data,
		Log:  r.Log,
		Hash: tx.Hash(),
	}, nil
}

// CONTRACT: only returns error if mempool.BroadcastTx errs (ie. problem with the app)
// or if we timeout waiting for tx to commit.
// If CheckTx or DeliverTx fail, no error will be returned, but the returned result
// will contain a non-OK ABCI code.
//
// ```shell
// curl 'localhost:46657/broadcast_tx_commit?tx="789"'
// ```
//
// ```go
// client := client.NewHTTP("tcp://0.0.0.0:46657", "/websocket")
// result, err := client.BroadcastTxCommit("789")
// ```
//
// > The above command returns JSON structured like this:
//
// ```json
// {
// 	"error": "",
// 	"result": {
// 		"height": 26682,
// 		"hash": "75CA0F856A4DA078FC4911580360E70CEFB2EBEE",
// 		"deliver_tx": {
// 			"log": "",
// 			"data": "",
// 			"code": 0
// 		},
// 		"check_tx": {
// 			"log": "",
// 			"data": "",
// 			"code": 0
// 		}
// 	},
// 	"id": "",
// 	"jsonrpc": "2.0"
// }
// ```
//
// ### Query Parameters
//
// | Parameter | Type | Default | Required | Description     |
// |-----------+------+---------+----------+-----------------|
// | tx        | Tx   | nil     | true     | The transaction |
// TODO: ptx dont need subscribe or wait include in block ...
func BroadcastTxCommit(tx types.Tx) (*ctypes.ResultBroadcastTxCommit, error) {
	// _, msg, err := DecodeMessage(tx)
	// if err != nil {
	// 	fmt.Errorf("Error decoding message", "err", err)
	// 	return nil, nil
	// }
	txType := types.RawTx
	// subscribe to tx being committed in block
	ctx, cancel := context.WithTimeout(context.Background(), subscribeTimeout)
	defer cancel()
	deliverTxResCh := make(chan interface{})
	q := types.EventQueryTxFor(tx)
	err := eventBus.Subscribe(ctx, "mempool", q, deliverTxResCh)
	if err != nil {
		err = errors.Wrap(err, "failed to subscribe to tx")
		logger.Error("Error on broadcastTxCommit", "err", err)
		return nil, fmt.Errorf("Error on broadcastTxCommit: %v", err)
	}
	defer eventBus.Unsubscribe(context.Background(), "mempool", q)

	// broadcast the tx and register checktx callback
	checkTxResCh := make(chan *abci.Response, 1)
	// switch msg := msg.(type) {
	// 	case *(types.TxMessage):
	// 		txType = types.RawTx
	// 		tx = msg.Tx
	// 	case *(types.ParallelTxMessage):
	// 		txType = types.ParallelTx
	// 		tx = msg.Tx
	// 	case *(types.ParallelTxHashMessage):
	// 		txType = types.ParallelTxHash
	// 		tx = msg.Tx
	// }
	err = mempool.CheckTx(tx, nil, txType, true, func(res *abci.Response) {
		checkTxResCh <- res
	})
	if err != nil {
		logger.Error("Error on broadcastTxCommit", "err", err)
		return nil, fmt.Errorf("Error on broadcastTxCommit: %v", err)
	}
	if (txType != types.RawTx) {
		return nil, nil	//TODO: check return if ok
	}
	checkTxRes := <-checkTxResCh
	checkTxR := checkTxRes.GetCheckTx()
	if checkTxR.Code != abci.CodeTypeOK {
		// CheckTx failed!
		return &ctypes.ResultBroadcastTxCommit{
			CheckTx:   *checkTxR,
			DeliverTx: abci.ResponseDeliverTx{},
			Hash:      tx.Hash(),
		}, nil
	}

	// Wait for the tx to be included in a block,
	// timeout after something reasonable.
	// TODO: configurable?
	timer := time.NewTimer(60 * 2 * time.Second)
	select {
	case deliverTxResMsg := <-deliverTxResCh:
		deliverTxRes := deliverTxResMsg.(types.TMEventData).Unwrap().(types.EventDataTx)
		// The tx was included in a block.
		deliverTxR := deliverTxRes.Result
		logger.Info("DeliverTx passed ", "tx", data.Bytes(tx), "response", deliverTxR)
		return &ctypes.ResultBroadcastTxCommit{
			CheckTx:   *checkTxR,
			DeliverTx: deliverTxR,
			Hash:      tx.Hash(),
			Height:    deliverTxRes.Height,
		}, nil
	case <-timer.C:
		logger.Error("failed to include tx")
		return &ctypes.ResultBroadcastTxCommit{
			CheckTx:   *checkTxR,
			DeliverTx: abci.ResponseDeliverTx{},
			Hash:      tx.Hash(),
		}, fmt.Errorf("Timed out waiting for transaction to be included in a block")
	}
}

// Get unconfirmed transactions including their number.
//
// ```shell
// curl 'localhost:46657/unconfirmed_txs'
// ```
//
// ```go
// client := client.NewHTTP("tcp://0.0.0.0:46657", "/websocket")
// result, err := client.UnconfirmedTxs()
// ```
//
// > The above command returns JSON structured like this:
//
// ```json
// {
//   "error": "",
//   "result": {
//     "txs": [],
//     "n_txs": 0
//   },
//   "id": "",
//   "jsonrpc": "2.0"
// }
// ```
func UnconfirmedTxs() (*ctypes.ResultUnconfirmedTxs, error) {
	txs := mempool.Reap(-1)
	return &ctypes.ResultUnconfirmedTxs{len(txs), txs}, nil
}

// Get number of unconfirmed transactions.
//
// ```shell
// curl 'localhost:46657/num_unconfirmed_txs'
// ```
//
// ```go
// client := client.NewHTTP("tcp://0.0.0.0:46657", "/websocket")
// result, err := client.UnconfirmedTxs()
// ```
//
// > The above command returns JSON structured like this:
//
// ```json
// {
//   "error": "",
//   "result": {
//     "txs": null,
//     "n_txs": 0
//   },
//   "id": "",
//   "jsonrpc": "2.0"
// }
// ```
func NumUnconfirmedTxs() (*ctypes.ResultUnconfirmedTxs, error) {
	return &ctypes.ResultUnconfirmedTxs{N: mempool.Size()}, nil
}
