package mempool

import (
	"bytes"
	"container/list"
	"os"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"

	"github.com/pkg/errors"

	abci "github.com/tendermint/abci/types"
	auto "github.com/tendermint/tmlibs/autofile"
	"github.com/tendermint/tmlibs/clist"
	cmn "github.com/tendermint/tmlibs/common"
	"github.com/tendermint/tmlibs/log"

	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"

	emtConfig "github.com/dora/ultron/node/config"
)

/*

The mempool pushes new txs onto the proxyAppConn.
It gets a stream of (req, res) tuples from the proxy.
The mempool stores good txs in a concurrent linked-list.

Multiple concurrent go-routines can traverse this linked-list
safely by calling .NextWait() on each element.

So we have several go-routines:
1. Consensus calling Update() and Reap() synchronously
2. Many mempool reactor's peer routines calling CheckTx()
3. Many mempool reactor's peer routines traversing the txs linked list
4. Another goroutine calling GarbageCollectTxs() periodically

To manage these goroutines, there are three methods of locking.
1. Mutations to the linked-list is protected by an internal mtx (CList is goroutine-safe)
2. Mutations to the linked-list elements are atomic
3. CheckTx() calls can be paused upon Update() and Reap(), protected by .proxyMtx

Garbage collection of old elements from mempool.txs is handlde via
the DetachPrev() call, which makes old elements not reachable by
peer broadcastTxRoutine() automatically garbage collected.

TODO: Better handle abci client errors. (make it automatically handle connection errors)

*/

const cacheSize = 100000
var removeCacheTx = false
var usePtxHash = true
var disablePtx = false
var compactBlock = true
var buildFullBlock = true

var replay_txs bool = false
var replay_same bool = false
var replay_loop bool = false
var replay_amount int = 0
var replay_txid_base int = 0
var replay_txid int = 0
var updated bool = false
var replayTx types.Tx
var replayHashTx map[types.TxHash]types.Tx
var repeatTxEpoch = (int)(10000)

var MempoolWaitSignal bool = false
var MempoolSignalChannel = make(chan interface{})

type TxsMap map[string]struct{}

// Mempool is an ordered in-memory pool for transactions before they are proposed in a consensus
// round. Transaction validity is checked using the CheckTx abci message before the transaction is
// added to the pool. The Mempool uses a concurrent list structure for storing transactions that
// can be efficiently accessed by multiple concurrent readers.
type Mempool struct {
	config *cfg.MempoolConfig

	proxyMtx             sync.Mutex
	proxyAppConn         proxy.AppConnMempool
	txs                  *clist.CList    // concurrent linked-list of good txs
	txsHash              *clist.CList    // concurrent linked-list of good txs hash
	txsHashMap           map[types.TxHash]types.Tx	// tx and hash(app hash) map
	ptxs                 *clist.CList    // parallel txs
	ptxsHash             *clist.CList    // parallel txs with tx hash
	txType               int32           // mempool tx type
	counter              int64           // simple incrementing counter
	height               int64           // the last block Update()'d to
	rechecking           int32           // for re-checking filtered txs on Update()
	recheckCursor        *clist.CElement // next expected response
	recheckEnd           *clist.CElement // re-checking stops here
	notifiedTxsAvailable bool            // true if fired on txsAvailable for this height
	txsAvailable         chan int64      // fires the next height once for each height, when the mempool is not empty
	TxsAllRequested      chan int64     // all tx requested arrived, used for response of GetTx
	fetchingTx           [][]byte		 // fetching tx from remote peer
	requestingTx		 chan [][]byte   // requesting tx from remote peer
	// Keep a cache of already-seen txs.
	// This reduces the pressure on the proxyApp.
	cache *txCache

	// A log of mempool txs
	wal *auto.AutoFile

	logger log.Logger
	txsCh chan *txsSet

	// Keep track of uncommitted transactions
	uncommittedTxs       map[int64]([]*mempoolTx)
	uncommittedTxsHash   map[int64]([]*mempoolTx)
	uncommittedPtxs      map[int64](types.Txs)
	uncommittedPtxsHash  map[int64](types.Txs)

	// Keep track of all transactions in pending blocks
	pendingBlockTxs      map[int64]TxsMap
}


type txsSet struct {
	txs types.Txs
}

// NewMempool returns a new Mempool with the given configuration and connection to an application.
// TODO: Extract logger into arguments.
func NewMempool(config *cfg.MempoolConfig, proxyAppConn proxy.AppConnMempool, height int64) *Mempool {
	mempool := &Mempool{
		config:        config,
		proxyAppConn:  proxyAppConn,
		txs:           clist.New(),
		txsHash:       clist.New(),
		txsHashMap:    make(map[types.TxHash]types.Tx),
		ptxs:          clist.New(),
		ptxsHash:      clist.New(),
		txType:        types.RawTx,
		counter:       0,
		height:        height,
		rechecking:    0,
		recheckCursor: nil,
		recheckEnd:    nil,
		logger:        log.NewNopLogger(),
		cache:         newTxCache(cacheSize),
		uncommittedTxs:      make(map[int64]([]*mempoolTx)),
		uncommittedTxsHash:  make(map[int64]([]*mempoolTx)),
		uncommittedPtxs:     make(map[int64](types.Txs)),
		uncommittedPtxsHash: make(map[int64](types.Txs)),
		pendingBlockTxs:     make(map[int64]TxsMap),
	}
	mempool.initWAL()
	mempool.fetchingTx = make([][]byte, 0)
	proxyAppConn.SetResponseCallback(mempool.resCb)
	mempool.TxsAllRequested = make(chan int64, 1)
	mempool.requestingTx = make(chan [][]byte, 1)

	testConfig, _ := emtConfig.ParseConfig()
	if testConfig != nil {
		if testConfig.TestConfig.RepeatTxTest {
			removeCacheTx = true
		}
		if !testConfig.TestConfig.UsePtxHash {
			usePtxHash = false
		}
		if testConfig.TestConfig.DisablePtx {
			disablePtx = true
		}
		if !testConfig.TestConfig.CompactBlock {
			compactBlock = false
		}
		if !testConfig.TestConfig.BuildFullBlock {
			buildFullBlock = false
		}
		if testConfig.TestConfig.ReplayTxInMempool > 0 {
			replay_txs = true
			if testConfig.TestConfig.ReplayTxInMempool == 1 {
				replay_same = true
			}
			repeatTxEpoch = testConfig.TestConfig.ReplayNumEpoch
		}
		if replay_txs {
			mempool.initSQL()
		}
		if replay_same {
			replayHashTx = make(map[types.TxHash]types.Tx)
			replayTx = mempool.readOneTx()
		}
	}
	if !disablePtx {
		types.RcPtxInBlock()
	}
	return mempool
}

// EnableTxsAvailable initializes the TxsAvailable channel,
// ensuring it will trigger once every height when transactions are available.
// NOTE: not thread safe - should only be called once, on startup
func (mem *Mempool) EnableTxsAvailable() {
	mem.txsAvailable = make(chan int64, 1)
}

// SetLogger sets the Logger.
func (mem *Mempool) SetLogger(l log.Logger) {
	mem.logger = l
}

// CloseWAL closes and discards the underlying WAL file.
// Any further writes will not be relayed to disk.
func (mem *Mempool) CloseWAL() bool {
	if mem == nil {
		return false
	}

	mem.proxyMtx.Lock()
	defer mem.proxyMtx.Unlock()

	if mem.wal == nil {
		return false
	}
	if err := mem.wal.Close(); err != nil && mem.logger != nil {
		mem.logger.Error("Mempool.CloseWAL", "err", err)
	}
	mem.wal = nil
	return true
}

func (mem *Mempool) initWAL() {
	walDir := mem.config.WalDir()
	if walDir != "" {
		err := cmn.EnsureDir(walDir, 0700)
		if err != nil {
			cmn.PanicSanity(errors.Wrap(err, "Error ensuring Mempool wal dir"))
		}
		af, err := auto.OpenAutoFile(walDir + "/wal")
		if err != nil {
			cmn.PanicSanity(errors.Wrap(err, "Error opening Mempool wal file"))
		}
		mem.wal = af
	}
}

func (mem *Mempool) initSQL() {
	db, err := sql.Open("sqlite3", "./record_account_txs.db")
	if err != nil {
		fmt.Printf("********open sqlite3 error\n")
	}
	defer db.Close()

	sql := `CREATE TABLE IF NOT EXISTS txs (txId integer, tx blob);`
	db.Exec(sql)
}

func (mem *Mempool) readOneTx() types.Tx {
	var data = make([]byte, 1000)
	f, err := os.Open("./replayOneData")
	if err != nil {
	}
	size, err := f.Read(data)
	if err != nil {
	}
	txHash := data[0:32]
	tx := data[32:size]
	fmt.Println("---------------------")
	fmt.Println("total size", size, "hash", txHash, "tx", tx)
	fmt.Println("---------------------")
	replayHashTx[types.BytesToHash(txHash)] = tx[:]
	if compactBlock {
		return txHash
	}
	return tx
}

// Lock locks the mempool. The consensus must be able to hold lock to safely update.
func (mem *Mempool) Lock() {
	mem.proxyMtx.Lock()
}

// Unlock unlocks the mempool.
func (mem *Mempool) Unlock() {
	mem.proxyMtx.Unlock()
}

// Size returns the number of transactions in the mempool.
func (mem *Mempool) Size() int {
	return mem.txs.Len()
}

// Flush removes all transactions from the mempool and cache
func (mem *Mempool) Flush() {
	mem.proxyMtx.Lock()
	defer mem.proxyMtx.Unlock()

	mem.cache.Reset()

	for e := mem.txs.Front(); e != nil; e = e.Next() {
		mem.txs.Remove(e)
		e.DetachPrev()
	}
}

// TxsFrontWait returns the first transaction in the ordered list for peer goroutines to call .NextWait() on.
// It blocks until the mempool is not empty (ie. until the internal `mem.txs` has at least one element)
func (mem *Mempool) TxsFrontWait() *clist.CElement {
	return mem.txs.FrontWait()
}

// TxsFetching is mempool/reactor to get tx would to fetch
// TODO: need assign the requsting tx to the send block peer but not random one
func (mem *Mempool) TxsFetching() <-chan [][]byte {
	return mem.requestingTx
}

// NotifiyFetchingTx is set fetchingTx to chan
// TODO: avoid chan block when there is no peer read the chan
func (mem *Mempool) NotifiyFetchingTx(fetchingTxs [][]byte) {
	if (len(mem.fetchingTx) == 0) {
		mem.requestingTx <- nil		//clear fetching in mempool/reactor
	} else {
		mem.requestingTx <- fetchingTxs
	}
}

// GetTx returns the tx from 'from' type to 'to' type
func (mem *Mempool) GetTx(hash []byte, from int32, to int32) (bool, types.Tx) {
	mem.proxyMtx.Lock()
	defer mem.proxyMtx.Unlock()
	mem.logger.Debug("Get tx", "from", from, "to", to)

	missTx := false
	missedTxs := make([][]byte, 0, 1)
	if from == types.ParallelTxHash && to == types.RawTxHash {
		ptx, _, _ := types.DecodePtx(hash)
		if (ptx == nil) {
			cmn.PanicSanity(cmn.Fmt("decode ptx fail"))
		}
		for _, txHash := range ptx.Data.TxIds {
			txHashTmp := make([]byte, 32)
			copy(txHashTmp, txHash[:])
			if (mem.txsHashMap[txHash] == nil) {
				mem.fetchingTx = append(mem.fetchingTx, txHashTmp)
				missedTxs = append(missedTxs, txHashTmp)
				missTx = true
			}
		}
		if (missTx) {
			mem.logger.Info("Missing tx")
			mem.NotifiyFetchingTx(missedTxs)
			return true, nil
		}
	} else if from == types.RawTxHash && to == types.RawTx {
		if replay_txs {
			if replay_same {
				return false, replayHashTx[types.BytesToHash(hash)]
			}
		}
		timeStamp := time.Now()
		mem.wal.Write(
			[]byte(
				fmt.Sprintf("[%d:%d:%d:%d] Get Tx{0x%X}\n",
				timeStamp.Hour(), timeStamp.Minute(), timeStamp.Second(), timeStamp.Nanosecond(), hash)))
		mem.wal.Sync()

		tx := mem.txsHashMap[types.BytesToHash(hash)]
		if (tx != nil) {
			return false, tx
		} else {
			txHashTmp := make([]byte, 32)
			copy(txHashTmp, hash[:])
			mem.fetchingTx = append(mem.fetchingTx, txHashTmp)
			missedTxs = append(missedTxs, txHashTmp)
			mem.NotifiyFetchingTx(missedTxs)
			return false, nil
		}
	} else if from == types.RawTxHash && to == types.RawTxHash {
		if replay_txs {
			if replay_same {
				return false, nil
			}
		}
		if mem.txsHashMap[types.BytesToHash(hash)] == nil {
			txHashTmp := make([]byte, 32)
			copy(txHashTmp, hash[:])
			mem.fetchingTx = append(mem.fetchingTx, txHashTmp)
			missedTxs = append(missedTxs, txHashTmp)
			mem.NotifiyFetchingTx(missedTxs)
			return true, nil
		}
	} else if from == types.ParallelTxHash && to == types.ParallelTx {
		ptx, _, len := types.DecodePtx(hash)
		if (ptx == nil) {
			cmn.PanicSanity(cmn.Fmt("decode ptx fail"))
		}
		mem.logger.Debug("ptx has tx len", len)
		ptxRawTx := make([][]byte, 0, 1)//mem.txs.Len())
		for _, txHash := range ptx.Data.TxIds {
			if (mem.txsHashMap[txHash] == nil) {
				cmn.PanicSanity(cmn.Fmt("GetTx from ptxhash to ptx still miss some hash", txHash))
				continue
			}
			ptxRawTx = append(ptxRawTx, mem.txsHashMap[txHash])
		}
		txsInPts, err := types.EncodePtx(hash, ptxRawTx)
		if (err != nil) {
			cmn.PanicSanity(cmn.Fmt("GetTx EncodePtx fail"))
		}
		return false, txsInPts
	}
	return false, nil
}

// CheckTx executes a new transaction against the application to determine its validity
// and whether it should be added to the mempool.
// It blocks if we're waiting on Update() or Reap().
// cb: A callback from the CheckTx command.
//     It gets called from another goroutine.
// CONTRACT: Either cb will get called, or err returned.
func (mem *Mempool) CheckTx(tx types.Tx, hash types.CommonHash, txType int32, local bool, cb func(*abci.Response)) (err error) {
	// Note: If it is parallel tx, just PushBack to ptxs
	if txType == types.ParallelTx {
		memTx := &mempoolTx{
			counter: mem.counter,
			height:  mem.height,
			tx:      tx,
		}
		mem.ptxs.PushBack(memTx)
		mem.notifyTxsAvailable()
		types.RcPtxInBlock()
		return nil
	} else if (txType == types.ParallelTxHash) {
		// check tx in ptx exist in mempool
		ptx, _, _ := types.DecodePtx(tx)
		if (ptx == nil) {
			cmn.PanicSanity(cmn.Fmt("CheckTx() decodePtx is nil"))
		}
		for _, txHash := range ptx.Data.TxIds {
			txHashMaptx := mem.txsHashMap[txHash]
			if (txHashMaptx == nil) {
				cmn.PanicSanity(cmn.Fmt("Broadcast ptx but tx not exist in mempook", txHash))
			}
		}
		memTx := &mempoolTx{
			counter: mem.counter,
			height:  mem.height,
			tx:      tx,
		}
		mem.ptxsHash.PushBack(memTx)
		mem.notifyTxsAvailable()
		types.RcPtxInBlock()
		return nil
	}
	// // Use the hash send from remote peer
	// mem.handleTxArrive(hash)
	// CACHE
	if mem.cache.Exists(tx) {
		return fmt.Errorf("Tx already exists in cache")
	}
	mem.cache.Push(tx)
	// END CACHE

	// WAL
	// if mem.wal != nil {
	// 	// TODO: Notify administrators when WAL fails
	// 	_, err := mem.wal.Write([]byte(tx))
	// 	if err != nil {
	// 		mem.logger.Error("Error writing to WAL", "err", err)
	// 	}
	// 	_, err = mem.wal.Write([]byte("\n"))
	// 	if err != nil {
	// 		mem.logger.Error("Error writing to WAL", "err", err)
	// 	}
	// }
	// END WAL

	// NOTE: proxyAppConn may error if tx buffer is full
	if err = mem.proxyAppConn.Error(); err != nil {
		return err
	}
	reqRes := mem.proxyAppConn.CheckTxAsync(abci.RequestCheckTx{Tx: tx,Local: local})
	if cb != nil {
		reqRes.SetCallback(cb)
	}

	if reqRes.Response.Value.(*abci.Response_CheckTx).CheckTx.Code != abci.CodeTypeOK {
		return fmt.Errorf("Fail to Add Tx %s", reqRes.Response.Value.(*abci.Response_CheckTx).CheckTx.Log)
	}
	return nil
}

// ABCI callback function
func (mem *Mempool) resCb(req *abci.Request, res *abci.Response) {
	mem.proxyMtx.Lock()
	defer mem.proxyMtx.Unlock()
	if mem.recheckCursor == nil {
		mem.resCbNormal(req, res)
	} else {
		mem.resCbRecheck(req, res)
	}
}

func (mem *Mempool) handleTxArrive(height int64, txHash []byte) {
	if (len(mem.fetchingTx) == 0 || txHash == nil) {
		return
	}
	for index, hash := range mem.fetchingTx {
		if (bytes.Equal(hash, txHash)) {
			mem.fetchingTx = append(mem.fetchingTx[:index], mem.fetchingTx[index+1:]...)
			break
		}
	}
	if (len(mem.fetchingTx) == 0) {
		// notify, the txHash not useful now
		mem.logger.Info("txs all arrived")
		mem.TxsAllRequested <- height
	}
}

func (mem *Mempool) resCbNormal(req *abci.Request, res *abci.Response) {
	switch r := res.Value.(type) {
	case *abci.Response_CheckTx:
		tx := req.GetCheckTx().Tx
		if r.CheckTx.Code == abci.CodeTypeOK {
			mem.counter++
			memTx := &mempoolTx{
				counter: mem.counter,
				height:  mem.height,
				tx:      tx,
			}
			if (removeCacheTx) {
				mem.cache.Remove(tx)
			}

			txHeight := (int64)(-1)
			if height := mem.isInPendingBlock(tx); height > 0 {
				mem.uncommittedTxs[height] = append(mem.uncommittedTxs[height], memTx)
				txHeight = height
			} else {
				// f, _ := os.Create("/tmp/replaydata")
				// defer f.Close()
				// fmt.Println("--------------------")
				// fmt.Println("hash len", len(r.CheckTx.Data), "data len", len(tx))
				// fmt.Println("--------------------")
				// f.Write(r.CheckTx.Data)
				// f.Write(tx)
				// f.Sync()
				mem.txs.PushBack(memTx)
				if (disablePtx || usePtxHash) {
					hashValue := types.BytesToHash(r.CheckTx.Data)
					timeStamp := time.Now()
					mem.wal.Write(
						[]byte(
							fmt.Sprintf("[%d:%d:%d:%d] Add Tx{0x%X}\n",
							timeStamp.Hour(), timeStamp.Minute(), timeStamp.Second(), timeStamp.Nanosecond(), hashValue)))
					mem.txsHashMap[hashValue] = tx
				}
				if disablePtx && compactBlock {
					memTxHash := &mempoolTx{
						counter: mem.counter,
						height:  mem.height,
						tx:      types.Tx(r.CheckTx.Data),
					}
					mem.txsHash.PushBack(memTxHash)
				}
				if disablePtx {
					mem.notifyTxsAvailable()
				}
				mem.logger.Debug("Added good transaction", "tx", tx, "res", r)
			}
			mem.handleTxArrive(txHeight, r.CheckTx.Data)
		} else {
			// ignore bad transaction
			mem.logger.Info("Rejected bad transaction", "tx", tx, "res", r)

			// remove from cache (it might be good later)
			mem.cache.Remove(tx)

			// TODO: handle other retcodes
		}
	case *abci.Response_GetTx:
		if r.GetTx.Code == abci.CodeTypeOK {
			mem.logger.Info("Response_GetTx get missed tx hash is", r.GetTx.Response)
			// TODO: handle missedHash
			if (r.GetTx.Response != nil) {
				missedTxs := make([][]byte, 0)
				mem.fetchingTx = append(mem.fetchingTx, r.GetTx.Response)
				missedTxs = append(missedTxs, r.GetTx.Response)
				// send GetTx message to remote peer
				// TODO: notify together, current is each pts to notify
				mem.NotifiyFetchingTx(missedTxs)
			}
		} else {
			mem.logger.Info("Response_GetTx return error", r.GetTx.Code)
		}
	default:
		// ignore other messages
	}
}

func (mem *Mempool) resCbRecheck(req *abci.Request, res *abci.Response) {
	switch r := res.Value.(type) {
	case *abci.Response_CheckTx:
		memTx := mem.recheckCursor.Value.(*mempoolTx)
		if !bytes.Equal(req.GetCheckTx().Tx, memTx.tx) {
			cmn.PanicSanity(cmn.Fmt("Unexpected tx response from proxy during recheck\n"+
				"Expected %X, got %X", r.CheckTx.Data, memTx.tx))
		}
		if r.CheckTx.Code == abci.CodeTypeOK {
			// Good, nothing to do.
		} else {
			// Tx became invalidated due to newly committed block.
			mem.txs.Remove(mem.recheckCursor)
			mem.recheckCursor.DetachPrev()

			// remove from cache (it might be good later)
			mem.cache.Remove(req.GetCheckTx().Tx)
		}
		if mem.recheckCursor == mem.recheckEnd {
			mem.recheckCursor = nil
		} else {
			mem.recheckCursor = mem.recheckCursor.Next()
		}
		if mem.recheckCursor == nil {
			// Done!
			atomic.StoreInt32(&mem.rechecking, 0)
			mem.logger.Info("Done rechecking txs")

			// incase the recheck removed all txs
			if mem.Size() > 0 {
				//mem.notifyTxsAvailable()
			}
		}
	default:
		// ignore other messages
	}
}

func (mem *Mempool) isInPendingBlock(tx types.Tx) int64 {
	for height, txs := range mem.pendingBlockTxs {
		if _, ok := txs[string(tx)]; ok {
			return height
		}
	}

	return 0
}

// TxsAvailable returns a channel which fires once for every height,
// and only when transactions are available in the mempool.
// NOTE: the returned channel may be nil if EnableTxsAvailable was not called.
func (mem *Mempool) TxsAvailable() <-chan int64 {
	return mem.txsAvailable
}

func (mem *Mempool) notifyTxsAvailable() {
	// if mem.Size() == 0 {
	// 	panic("notified txs available but mempool is empty!")
	// }
	if mem.txsAvailable != nil && !mem.notifiedTxsAvailable {
		mem.notifiedTxsAvailable = true
		mem.txsAvailable <- mem.height + 1
	}
}

// TxResponsed is GetTx response
func (mem *Mempool) TxResponsed() <-chan int64 {
	return mem.TxsAllRequested
}

// Reap returns a list of transactions currently in the mempool.
// If maxTxs is -1, there is no cap on the number of returned transactions.
func (mem *Mempool) Reap(maxTxs int) types.Txs {
	if (MempoolWaitSignal) {
		<- MempoolSignalChannel
		MempoolWaitSignal = false
	}

	mem.proxyMtx.Lock()
	defer mem.proxyMtx.Unlock()

	for atomic.LoadInt32(&mem.rechecking) > 0 {
		// TODO: Something better?
		time.Sleep(time.Millisecond * 10)
	}

	txs := mem.collectTxs(maxTxs)
	return txs
}

// maxTxs: -1 means uncapped, 0 means none
// Note: get ptx(with raw tx hash) but not raw tx
func (mem *Mempool) collectTxs(maxTxs int) types.Txs {
	if maxTxs == 0 {
		return []types.Tx{}
	} else if maxTxs < 0 {
		if disablePtx {
			maxTxs = mem.txs.Len()
		} else {
			maxTxs = mem.ptxsHash.Len()
		}
	}
	txLen := 0
	if disablePtx {
		if compactBlock {
			txLen = mem.txsHash.Len()
		} else {
			txLen = mem.txs.Len()
		}
	} else if usePtxHash {
		txLen = mem.ptxsHash.Len()
	} else {
		txLen = mem.ptxs.Len()
	}
	if !replay_txs && txLen == 0 {
		return []types.Tx{}
	}

	txLen = cmn.MinInt(txLen, maxTxs)
	txs := make([]types.Tx, 0, txLen)

	if disablePtx {
		if compactBlock {
			if replay_same {
				for i := 0; i < repeatTxEpoch; i ++ {
					txs = append(txs, replayTx)
				}
			} else {
				for e := mem.txsHash.Front(); e != nil && len(txs) < txLen; e = e.Next() {
					memTx := e.Value.(*mempoolTx)
					txs = append(txs, memTx.tx)
				}
			}
		} else {
			if replay_same {
				for i := 0; i < repeatTxEpoch; i ++ {
					txs = append(txs, replayTx)
				}
			} else {
				for e := mem.txs.Front(); e != nil && len(txs) < txLen; e = e.Next() {
					memTx := e.Value.(*mempoolTx)
					txs = append(txs, memTx.tx)
				}
			}
		}
	} else if usePtxHash {
		for e := mem.ptxsHash.Front(); e != nil && len(txs) < txLen; e = e.Next() {
			memTx := e.Value.(*mempoolTx)
			txs = append(txs, memTx.tx)
		}
	} else {
		for e := mem.ptxs.Front(); e != nil && len(txs) < txLen; e = e.Next() {
			memTx := e.Value.(*mempoolTx)
			txs = append(txs, memTx.tx)
		}
	}
	return txs
}

// Update informs the mempool that the given txs were committed and can be discarded.
// NOTE: this should be called *after* block is committed by consensus.
// NOTE: unsafe; Lock/Unlock must be managed by caller
// TODO: remove the tx in tx map, current can not remove as txs are ptx hash
// TODO: handle tx,ptx,ptxhash...cases
func (mem *Mempool) Update(height int64, txs types.Txs) error {
    updated = true
    replay_txid += replay_amount
	if err := mem.proxyAppConn.FlushSync(); err != nil { // To flush async resCb calls e.g. from CheckTx
		return err
	}

	// Check whether txs have been moved into uncommited txs map
	if disablePtx {
		if compactBlock {
			if _, ok := mem.uncommittedTxsHash[height]; ok {
				for _, memTx := range mem.uncommittedTxsHash[height] {
					hashValue := types.BytesToHash(memTx.tx)
					if  mem.txsHashMap[hashValue] != nil {
						timeStamp := time.Now()
						mem.wal.Write(
							[]byte(
								fmt.Sprintf("[%d:%d:%d:%d] Del Tx{0x%X}\n",
								timeStamp.Hour(), timeStamp.Minute(), timeStamp.Second(), timeStamp.Nanosecond(), hashValue)))
						delete(mem.txsHashMap, hashValue)
					}
				}
				mem.wal.Sync()
				delete(mem.uncommittedTxsHash, height)
				delete(mem.uncommittedTxs, height)
				delete(mem.pendingBlockTxs, height)
				return nil
			}
		} else {
			if _, ok := mem.uncommittedTxs[height]; ok {
				delete(mem.uncommittedTxs, height)
				delete(mem.pendingBlockTxs, height)
				return nil
			}
		}
	} else if usePtxHash {
		if _, ok := mem.uncommittedPtxs[height]; ok {
			for _, tx := range mem.uncommittedPtxs[height] {
				ptx, _, _ := types.DecodePtx(tx)
				if (ptx == nil) {
					cmn.PanicSanity(cmn.Fmt("Update() decodePtx is nil"))
				}
				for _, txHash := range ptx.Data.TxIds {
					if mem.txsHashMap[txHash] != nil {
						delete(mem.txsHashMap, txHash)
					}
				}
			}
			delete(mem.uncommittedPtxs, height)
			delete(mem.pendingBlockTxs, height)
			return nil
		}
	} else {
		if _, ok := mem.uncommittedPtxsHash[height]; ok {
			delete(mem.uncommittedPtxsHash, height)
			delete(mem.pendingBlockTxs, height)
			return nil
		}
	}

	txsMap := make(map[string]struct{})
	if disablePtx {
		if compactBlock {
			removedTxsHash := make([]*mempoolTx, 0, mem.txsHash.Len())
			e := mem.txsHash.Front()
			for _, txHash := range txs {
				if (len(txHash) != 32) {
					mem.logger.Info(fmt.Sprintf("WARN: block tx %s is not a hash string", txHash))
				}
				
				txHashValue := types.BytesToHash(txHash)
				txHashMaptx := mem.txsHashMap[txHashValue]
				if (txHashMaptx != nil) {
					txsMap[string(txHashMaptx)] = struct{}{}
				} else {
					if !replay_txs {
						timeStamp := time.Now()
						errMsg := fmt.Sprintf("[%d:%d:%d:%d] Update mempool can not find Tx{%X}",
											timeStamp.Hour(), timeStamp.Minute(), timeStamp.Second(), timeStamp.Nanosecond(), txHash)
						mem.wal.Write([]byte(errMsg))
						mem.logger.Error(errMsg)
					}
				}
				// remove from txHash
				for ; e != nil; e = e.Next() {
					memTx := e.Value.(*mempoolTx)
					if bytes.Compare(txHash, memTx.tx) == 0 {
						mem.txsHash.Remove(e)
						e.DetachPrev()
						e = e.Next() // move to next pos for next txHash loop
						removedTxsHash = append(removedTxsHash, memTx)
						break
					}
				}
			}
			if len(removedTxsHash) > 0 {
				mem.uncommittedTxsHash[height] = removedTxsHash
			}
		} else {
			for _, tx := range txs {
				txsMap[string(tx)] = struct{}{}
			}
		}
	} else if usePtxHash {
		for _, tx := range txs {
			ptx, _, _ := types.DecodePtx(tx)
			if (ptx == nil) {
				cmn.PanicSanity(cmn.Fmt("Update() decodePtx is nil"))
			}
			for _, txHash := range ptx.Data.TxIds {
				txHashMaptx := mem.txsHashMap[txHash]
				if (txHashMaptx != nil) {
					txsMap[string(txHashMaptx)] = struct{}{}
				} else {
					errMsg := fmt.Sprintf("Update mempool can not find Tx{%X}", txHash)
					mem.wal.Write([]byte(errMsg))
					mem.logger.Error(errMsg)
				}
			}
		}
		mem.uncommittedPtxsHash[height] = txs
	} else {
		// First, create a lookup map of txns in new txs.
		for _, tx := range txs {
			ptx, _, _ := types.DecodePtx(tx)
			if (ptx == nil) {
				cmn.PanicSanity(cmn.Fmt("Update() decodePtx is nil"))
			}
			for _, txRaw := range ptx.Data.Txs {
				txsMap[string(txRaw)] = struct{}{}
			}
		}
		mem.uncommittedPtxs[height] = txs
	}

	if !replay_txs {
		mem.pendingBlockTxs[height] = txsMap
	}

	// Set height
	mem.height = height
	mem.notifiedTxsAvailable = false

	// Remove transactions that are already in txs.
	goodTxs := mem.filterTxs(height, txsMap)
	// Recheck mempool txs if any txs were committed in the block
	// NOTE/XXX: in some apps a tx could be invalidated due to EndBlock,
	//	so we really still do need to recheck, but this is for debugging
	if false && mem.config.Recheck && (mem.config.RecheckEmpty || len(txs) > 0) {
		mem.logger.Info("Recheck txs", "numtxs", len(goodTxs), "height", height)
		mem.recheckTxs(goodTxs)
		// At this point, mem.txs are being rechecked.
		// mem.recheckCursor re-scans mem.txs and possibly removes some txs.
		// Before mem.Reap(), we should wait for mem.recheckCursor to be nil.
	}

	// Clear PTX pool
	// TODO: reset directly but not use for
	for e := mem.ptxs.Front(); e != nil; e = e.Next() {
		mem.ptxs.Remove(e)
	}
	for e := mem.ptxsHash.Front(); e != nil; e = e.Next() {
		mem.ptxsHash.Remove(e)
	}
	mem.fetchingTx = mem.fetchingTx[:0]

	mem.logger.Info("After Update() txs size", mem.txs.Len())
	mem.logger.Info("After Update() txsHash size", mem.txsHash.Len())
	mem.logger.Info("After Update() ptxs size", mem.ptxs.Len())
	mem.logger.Info("After Update() ptxsHash size", mem.ptxsHash.Len())
	mem.logger.Info("After Update() txsHashMap size", len(mem.txsHashMap))
	mem.logger.Info("After Update() pendingBlockTx size", len(mem.pendingBlockTxs))
	mem.logger.Info("After Update() uncommitted txs size", len(mem.uncommittedTxs))
	mem.logger.Info("After Update() uncommitted txsHash size", len(mem.uncommittedTxsHash))
	mem.logger.Info("After Update() uncommitted ptxs size", len(mem.uncommittedPtxs))
	mem.logger.Info("After Update() uncommitted ptxsHash size", len(mem.uncommittedPtxsHash))

	mem.wal.Sync()
	//mem.NotifiyFetchingTx()
	return nil
}

// Note: remove the tx in txs from blockTxsMap
func (mem *Mempool) filterTxs(height int64, blockTxsMap map[string]struct{}) []types.Tx {
	goodTxs := make([]types.Tx, 0, mem.txs.Len())
	removedTxs := make([]*mempoolTx, 0, mem.txs.Len())
	mem.logger.Info("Before update mempool, tx size", mem.txs.Len())
	for e := mem.txs.Front(); e != nil; e = e.Next() {
		memTx := e.Value.(*mempoolTx)
		// Remove the tx if it's alredy in a block.
		if _, ok := blockTxsMap[string(memTx.tx)]; ok {
			// save removed txs
			removedTxs = append(removedTxs, memTx)
			// remove from clist
			mem.txs.Remove(e)
			e.DetachPrev()

			// NOTE: we don't remove committed txs from the cache.
			continue
		}
		// Good tx!
		goodTxs = append(goodTxs, memTx.tx)
	}

	// save into uncommitted txs map
	if len(removedTxs) > 0 {
		mem.uncommittedTxs[height] = removedTxs
	}
	return goodTxs
}

// NOTE: pass in goodTxs because mem.txs can mutate concurrently.
func (mem *Mempool) recheckTxs(goodTxs []types.Tx) {
	if len(goodTxs) == 0 {
		return
	}
	atomic.StoreInt32(&mem.rechecking, 1)
	mem.recheckCursor = mem.txs.Front()
	mem.recheckEnd = mem.txs.Back()

	// Push txs to proxyAppConn
	// NOTE: resCb() may be called concurrently.
	for _, tx := range goodTxs {
		mem.proxyAppConn.CheckTxAsync(abci.RequestCheckTx{Tx: tx, Local: false})
	}
	mem.proxyAppConn.FlushAsync()
}

// Restore txs back to mempool when rolling back pipeline
// NOTE: unsafe; Lock/Unlock must be managed by caller
func (mem *Mempool) Restore(height int64) {
	if disablePtx {
		if txs, ok := mem.uncommittedTxs[height]; ok {
			size := len(txs)
			for i := 0; i < size; i++ {
				mem.txs.PushBack(txs[i])
			}
		}
	} else if usePtxHash {
		if txs, ok := mem.uncommittedPtxsHash[height]; ok {
			size := len(txs)
			for i := 0; i < size; i++ {
				mem.ptxsHash.PushBack(txs[i])
			}
		}
	} else {
		if txs, ok := mem.uncommittedPtxs[height]; ok {
			size := len(txs)
			for i := 0; i < size; i++ {
				mem.ptxs.PushBack(txs[i])
			}
		}
	}
}

//--------------------------------------------------------------------------------

// mempoolTx is a transaction that successfully ran
type mempoolTx struct {
	counter int64    // a simple incrementing counter
	height  int64    // height that this tx had been validated in
	tx      types.Tx //
}

// Height returns the height for this transaction
func (memTx *mempoolTx) Height() int64 {
	return atomic.LoadInt64(&memTx.height)
}

//--------------------------------------------------------------------------------

// txCache maintains a cache of transactions.
type txCache struct {
	mtx  sync.Mutex
	size int
	map_ map[string]struct{}
	list *list.List // to remove oldest tx when cache gets too big
}

// newTxCache returns a new txCache.
func newTxCache(cacheSize int) *txCache {
	return &txCache{
		size: cacheSize,
		map_: make(map[string]struct{}, cacheSize),
		list: list.New(),
	}
}

// Reset resets the txCache to empty.
func (cache *txCache) Reset() {
	cache.mtx.Lock()
	cache.map_ = make(map[string]struct{}, cacheSize)
	cache.list.Init()
	cache.mtx.Unlock()
}

// Exists returns true if the given tx is cached.
func (cache *txCache) Exists(tx types.Tx) bool {
	cache.mtx.Lock()
	_, exists := cache.map_[string(tx)]
	cache.mtx.Unlock()
	return exists
}

// Push adds the given tx to the txCache. It returns false if tx is already in the cache.
func (cache *txCache) Push(tx types.Tx) bool {
	cache.mtx.Lock()
	defer cache.mtx.Unlock()

	if _, exists := cache.map_[string(tx)]; exists {
		return false
	}

	if cache.list.Len() >= cache.size {
		popped := cache.list.Front()
		poppedTx := popped.Value.(types.Tx)
		// NOTE: the tx may have already been removed from the map
		// but deleting a non-existent element is fine
		delete(cache.map_, string(poppedTx))
		cache.list.Remove(popped)
	}
	cache.map_[string(tx)] = struct{}{}
	cache.list.PushBack(tx)
	return true
}

// Remove removes the given tx from the cache.
func (cache *txCache) Remove(tx types.Tx) {
	cache.mtx.Lock()
	delete(cache.map_, string(tx))
	cache.mtx.Unlock()
}
