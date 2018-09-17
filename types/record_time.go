package types

import (
	"fmt"
	"time"
	"sync"
	wire "github.com/tendermint/go-wire"
	emtConfig "github.com/dora/ultron/node/config"
)

type RecordTime struct {
	enterProposeTime			int64
	startCreateBlockTime		int64
	endCreateBlockTime			int64
	recevieBlockTime			int64
	enterPrevoteTime			int64
	receive23prevoteTime		int64
	enterPrecommitTime			int64
	receive23Precommit			int64
	enterCommitTime				int64
	commitOverTime				int64
	startNewHeightTime			int64
	blockHeight					int64
	blockTxs					int64
	blockPtxs					int64
	blockSize					int
	blockPartCount				int
	cmpctBlockSize				int
	cmpctBlockPartCount			int
}

var (
	parellelPBFT = true
	parellelSteps = (int64)(4)
	parellelTxs = (int64)(0)
	parellelStartTime = (int64)(0)
	printConsensusLog = false
	printSendRecvLog = false
	ptxInBlock = false
	mtx sync.Mutex
	recordTime map[int64]*RecordTime
)

func RcInit() {
	testConfig, _ := emtConfig.ParseConfig()
	if testConfig != nil {
		if testConfig.TestConfig.PrintConsensusLog {
			printConsensusLog = true
		}
		if testConfig.TestConfig.PrintSendRecvLog {
			printSendRecvLog = true
		}
	}
	if printConsensusLog {
		recordTime = make(map[int64]*RecordTime)
	}
}

func RcPtxInBlock() {
	ptxInBlock = true
}

func reset(rT *RecordTime) {
	rT.enterProposeTime = (int64)(0)
	rT.startCreateBlockTime = (int64)(0)
	rT.endCreateBlockTime = (int64)(0)
	rT.recevieBlockTime = (int64)(0)
	rT.enterPrevoteTime = (int64)(0)
	rT.receive23prevoteTime = (int64)(0)
	rT.enterPrecommitTime = (int64)(0)
	rT.receive23Precommit = (int64)(0)
	rT.enterCommitTime = (int64)(0)
	rT.commitOverTime = (int64)(0)
	rT.blockTxs = (int64)(0)
	rT.blockPtxs = (int64)(0)
	rT.blockSize = (int)(0)
	rT.blockPartCount = (int)(0)
}

func getRecordTimeAtHeight(height int64) *RecordTime{
	mtx.Lock()
	defer mtx.Unlock()
	if _, ok := recordTime[height]; !ok {
		rT := &RecordTime {}
		// reset(rT)
		recordTime[height] = rT
	}
	return recordTime[height]
}

func removeRecordTime(height int64) {
	mtx.Lock()
	defer mtx.Unlock()
	if _, ok := recordTime[height]; ok {
		delete(recordTime, height)
	}
}

func RcenterPropose(height int64) {
	if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.enterProposeTime = time.Now().UnixNano()
	// fmt.Println("*****Block height", blockHeight, "propose time", enterProposeTime)
}

func RcstartCreateBlock(height int64) {
    if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.startCreateBlockTime = time.Now().UnixNano()
}

func RcendCreateBlock(height int64) {
    if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.endCreateBlockTime = time.Now().UnixNano()
}

func RcreceiveBlock(height int64) {
    if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.recevieBlockTime = time.Now().UnixNano()
}

func RcenterPrevote(height int64) {
	if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.enterPrevoteTime = time.Now().UnixNano()
	// fmt.Println("*****Block height", blockHeight, "prevote time", enterPrevoteTime)
}

func Rcreceive23Prevote(height int64) {
    if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.receive23prevoteTime = time.Now().UnixNano()
}

func RcenterPrecommit(height int64) {
	if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.enterPrecommitTime = time.Now().UnixNano()
	// fmt.Println("*****Block height", blockHeight, "precommit time", enterPrecommitTime)
}

func Rcreceive23Precommit(height int64) {
    if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.receive23Precommit = time.Now().UnixNano()
}

func RcenterCommit(height int64) {
    if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.enterCommitTime = time.Now().UnixNano()
}

func RccommitOver(height int64) {
	if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.commitOverTime = time.Now().UnixNano()
	RccalcTimeDiff(height)
}

func GetStringSize(size int) string {
	if size >= 1024*1024 {
		return fmt.Sprintf("%dB(%dmB %dkB %dB)", size, size/(1024*1024), (size%(1024*1024))/1024, size%1024)
	} else if size >= 1024 {
		return fmt.Sprintf("%dB(%dkB %dB)", size, size/1024, size%1024)
	} else {
		return fmt.Sprintf("%dB", size)
	}
}

func GetTimeString(time int64) string {
	if time > 1000000000 {
		return fmt.Sprintf("%dns(%.2fs)", time, (float64)(time)/1000000000)
	} else if time > 1000000 {
		return fmt.Sprintf("%dns(%.2fms)", time, (float64)(time)/1000000)
	} else if time > 1000 {
		return fmt.Sprintf("%dns(%.2fus)", time, (float64)(time)/1000)
	} else {
		return fmt.Sprintf("%dns", time)
	}
}

func RcCMPCTBlock(height int64, block *Block, blockParts *PartSet) {
	if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.cmpctBlockPartCount = blockParts.count
	rT.cmpctBlockSize = len(wire.BinaryBytes(block))
	// if (ptxInBlock && block.NumTxs > 0) {
	// 	rT.blockPtxs = block.NumTxs
	// 	for _, tx := range block.Txs {
	// 		ptx, _, size := DecodePtx(tx)
	// 		if (ptx == nil) {
	// 			fmt.Println("record time decodePtx is nil")
	// 			continue
	// 		}
	// 		rT.blockTxs = rT.blockTxs + (int64)(size)
	// 	}
	// } else {
	// 	rT.blockTxs = block.NumTxs
	// }
}

func RcBlock(height int64, block *Block, blockParts *PartSet) {
	if !printConsensusLog {
		return
	}
	rT := getRecordTimeAtHeight(height)
	rT.blockPartCount = blockParts.count
	rT.blockSize = len(wire.BinaryBytes(block))
	if (ptxInBlock && block.NumTxs > 0) {
		rT.blockPtxs = block.NumTxs
		for _, tx := range block.Txs {
			ptx, _, size := DecodePtx(tx)
			if (ptx == nil) {
				fmt.Println("record time decodePtx is nil")
				continue
			}
			rT.blockTxs = rT.blockTxs + (int64)(size)
		}
	} else {
		rT.blockTxs = block.NumTxs
	}
}

func getMsgTypeString(id byte) string {
	if (id == byte(0x00)) {
		return "PeerExchange"
	} else if (id == byte(0x30)) {
		return "MemPool"
	} else if (id == byte(0x38)) {
		return "Evidence"
	} else if (id == byte(0x20)) {
		return "State"
	} else if (id == byte(0x21)) {
		return "Data"
	} else if (id == byte(0x22)) {
		return "Vote"
	} else if (id == byte(0x23)) {
		return "VoteSetBits"
	} else if (id == byte(0x40)) {
		return "BlockChain"
	}
	fmt.Println("default type is ", id)
	return "Default"
}

func RcSendMsg(id byte, size int) {
	if (!printConsensusLog) {
		return
	}
	if (!printSendRecvLog) {
		return
	}
	fmt.Println("*****Block height", "Send msg type", getMsgTypeString(id), "msg size", GetStringSize(size))
}

func RcReciveMsg(id byte, size int) {
	if (!printConsensusLog) {
		return
	}
	if (!printSendRecvLog) {
		return
	}
	fmt.Println("*****Block height", "Receive msg type", getMsgTypeString(id), "msg size", GetStringSize(size))

}

func RccalcTimeDiff(height int64) {
	if (!printConsensusLog) {
		return
	}
	rT := getRecordTimeAtHeight(height)
	diff := (int64)(0)
	interval := (int64)(0)
	if !parellelPBFT && rT.startNewHeightTime > 0 && rT.commitOverTime > rT.startNewHeightTime {
		interval = rT.commitOverTime - rT.startNewHeightTime
		fmt.Println("*****Block height", height, "interval", GetTimeString(interval))
	}
	if parellelPBFT {
		if height % parellelSteps == 0 && parellelStartTime > 0 {
			interval = rT.commitOverTime - parellelStartTime
			fmt.Println("*****Block height", height, "interval for 4 is", GetTimeString(interval))
		}
	}
	if (rT.cmpctBlockSize > 0) {
		fmt.Println("*****Block height", height, "cmpct block parts", rT.cmpctBlockPartCount, "size", GetStringSize(rT.cmpctBlockSize))
	}
	fmt.Println("*****Block height", height, "block parts", rT.blockPartCount, "size", GetStringSize(rT.blockSize))
	if !parellelPBFT {
		rTNext := getRecordTimeAtHeight(height+1)
		rTNext.startNewHeightTime = rT.commitOverTime
	} else if height % parellelSteps == 0 {
		parellelStartTime = rT.commitOverTime
	}
	if (rT.endCreateBlockTime > 0 && rT.startCreateBlockTime > 0) {
		diff = rT.endCreateBlockTime - rT.startCreateBlockTime
		fmt.Println("*****Block height", height, "create block cost", GetTimeString(diff))
		fmt.Println("*****Block height", height, "end create block time", rT.endCreateBlockTime/1000000, "ms")
	}
	fmt.Println("*****Block height", height, "receive block time", rT.recevieBlockTime/1000000, "ms")
	diff = rT.enterPrevoteTime - rT.enterProposeTime
	fmt.Println("*****Block height", height, "propose cost", GetTimeString(diff))
	diff = rT.enterPrecommitTime - rT.enterPrevoteTime
	fmt.Println("*****Block height", height, "prevote cost", GetTimeString(diff))
	diff = rT.enterCommitTime - rT.enterPrecommitTime
	fmt.Println("*****Block height", height, "precommit cost", GetTimeString(diff))
	diff = rT.commitOverTime - rT.enterCommitTime
	fmt.Println("*****Block height", height, "commit cost", GetTimeString(diff))
	consensusTime := rT.commitOverTime - rT.enterProposeTime
	if (rT.blockTxs > 0) {
		tps := rT.blockTxs*1000000000/consensusTime
		eachTime := consensusTime/rT.blockTxs
		totalTps := (int64)(0)
		if (interval > 0) {
			totalTps = rT.blockTxs*1000000000/interval
		}
		if (ptxInBlock) {
			fmt.Println("*****Block height", height, "consensus time", GetTimeString(consensusTime), "ptxNum", rT.blockPtxs, "txNum", rT.blockTxs, "each tx cost", GetTimeString(eachTime))
		} else {
			fmt.Println("*****Block height", height, "consensus time", GetTimeString(consensusTime), "txNum", rT.blockTxs, "each tx cost", GetTimeString(eachTime))
		}
		if parellelPBFT {
			if height > 4 {
				parellelTxs = parellelTxs + rT.blockTxs
			}
			if height % parellelSteps == 0  && interval > 0 {
				parellelTps := parellelTxs*1000000000/interval
				fmt.Println("*****Block height", height, "txNum in 4 is", parellelTxs, "consensus tps", tps, "total tps", parellelTps)
				parellelTxs = 0
			} else {
				fmt.Println("*****Block height", height, "consensus tps", tps)
			}
		} else {
			fmt.Println("*****Block height", height, "consensus tps", tps, "total tps", totalTps)
		}
	} else {
		fmt.Println("*****Block height", height, "consensus time", GetTimeString(consensusTime))
	}
	removeRecordTime(height)
}
