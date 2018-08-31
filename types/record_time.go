package types

import (
	"fmt"
	"time"
	wire "github.com/tendermint/go-wire"
	emtConfig "github.com/dora/ultron/node/config"
)

var (
	enterProposeTime = (int64)(0)
	startCreateBlockTime = (int64)(0)
	endCreateBlockTime = (int64)(0)
	recevieBlockTime = (int64)(0)
	enterPrevoteTime = (int64)(0)
	receive23prevoteTime = (int64)(0)
	enterPrecommitTime = (int64)(0)
	receive23Precommit = (int64)(0)
	enterCommitTime = (int64)(0)
	commitOverTime = (int64)(0)
	startNewHeightTime = (int64)(0)
	blockHeight = (int64)(0)
	blockTxs = (int64)(0)
	blockPtxs = (int64)(0)
	blockSize = (int)(0)
	blockPartCount = (int)(0)
	printConsensusLog = false
	printSendRecvLog = false
	ptxInBlock = false
)

func RcInit() {
	testConfig, _ := emtConfig.ParseConfig()
	if (testConfig != nil) {
		if (testConfig.TestConfig.PrintConsensusLog) {
			printConsensusLog = true
		}
		if (testConfig.TestConfig.PrintSendRecvLog) {
			printSendRecvLog = true
		}
	}
}

func RcPtxInBlock() {
	ptxInBlock = true
}

func reset() {
	enterProposeTime = (int64)(0)
	startCreateBlockTime = (int64)(0)
	endCreateBlockTime = (int64)(0)
	recevieBlockTime = (int64)(0)
	enterPrevoteTime = (int64)(0)
	receive23prevoteTime = (int64)(0)
	enterPrecommitTime = (int64)(0)
	receive23Precommit = (int64)(0)
	enterCommitTime = (int64)(0)
	commitOverTime = (int64)(0)
	blockTxs = (int64)(0)
	blockPtxs = (int64)(0)
	blockSize = (int)(0)
	blockPartCount = (int)(0)
}

func RcenterPropose() {
	enterProposeTime = time.Now().UnixNano()
	// fmt.Println("*****Block height", blockHeight, "propose time", enterProposeTime)
}

func RcstartCreateBlock() {
    startCreateBlockTime = time.Now().UnixNano()
}

func RcendCreateBlock() {
    endCreateBlockTime = time.Now().UnixNano()
}

func RcreceiveBlock() {
    recevieBlockTime = time.Now().UnixNano()
}

func RcenterPrevote() {
	enterPrevoteTime = time.Now().UnixNano()
	// fmt.Println("*****Block height", blockHeight, "prevote time", enterPrevoteTime)
}

func Rcreceive23Prevote() {
    receive23prevoteTime = time.Now().UnixNano()
}

func RcenterPrecommit() {
	enterPrecommitTime = time.Now().UnixNano()
	// fmt.Println("*****Block height", blockHeight, "precommit time", enterPrecommitTime)
}

func Rcreceive23Precommit() {
    receive23Precommit = time.Now().UnixNano()
}

func RcenterCommit() {
    enterCommitTime = time.Now().UnixNano()
}

func RccommitOver() {
	commitOverTime = time.Now().UnixNano()
	RccalcTimeDiff()
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

func RcBlockHeight(block *Block, blockParts *PartSet) {
	blockHeight = block.Height
	blockPartCount = blockParts.count
	blockSize = len(wire.BinaryBytes(block))
	if (ptxInBlock && block.NumTxs > 0) {
		blockPtxs = block.NumTxs
		for _, tx := range block.Txs {
			ptx, _, size := DecodePtx(tx)
			if (ptx == nil) {
				fmt.Println("record time decodePtx is nil")
				continue
			}
			blockTxs = blockTxs + (int64)(size)
		}
	} else {
		blockTxs = block.NumTxs
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
	fmt.Println("*****Block height", blockHeight, "Send msg type", getMsgTypeString(id), "msg size", GetStringSize(size))
}

func RcReciveMsg(id byte, size int) {
	if (!printConsensusLog) {
		return
	}
	if (!printSendRecvLog) {
		return
	}
	fmt.Println("*****Block height", blockHeight, "Receive msg type", getMsgTypeString(id), "msg size", GetStringSize(size))

}

func RccalcTimeDiff() {
	if (!printConsensusLog) {
		return
	}
	diff := (int64)(0)
	interval := (int64)(0)
	if startNewHeightTime > 0 && commitOverTime > startNewHeightTime {
		interval = commitOverTime - startNewHeightTime
		fmt.Println("*****Block height", blockHeight, "interval", GetTimeString(interval))
	}
	fmt.Println("*****Block height", blockHeight, "block parts", blockPartCount, "block size", GetStringSize(blockSize))
	startNewHeightTime = commitOverTime
	if (endCreateBlockTime > 0 && startCreateBlockTime > 0) {
		diff = endCreateBlockTime - startCreateBlockTime
		fmt.Println("*****Block height", blockHeight, "create block cost", GetTimeString(diff))
		fmt.Println("*****Block height", blockHeight, "end create block time", endCreateBlockTime/1000000, "ms")
	}
	fmt.Println("*****Block height", blockHeight, "receive block time", recevieBlockTime/1000000, "ms")
	diff = enterPrevoteTime - enterProposeTime
	fmt.Println("*****Block height", blockHeight, "propose cost", GetTimeString(diff))
	diff = enterPrecommitTime - enterPrevoteTime
	fmt.Println("*****Block height", blockHeight, "prevote cost", GetTimeString(diff))
	diff = enterCommitTime - enterPrecommitTime
	fmt.Println("*****Block height", blockHeight, "precommit cost", GetTimeString(diff))
	diff = commitOverTime - enterCommitTime
	fmt.Println("*****Block height", blockHeight, "commit cost", GetTimeString(diff))
	consensusTime := commitOverTime - enterProposeTime
	if (blockTxs > 0) {
		tps := blockTxs*1000000000/consensusTime
		eachTime := consensusTime/blockTxs
		totalTps := (int64)(0)
		if (interval > 0) {
			totalTps = blockTxs*1000000000/interval
		}
		if (ptxInBlock) {
			fmt.Println("*****Block height", blockHeight, "consensus time", GetTimeString(consensusTime), "ptxNum", blockPtxs, "txNum", blockTxs, "each tx cost", GetTimeString(eachTime))
		} else {
			fmt.Println("*****Block height", blockHeight, "consensus time", GetTimeString(consensusTime), "txNum", blockTxs, "each tx cost", GetTimeString(eachTime))
		}
		
		fmt.Println("*****Block height", blockHeight, "consensus tps", tps, "total tps", totalTps)
	} else {
		fmt.Println("*****Block height", blockHeight, "consensus time", GetTimeString(consensusTime))
	}
	reset()
	blockHeight = blockHeight + 1
}
