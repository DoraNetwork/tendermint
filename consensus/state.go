package consensus

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	fail "github.com/ebuchman/fail-test"

	wire "github.com/tendermint/go-wire"
	cmn "github.com/tendermint/tmlibs/common"
	"github.com/tendermint/tmlibs/log"

	cfg "github.com/tendermint/tendermint/config"
	cstypes "github.com/tendermint/tendermint/consensus/types"
	sm "github.com/tendermint/tendermint/state"
	"github.com/tendermint/tendermint/types"
	data "github.com/tendermint/go-wire/data"

	emtConfig "github.com/dora/ultron/node/config"
)

//-----------------------------------------------------------------------------
// Config

const (
	proposalHeartbeatIntervalSeconds = 2
)

//-----------------------------------------------------------------------------
// Errors

var (
	ErrInvalidProposalSignature = errors.New("Error invalid proposal signature")
	ErrInvalidProposalPOLRound  = errors.New("Error invalid proposal POL round")
	ErrAddingVote               = errors.New("Error adding vote")
	ErrVoteHeightMismatch       = errors.New("Error vote height mismatch")
	ErrInvalidStateTransition   = errors.New("Error invalid state transition")
)

//-----------------------------------------------------------------------------

var (
	msgQueueSize = 1000
	initialHeight = int64(1)
	compactBlock = true
	buildFullBlock = false	// use compact block build full block with raw tx
	broadcastPtxHash = true
	disablePtx = false
	loadStateFromStore = false
)

// msgs from the reactor which may update the state
type msgInfo struct {
	Msg     ConsensusMessage `json:"msg"`
	PeerKey string           `json:"peer_key"`
}

// internally generated messages which may update the state
type timeoutInfo struct {
	Duration time.Duration         `json:"duration"`
	Height   int64                 `json:"height"`
	Round    int                   `json:"round"`
	Step     cstypes.RoundStepType `json:"step"`
}

func (ti *timeoutInfo) String() string {
	return fmt.Sprintf("%v ; %d/%d %v", ti.Duration, ti.Height, ti.Round, ti.Step)
}

type handleRoutine struct {
	id       int              // equals to (height % 4)
	quit     chan struct{}    // channel to receive quit signal
	msgQueue chan msgInfo     // channel to receive peer/internal messages
	tockChan chan timeoutInfo // channel to receive timeout info
}

// ConsensusState handles execution of the consensus algorithm.
// It processes votes and proposals, and upon reaching agreement,
// commits blocks to the chain and executes them against the application.
// The internal state machine receives input from peers, the internal validator, and from a timer.
type ConsensusState struct {
	cmn.BaseService

	// config details
	config        *cfg.ConsensusConfig
	privValidator types.PrivValidator // for signing votes

	// services for creating and executing blocks
	// TODO: encapsulate all of this in one "BlockManager"
	blockExec  *sm.BlockExecutor
	blockStore types.BlockStore
	mempool    types.Mempool
	evpool     types.EvidencePool

	// internal state
	mtx sync.Mutex
	resetStateMtx sync.Mutex
	precommitMtx sync.Mutex
	commitMtx sync.Mutex
	roundStates map[int64]*RoundStateWrapper
	state sm.State // State until height-1.

	// state changes may be triggered by msgs from peers,
	// msgs from ourself, or by timeouts
	peerMsgQueue     chan msgInfo
	internalMsgQueue chan msgInfo
	timeoutTicker    TimeoutTicker
	msgHandlers      [4]handleRoutine

	// we use eventBus to trigger msg broadcasts in the reactor,
	// and to notify external subscribers, eg. through a websocket
	eventBus *types.EventBus

	// a Write-Ahead Log ensures we can recover from any kind of crash
	// and helps us avoid signing conflicting votes
	wal          WAL
	replayMode   bool // so we don't log signing errors during replay
	doWALCatchup bool // determines if we even try to do the catchup

	// for tests where we want to limit the number of transitions the state makes
	nSteps int

	// for store the height when addProposalCMPCTBlock
	cmpctBlockHeight int64

	// some functions can be overwritten for testing
	decideProposal func(height int64, round int)
	doPrevote      func(height int64, round int)
	setProposal    func(proposal *types.Proposal, force bool) error

	// closed when we finish shutting down
	done chan struct{}

	startNewHeightTime time.Time
	enterProposeTime time.Time
}

type RoundStateWrapper struct {
	cstypes.RoundState
	state sm.State
	timeoutTicker    TimeoutTicker
	timeout          map[cstypes.RoundStepType]bool
}

// NewConsensusState returns a new ConsensusState.
func NewConsensusState(config *cfg.ConsensusConfig, state sm.State, blockExec *sm.BlockExecutor, blockStore types.BlockStore, mempool types.Mempool, evpool types.EvidencePool) *ConsensusState {
	cs := &ConsensusState{
		config:           config,
		blockExec:        blockExec,
		blockStore:       blockStore,
		mempool:          mempool,
		roundStates:      make(map[int64]*RoundStateWrapper),
		peerMsgQueue:     make(chan msgInfo, msgQueueSize),
		internalMsgQueue: make(chan msgInfo, msgQueueSize),
		timeoutTicker:    NewTimeoutTicker(),
		done:             make(chan struct{}),
		doWALCatchup:     true,
		wal:              nilWAL{},
		evpool:           evpool,
	}
	// set function defaults (may be overwritten before calling Start)
	cs.decideProposal = cs.defaultDecideProposal
	cs.doPrevote = cs.defaultDoPrevote
	cs.setProposal = cs.defaultSetProposal

	cs.updateToState(state)
	// Don't call scheduleRound0 yet.
	// We do that upon Start().
	cs.reconstructLastCommit(state)
	cs.BaseService = *cmn.NewBaseService(nil, "ConsensusState", cs)

	types.RcInit()

	testConfig, _ := emtConfig.ParseConfig()
	if testConfig != nil {
		if !testConfig.TestConfig.CompactBlock {
			compactBlock = false
		}
		if testConfig.TestConfig.BuildFullBlock {
			buildFullBlock = true
		}
		if !testConfig.TestConfig.UsePtxHash {
			broadcastPtxHash = false
		}
		if testConfig.TestConfig.DisablePtx {
			disablePtx = true
		}
	}

	return cs
}

//----------------------------------------
// Public interface

// SetLogger implements Service.
func (cs *ConsensusState) SetLogger(l log.Logger) {
	cs.BaseService.Logger = l
}

// SetEventBus sets event bus.
func (cs *ConsensusState) SetEventBus(b *types.EventBus) {
	cs.eventBus = b
	cs.blockExec.SetEventBus(b)
}

// String returns a string.
func (cs *ConsensusState) String() string {
	// better not to access shared variables
	return cmn.Fmt("ConsensusState") //(H:%v R:%v S:%v", cs.Height, cs.Round, cs.Step)
}

// GetState returns a copy of the chain state.
func (cs *ConsensusState) GetState() sm.State {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	return cs.state.Copy()
}

// GetRoundState returns a copy of the internal consensus state.
func (cs *ConsensusState) GetRoundState() *cstypes.RoundState {
	rsCopy := *cs.GetRoundStateAtHeight(cs.state.LastBlockHeight+1) // Copy
	return &rsCopy.RoundState
}

// Should be invoked within mutex protection
func (cs *ConsensusState) updateRoundStateAtHeight(height int64) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	if _, ok := cs.roundStates[height]; ok {
		cs.roundStates[height].state = cs.state.Copy()
		cs.roundStates[height].Validators = cs.roundStates[height].state.Validators
	}
}

func (cs *ConsensusState) startNewRound(height int64, round int) {
	cs.setStepTimeout(height, cstypes.RoundStepNewHeight)
	cs.enterNewRound(height, round+1)
	for i := 1; i <= 3; i++ {
		cs.scheduleRound0(cs.GetRoundStateAtHeight(height + int64(i)))
	}
}

// reset round state, roll back pipeline
func (cs *ConsensusState) resetRoundState(height int64, round int) {
	// use resetStateMtx as resetRoundState/enterNewRound may come not one time and need not reset each time
	cs.resetStateMtx.Lock()
	defer cs.resetStateMtx.Unlock()
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.Logger.Info(cmn.Fmt("resetRoundState(%v/%v):", height, round))
	rs := cs.getRoundStateAtHeight(height)
	if round <= rs.Round {
		cs.Logger.Debug(cmn.Fmt("resetRoundState(%v/%v): Invalid args. Current step: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))
		return
	}
	cs.cmpctBlockHeight = height
	for h := range cs.roundStates {
		if h > height {
			cs.roundStates[h].timeoutTicker.Stop()
			delete(cs.roundStates, h)
		}
		if h >= height {
			// restore txs back into mempool
			cs.mempool.Lock()
			cs.mempool.Restore(height)
			cs.mempool.Unlock()
		}
	}
}

func (cs *ConsensusState) TryGetRoundStateAtHeight(height int64) *cstypes.RoundState {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	if rs, ok := cs.roundStates[height]; ok {
		 rsCopy := *rs // Copy
		 return &rsCopy.RoundState
	}

	return nil
}

// GetRoundStateAtHeight Should be invoked within mutex protection
func (cs *ConsensusState) GetRoundStateAtHeight(height int64) *RoundStateWrapper {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	return cs.getRoundStateAtHeight(height)
}

func (cs *ConsensusState) getRoundStateAtHeight(height int64) *RoundStateWrapper {
	if _, ok := cs.roundStates[height]; !ok {
		rsWrapper := RoundStateWrapper {
			RoundState: cstypes.RoundState {
				Height: height,
				Round: 0,
				Step: cstypes.RoundStepNewHeight,
				StartTime: cs.config.Commit(time.Now()),
				CommitRound: -1,
			},
			timeoutTicker: NewTimeoutTicker(),
			timeout: make(map[cstypes.RoundStepType]bool),
		}
		if (loadStateFromStore && (height > 0)) {
			rsWrapper.state = sm.LoadStateAtHeight(cs.blockExec.GetDb(), height)
		} else {
			rsWrapper.state = cs.state.Copy()
			if height > 4 {
				rsB4 := cs.roundStates[height-4]
				if rsB4 != nil {
					rsWrapper.state = rsB4.state.Copy()
				} else {
					rsWrapper.state = sm.LoadStateAtHeight(cs.blockExec.GetDb(), height-4)
				}
			}
		}
		rsWrapper.Validators = rsWrapper.state.Validators
		if height > 0 {
			rsWrapper.Votes = cstypes.NewHeightVoteSet(cs.state.ChainID, height, rsWrapper.Validators)
		}

		rsWrapper.timeoutTicker.SetLogger(log.NewNopLogger())
		rsWrapper.timeoutTicker.Start()

		cs.roundStates[height] = &rsWrapper
	}
	return cs.roundStates[height]
}

func (cs *ConsensusState) GetVotesAtHeight(height int64) *cstypes.HeightVoteSet {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	rs := cs.getRoundStateAtHeight(height)
	return rs.Votes
}

func (cs *ConsensusState) GetValidatorSize(height int64) int {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	rs := cs.getRoundStateAtHeight(height)
	return rs.Validators.Size()
}

func (cs *ConsensusState) GetLastCommitSize(height int64) int {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	rs := cs.getRoundStateAtHeight(height)
	return rs.LastCommit.Size()
}

// GetValidators returns a copy of the current validators.
func (cs *ConsensusState) GetValidators() (int64, []*types.Validator) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	return cs.state.LastBlockHeight, cs.state.Validators.Copy().Validators
}

// SetPrivValidator sets the private validator account for signing votes.
func (cs *ConsensusState) SetPrivValidator(priv types.PrivValidator) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.privValidator = priv
}

// SetTimeoutTicker sets the local timer. It may be useful to overwrite for testing.
func (cs *ConsensusState) SetTimeoutTicker(timeoutTicker TimeoutTicker) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	// TODO: add test support
	cs.timeoutTicker = timeoutTicker
}

// LoadCommit loads the commit for a given height.
func (cs *ConsensusState) LoadCommit(height int64) *types.Commit {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	if height == cs.blockStore.Height() {
		return cs.blockStore.LoadSeenCommit(height)
	}
	return cs.blockStore.LoadBlockCommit(height)
}

// LoadBlockCommit loads the commit for a given height.
func (cs *ConsensusState) LoadBlockCommit(height int64) *types.Commit {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	return cs.blockStore.LoadBlockCommit(height)
}

// LoadSeenCommit loads the commit for a given height.
func (cs *ConsensusState) LoadSeenCommit(height int64) *types.Commit {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	return cs.blockStore.LoadSeenCommit(height)
}

// OnStart implements cmn.Service.
// It loads the latest state via the WAL, and starts the timeout and receive routines.
func (cs *ConsensusState) OnStart() error {
	// we may set the WAL in testing before calling Start,
	// so only OpenWAL if its still the nilWAL
	if _, ok := cs.wal.(nilWAL); ok {
		walFile := cs.config.WalFile()
		wal, err := cs.OpenWAL(walFile)
		if err != nil {
			cs.Logger.Error("Error loading ConsensusState wal", "err", err.Error())
			return err
		}
		cs.wal = wal
	}

	// we need the timeoutRoutine for replay so
	// we don't block on the tick chan.
	// NOTE: we will get a build up of garbage go routines
	// firing on the tockChan until the receiveRoutine is started
	// to deal with them (by that point, at most one will be valid)
	err := cs.timeoutTicker.Start()
	if err != nil {
		return err
	}

	// we may have lost some votes if the process crashed
	// reload from consensus log to catchup
	if cs.doWALCatchup {
		// TODO: handle catchup replay
		// if err := cs.catchupReplay(cs.Height); err != nil {
		// 	cs.Logger.Error("Error on catchup replay. Proceeding to start ConsensusState anyway", "err", err.Error())
			// NOTE: if we ever do return an error here,
			// make sure to stop the timeoutTicker
		// }
	}

	// start message handlers
	cs.startMsgHandlers()

	// now start the receiveRoutine
	go cs.receiveRoutine(0)

	// schedule the first 4 rounds!
	// use GetRoundState so we don't race the receiveRoutine for access
	initialHeight = cs.state.LastBlockHeight + 1

	for i := 1; i <= 4; i++ {
		cs.scheduleRound0(cs.GetRoundStateAtHeight(cs.state.LastBlockHeight + int64(i)))
	}


	return nil
}

// timeoutRoutine: receive requests for timeouts on tickChan and fire timeouts on tockChan
// receiveRoutine: serializes processing of proposoals, block parts, votes; coordinates state transitions
func (cs *ConsensusState) startRoutines(maxSteps int) {
	err := cs.timeoutTicker.Start()
	if err != nil {
		cs.Logger.Error("Error starting timeout ticker", "err", err)
		return
	}
	go cs.receiveRoutine(maxSteps)
}

// OnStop implements cmn.Service. It stops all routines and waits for the WAL to finish.
func (cs *ConsensusState) OnStop() {
	cs.BaseService.OnStop()

	cs.timeoutTicker.Stop()

	// Make BaseService.Wait() wait until cs.wal.Wait()
	if cs.IsRunning() {
		cs.wal.Wait()
	}
}

// Wait waits for the the main routine to return.
// NOTE: be sure to Stop() the event switch and drain
// any event channels or this may deadlock
func (cs *ConsensusState) Wait() {
	<-cs.done
}

// OpenWAL opens a file to log all consensus messages and timeouts for deterministic accountability
func (cs *ConsensusState) OpenWAL(walFile string) (WAL, error) {
	wal, err := NewWAL(walFile, cs.config.WalLight)
	if err != nil {
		cs.Logger.Error("Failed to open WAL for consensus state", "wal", walFile, "err", err)
		return nil, err
	}
	wal.SetLogger(cs.Logger.With("wal", walFile))
	if err := wal.Start(); err != nil {
		return nil, err
	}
	return wal, nil
}

//------------------------------------------------------------
// Public interface for passing messages into the consensus state, possibly causing a state transition.
// If peerKey == "", the msg is considered internal.
// Messages are added to the appropriate queue (peer or internal).
// If the queue is full, the function may block.
// TODO: should these return anything or let callers just use events?

// AddVote inputs a vote.
func (cs *ConsensusState) AddVote(vote *types.Vote, peerKey string) (added bool, err error) {
	if peerKey == "" {
		cs.internalMsgQueue <- msgInfo{&VoteMessage{vote}, ""}
	} else {
		cs.peerMsgQueue <- msgInfo{&VoteMessage{vote}, peerKey}
	}

	// TODO: wait for event?!
	return false, nil
}

// SetProposal inputs a proposal.
func (cs *ConsensusState) SetProposal(proposal *types.Proposal, peerKey string) error {

	if peerKey == "" {
		cs.internalMsgQueue <- msgInfo{&ProposalMessage{proposal}, ""}
	} else {
		cs.peerMsgQueue <- msgInfo{&ProposalMessage{proposal}, peerKey}
	}

	// TODO: wait for event?!
	return nil
}

// AddProposalBlockPart inputs a part of the proposal block.
func (cs *ConsensusState) AddProposalBlockPart(height int64, round int, part *types.Part, peerKey string) error {

	if peerKey == "" {
		cs.internalMsgQueue <- msgInfo{&BlockPartMessage{height, round, part}, ""}
	} else {
		cs.peerMsgQueue <- msgInfo{&BlockPartMessage{height, round, part}, peerKey}
	}

	// TODO: wait for event?!
	return nil
}

// SetProposalAndBlock inputs the proposal and all block parts.
func (cs *ConsensusState) SetProposalAndBlock(proposal *types.Proposal, block *types.Block, parts *types.PartSet, peerKey string) error {
	if err := cs.SetProposal(proposal, peerKey); err != nil {
		return err
	}
	for i := 0; i < parts.Total(); i++ {
		part := parts.GetPart(i)
		if err := cs.AddProposalBlockPart(proposal.Height, proposal.Round, part, peerKey); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------
// internal functions for managing the state

func (cs *ConsensusState) updateHeight(height int64) {
	rs := cs.GetRoundStateAtHeight(height)
	rs.Height = height
}

func (cs *ConsensusState) updateRoundStep(height int64, round int, step cstypes.RoundStepType) {
	rs := cs.GetRoundStateAtHeight(height)
	rs.Round = round
	rs.Step = step
}

// enterNewRound(height, 0) at cs.StartTime.
func (cs *ConsensusState) scheduleRound0(rs *RoundStateWrapper) {
	//cs.Logger.Info("scheduleRound0", "now", time.Now(), "startTime", cs.StartTime)
	sleepDuration := rs.StartTime.Sub(time.Now()) // nolint: gotype, gosimple
	cs.scheduleTimeout(sleepDuration, rs.Height, 0, cstypes.RoundStepNewHeight)
}

// Attempt to schedule a timeout (by sending timeoutInfo on the tickChan)
func (cs *ConsensusState) scheduleTimeout(duration time.Duration, height int64, round int, step cstypes.RoundStepType) {
	rs := cs.GetRoundStateAtHeight(height)
	rs.timeoutTicker.ScheduleTimeout(timeoutInfo{duration, height, round, step})
}

// send a msg into the receiveRoutine regarding our own proposal, block part, or vote
func (cs *ConsensusState) sendInternalMessage(mi msgInfo) {
	select {
	case cs.internalMsgQueue <- mi:
	default:
		// NOTE: using the go-routine means our votes can
		// be processed out of order.
		// TODO: use CList here for strict determinism and
		// attempt push to internalMsgQueue in receiveRoutine
		cs.Logger.Info("Internal msg queue is full. Using a go-routine")
		go func() { cs.internalMsgQueue <- mi }()
	}
}

// Reconstruct LastCommit from SeenCommit, which we saved along with the block,
// (which happens even before saving the state)
func (cs *ConsensusState) reconstructLastCommit(state sm.State) {
	if state.LastBlockHeight == 0 {
		return
	}
	// seenCommit := cs.blockStore.LoadSeenCommit(state.LastBlockHeight)
	// lastPrecommits := types.NewVoteSet(state.ChainID, state.LastBlockHeight, seenCommit.Round(), types.VoteTypePrecommit, state.LastValidators)
	// for _, precommit := range seenCommit.Precommits {
	// 	if precommit == nil {
	// 		continue
	// 	}
	// 	added, err := lastPrecommits.AddVote(precommit)
	// 	if !added || err != nil {
	// 		cmn.PanicCrisis(cmn.Fmt("Failed to reconstruct LastCommit: %v", err))
	// 	}
	// }
	// if !lastPrecommits.HasTwoThirdsMajority() {
	// 	cmn.PanicSanity("Failed to reconstruct LastCommit: Does not have +2/3 maj")
	// }
	loadStateFromStore = true
	for i := 0; i <= 3; i++ {
		if cs.state.LastBlockHeight - int64(i) > 0 {
			cs.GetRoundStateAtHeight(cs.state.LastBlockHeight - int64(i))
		}
	}
	loadStateFromStore = false
	// rs := cs.GetRoundStateAtHeight(state.LastBlockHeight + 1)
	// rs.LastCommit = lastPrecommits
}

// Updates ConsensusState and increments height to match that of state.
// The round becomes 0 and cs.Step becomes cstypes.RoundStepNewHeight.
func (cs *ConsensusState) updateToState(state sm.State) {
	// if cs.CommitRound > -1 && 0 < cs.Height && cs.Height != state.LastBlockHeight {
	// 	cmn.PanicSanity(cmn.Fmt("updateToState() expected state height of %v but found %v",
	// 		cs.Height, state.LastBlockHeight))
	// }
	// if !cs.state.IsEmpty() && cs.state.LastBlockHeight+1 != cs.Height {
	// 	// This might happen when someone else is mutating cs.state.
	// 	// Someone forgot to pass in state.Copy() somewhere?!
	// 	cmn.PanicSanity(cmn.Fmt("Inconsistent cs.state.LastBlockHeight+1 %v vs cs.Height %v",
	// 		cs.state.LastBlockHeight+1, cs.Height))
	// }

	// If state isn't further out than cs.state, just ignore.
	// This happens when SwitchToConsensus() is called in the reactor.
	// We don't want to reset e.g. the Votes.
	if !cs.state.IsEmpty() && (state.LastBlockHeight <= cs.state.LastBlockHeight) {
		cs.Logger.Info("Ignoring updateToState()", "newHeight", state.LastBlockHeight+1, "oldHeight", cs.state.LastBlockHeight+1)
		return
	}

	cs.state = state
}

func (cs *ConsensusState) RoundStateEvent(height int64) types.EventDataRoundState {
	rs := cs.GetRoundStateAtHeight(height)
	return rs.RoundStateEvent()
}

func (cs *ConsensusState) newStep(height int64) {
	rs := cs.RoundStateEvent(height)
	cs.wal.Save(rs)
	cs.nSteps += 1
	// newStep is called by updateToStep in NewConsensusState before the eventBus is set!
	if cs.eventBus != nil {
		cs.eventBus.PublishEventNewRoundStep(rs)
	}
}

//-----------------------------------------
// the main go routines

// receiveRoutine handles messages which may cause state transitions.
// it's argument (n) is the number of messages to process before exiting - use 0 to run forever
// It keeps the RoundState and is the only thing that updates it.
// Updates (state transitions) happen on timeouts, complete proposals, and 2/3 majorities.
// ConsensusState must be locked before any internal state is updated.
func (cs *ConsensusState) receiveRoutine(maxSteps int) {
	defer func() {
		if r := recover(); r != nil {
			cs.Logger.Error("CONSENSUS FAILURE!!!", "err", r, "stack", string(debug.Stack()))
		}
	}()

	for {
		if maxSteps > 0 {
			if cs.nSteps >= maxSteps {
				cs.Logger.Info("reached max steps. exiting receive routine")
				cs.nSteps = 0
				return
			}
		}
		var mi msgInfo

		select {
		case height := <-cs.mempool.TxsAvailable():
			cs.handleTxsAvailable(height)
		case txHeight := <-cs.mempool.TxResponsed():
			if txHeight < 0 {
				cs.Logger.Error("mempool TxResponsed height", txHeight, "abnormal, Use cmpctBlockHeight", cs.cmpctBlockHeight)
				txHeight = cs.cmpctBlockHeight
			}
			cs.handleBuildProposalBlock(txHeight)
		case mi = <-cs.peerMsgQueue:
			cs.wal.Save(mi)
			// handles proposals, block parts, votes
			// may generate internal events (votes, complete proposals, 2/3 majorities)
			cs.dispatchMsg(mi)
		case mi = <-cs.internalMsgQueue:
			cs.wal.Save(mi)
			// handles proposals, block parts, votes
			cs.dispatchMsg(mi)
		case ti := <-cs.timeoutTicker.Chan(): // tockChan:
			cs.wal.Save(ti)
			// if the timeout is relevant to the rs
			// go to the next step
			cs.dispatchTimeout(ti)
		case <-cs.Quit:

			// NOTE: the internalMsgQueue may have signed messages from our
			// priv_val that haven't hit the WAL, but its ok because
			// priv_val tracks LastSig

			// close wal now that we're done writing to it
			cs.wal.Stop()

			close(cs.done)
			return
		}
	}
}

func (cs *ConsensusState) dispatchMsg(mi msgInfo) {
	msg := mi.Msg
	var handler *handleRoutine
	switch msg := msg.(type) {
	case *ProposalMessage:
		handler = &cs.msgHandlers[msg.Proposal.Height % 4]
	case *BlockPartMessage:
		handler = &cs.msgHandlers[msg.Height % 4]
	case *CMPCTBlockPartMessage:
		handler = &cs.msgHandlers[msg.Height % 4]
	case *VoteMessage:
		handler = &cs.msgHandlers[msg.Vote.Height % 4]
	case *StateTransitionMessage:
		handler = &cs.msgHandlers[(msg.Height+1) % 4]
	default:
		cs.Logger.Error("Non-dispatchable msg type", reflect.TypeOf(msg))
	}

	if handler != nil {
		cs.Logger.Debug("Dispatch message", "handler", handler.id, "msg", msg)
		select {
		case handler.msgQueue <- mi:
		default:
			cs.Logger.Info("Handler msg queue is full")
			go func() { handler.msgQueue <- mi }()
		}
	}
}

func (cs *ConsensusState) dispatchTimeout(ti timeoutInfo) {
	cs.Logger.Debug("Dispatch timeout", "handler", ti.Height % 4, "ti", ti)
	handler := cs.msgHandlers[ti.Height % 4]
	select {
	case handler.tockChan <- ti:
	default:
		cs.Logger.Info("Handler tock chan is full")
		go func() { handler.tockChan <- ti }()
	}
}

func (cs *ConsensusState) startMsgHandlers() {
	for i := 0; i < 4; i++ {
		cs.msgHandlers[i] = handleRoutine {
			id: i,
			quit: make(chan struct{}),
			msgQueue: make(chan msgInfo, msgQueueSize),
			tockChan: make(chan timeoutInfo, tickTockBufferSize),
		}
		go cs.startHandleRoutine(i)
	}
}

func (cs *ConsensusState) startHandleRoutine(index int) {
	hr := cs.msgHandlers[index]
	for {
		select {
		case mi := <-hr.msgQueue:
			cs.handleMsg(mi)
		case ti := <-hr.tockChan:
			cs.handleTimeout(ti)
		case <-hr.quit:
			return
		}
	}
}

// state transitions on complete-proposal, 2/3-any, 2/3-one
func (cs *ConsensusState) handleMsg(mi msgInfo) {
	// cs.mtx.Lock()
	// defer cs.mtx.Unlock()

	var err error
	msg, peerKey := mi.Msg, mi.PeerKey
	switch msg := msg.(type) {
	case *ProposalMessage:
		// will not cause transition.
		// once proposal is set, we can receive block parts
		err = cs.setProposal(msg.Proposal, peerKey == "")
	case *BlockPartMessage:
		// if the proposal is complete, we'll enterPrevote or tryFinalizeCommit
		_, err = cs.addProposalBlockPart(msg.Height, msg.Part, peerKey != "")
	case *CMPCTBlockPartMessage:
		// if the proposal is complete, we'll enterPrevote or tryFinalizeCommit
		_, err = cs.addProposalCMPCTBlockPart(msg.Height, msg.Part, peerKey != "")
	case *VoteMessage:
		// attempt to add the vote and dupeout the validator if its a duplicate signature
		// if the vote gives us a 2/3-any or 2/3-one, we transition
		err := cs.tryAddVote(msg.Vote, peerKey)
		if err == ErrAddingVote {
			// TODO: punish peer
		}

		// NOTE: the vote is broadcast to peers by the reactor listening
		// for vote events

		// TODO: If rs.Height == vote.Height && rs.Round < vote.Round,
		// the peer is sending us CatchupCommit precommits.
		// We could make note of this and help filter in broadcastHasVoteMessage().
	case *StateTransitionMessage:
		err = cs.handleStateTransition(msg.Height, msg.Type)
	default:
		cs.Logger.Error("Unknown msg type", reflect.TypeOf(msg))
	}
	if err != nil {
		cs.Logger.Error("Error with msg", "type", reflect.TypeOf(msg), "peer", peerKey, "err", err, "msg", msg)
	}
}

func (cs *ConsensusState) handleTimeout(ti timeoutInfo) {
	cs.Logger.Debug("Received tock", "timeout", ti.Duration, "height", ti.Height, "round", ti.Round, "step", ti.Step)

	// cs.mtx.Lock()
	// defer cs.mtx.Unlock()

	// timeouts must be for current height, round, step
	rs := cs.GetRoundStateAtHeight(ti.Height)
	if ti.Height != rs.Height || ti.Round < rs.Round {
		cs.Logger.Debug("Ignoring tock because we're ahead", "height", rs.Height, "round", rs.Round)
		return
	}

	cs.setStepTimeout(ti.Height, ti.Step)

	// the timeout will now cause a state transition
	switch ti.Step {
	case cstypes.RoundStepNewHeight:
		// NewRound event fired from enterNewRound.
		// XXX: should we fire timeout here (for timeout commit)?
		cs.enterNewRound(ti.Height, 0)
		if rs.Step == cstypes.RoundStepWaitToPropose {
			cs.enterPropose(ti.Height, rs.Round)
		}
	case cstypes.RoundStepNewRound:
		cs.enterPropose(ti.Height, 0)
	case cstypes.RoundStepPropose:
		cs.eventBus.PublishEventTimeoutPropose(cs.RoundStateEvent(ti.Height))
		cs.enterPrevote(ti.Height, ti.Round)
		if rs.Step == cstypes.RoundStepWaitToPrevote {
			cs.enterPrevote(ti.Height, rs.Round)
		}
	case cstypes.RoundStepPrevoteWait:
		cs.eventBus.PublishEventTimeoutWait(cs.RoundStateEvent(ti.Height))
		cs.enterPrecommit(ti.Height, ti.Round)
		if rs.Step == cstypes.RoundStepWaitToPrecommit {
			cs.enterPrecommit(ti.Height, rs.Round)
		}
	case cstypes.RoundStepPrecommitWait:
		// rollback the pipeline
		cs.commitMtx.Lock()
		rs = cs.GetRoundStateAtHeight(ti.Height)
		if rs.Step == cstypes.RoundStepCommit {
			cs.commitMtx.Unlock()
			return
		}
		cs.commitMtx.Unlock()
		cs.eventBus.PublishEventTimeoutWait(cs.RoundStateEvent(ti.Height))
		cs.resetRoundState(ti.Height, ti.Round)
		cs.startNewRound(ti.Height, ti.Round)
	default:
		panic(cmn.Fmt("Invalid timeout step: %v", ti.Step))
	}

}

func (cs *ConsensusState) handleTxsAvailable(height int64) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	// we only need to do this for round 0
	cs.enterPropose(height, 0)
}

func (cs *ConsensusState) buildFullBlockFromCMPCTBlock(height int64) {
	rs := cs.GetRoundStateAtHeight(height)
	// if get all ptx with raw tx, build ProposalBlock with raw tx
	if (rs.ProposalCMPCTBlock != nil) {
		txs := make([]types.Tx, 0, len(rs.ProposalCMPCTBlock.Txs))
		for _, tx := range rs.ProposalCMPCTBlock.Txs {
			// TODO: handle GetTx return nil, which should not happen
			if disablePtx {
				_, txTemp := cs.mempool.GetTx(tx, types.RawTxHash, types.RawTx)
				txs = append(txs, txTemp)
			} else if compactBlock {
				_, ptxTemp := cs.mempool.GetTx(tx, types.ParallelTxHash, types.ParallelTx)
				txs = append(txs, ptxTemp)
			}
		}
		rs.ProposalBlock = rs.ProposalCMPCTBlock.CopyBlockWithTx(txs)
		rs.ProposalBlockParts = rs.ProposalBlock.MakePartSet(cs.state.ConsensusParams.BlockGossip.BlockPartSizeBytes)
		cs.Logger.Info("Build proposal block", "height", rs.ProposalBlock.Height, "hash", rs.ProposalBlock.Hash())
		cs.updateMemPool(height, rs)
		types.RcBlock(height, rs.ProposalBlock, rs.ProposalBlockParts)
		if cs.isProposalComplete(height) &&
			(rs.Step == cstypes.RoundStepPropose || rs.Step == cstypes.RoundStepWaitToPrevote) {
			// Move onto the next step
			cs.enterPrevote(height, rs.Round)
		} else if rs.Step == cstypes.RoundStepCommit {
			// If we're waiting on the proposal block...
			cs.tryFinalizeCommit(height)
			// Trigger next height to enter propose step
			cs.notifyStateTransition(height, cstypes.RoundStepPropose)
		} else if cs.isProposalComplete(height) {
			// Trigger next height to enter propose step
			cs.notifyStateTransition(height, cstypes.RoundStepPropose)
		}
	}
}

// handleBuildProposalBlock: handle requested tx and build ProposalBlock
func (cs *ConsensusState) handleBuildProposalBlock(height int64) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()

	if disablePtx || (broadcastPtxHash && buildFullBlock) {
		cs.buildFullBlockFromCMPCTBlock(height)
	} else {
		rs := cs.GetRoundStateAtHeight(height)
		if (rs.ProposalCMPCTBlock != nil) {
			rs.ProposalBlock = rs.ProposalCMPCTBlock
			rs.ProposalBlockParts = rs.ProposalCMPCTBlockParts
			cs.Logger.Info("All pending tx avaiable, assign cmpct block to ProposalBlock", "height", rs.ProposalBlock.Height, "hash", rs.ProposalBlock.Hash())
			if cs.isProposalComplete(height) &&
				(rs.Step == cstypes.RoundStepPropose || rs.Step == cstypes.RoundStepWaitToPrevote) {
				// Move onto the next step
				cs.enterPrevote(height, rs.Round)
			} else if rs.Step == cstypes.RoundStepCommit {
				// If we're waiting on the proposal block...
				cs.tryFinalizeCommit(height)
			}
		}
	}
}

func (cs* ConsensusState) notifyStateTransition(height int64, typ cstypes.RoundStepType) {
	cs.Logger.Debug("notifyStateTransition", "height", height, "type", typ)
	cs.sendInternalMessage(msgInfo{
		&StateTransitionMessage{
			Height: height,
			Type: typ,
		}, ""})
}

func (cs *ConsensusState) handleStateTransition(height int64, typ cstypes.RoundStepType) error {
	rs := cs.GetRoundStateAtHeight(height + 1)
	cs.Logger.Debug(cmn.Fmt("handleStateTransition height %v type %v next height %v step %v", height, typ, rs.Height, rs.Step))
	if typ == cstypes.RoundStepCommit {
		if rs.Step == cstypes.RoundStepWaitToPropose {
			cs.enterPropose(rs.Height, rs.Round)
		} else if rs.Step == cstypes.RoundStepWaitToPrevote {
			cs.enterPrevote(rs.Height, rs.Round)
		} else if rs.Step == cstypes.RoundStepWaitToPrecommit {
			cs.enterPrecommit(rs.Height, rs.Round)
		} else if rs.Step == cstypes.RoundStepWaitToCommit {
			cs.enterCommit(rs.Height, rs.Round)
		}
	} else if typ == cstypes.RoundStepPrecommit {
		if rs.Step == cstypes.RoundStepWaitToPropose {
			cs.enterPropose(rs.Height, rs.Round)
		} else if rs.Step == cstypes.RoundStepWaitToPrevote {
			cs.enterPrevote(rs.Height, rs.Round)
		} else if rs.Step == cstypes.RoundStepWaitToPrecommit {
			cs.enterPrecommit(rs.Height, rs.Round)
		}
	} else if typ == cstypes.RoundStepPrevote {
		if rs.Step == cstypes.RoundStepWaitToPropose {
			cs.enterPropose(rs.Height, rs.Round)
		} else if rs.Step == cstypes.RoundStepWaitToPrevote {
			cs.enterPrevote(rs.Height, rs.Round)
		}
	}
	return nil
}

//-----------------------------------------------------------------------------
// State functions
// Used internally by handleTimeout and handleMsg to make state transitions

// Enter: `timeoutNewHeight` by startTime (commitTime+timeoutCommit),
// 	or, if SkipTimeout==true, after receiving all precommits from (height,round-1)
// Enter: `timeoutPrecommits` after any +2/3 precommits from (height,round-1)
// Enter: +2/3 precommits for nil at (height,round-1)
// Enter: +2/3 prevotes any or +2/3 precommits for block or any from (height, round)
// NOTE: cs.StartTime was already set for height.
func (cs *ConsensusState) enterNewRound(height int64, round int) {
	cs.resetStateMtx.Lock()
	rs := cs.GetRoundStateAtHeight(height)
	if round < rs.Round || (rs.Round == round && rs.Step != cstypes.RoundStepNewHeight) {
		cs.resetStateMtx.Unlock()
		cs.Logger.Debug(cmn.Fmt("enterNewRound(%v/%v): Invalid args. Current step: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))
		return
	}

	if now := time.Now(); rs.StartTime.After(now) {
		cs.Logger.Info("Need to set a buffer and log message here for sanity.", "startTime", rs.StartTime, "now", now)
	}

	cs.Logger.Info(cmn.Fmt("enterNewRound(%v/%v). Current: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))

	// Increment validators if necessary
	// validators := cs.Validators
	// if cs.Round < round {
	// 	validators = validators.Copy()
	// 	validators.IncrementAccum(round - cs.Round)
	// }

	// Setup new round
	// we don't fire newStep for this step,
	// but we fire an event, so update the round step first
	cs.updateRoundStep(height, round, cstypes.RoundStepNewRound)
	if round == 0 {
		// We've already reset these upon new height,
		// and meanwhile we might have received a proposal
		// for round 0.
	} else {
		rs.RWMtx.Lock()
		rs.Proposal = nil
		rs.ProposalBlock = nil
		rs.ProposalBlockParts = nil
		rs.ProposalCMPCTBlock = nil
		rs.ProposalCMPCTBlockParts = nil
		rs.RWMtx.Unlock()
	}
	rs.Votes.SetRound(round + 1) // also track next round (round+1) to allow round-skipping

	cs.eventBus.PublishEventNewRound(cs.RoundStateEvent(height))

	// Wait for txs to be available in the mempool
	// before we enterPropose in round 0. If the last block changed the app hash,
	// we may need an empty "proof" block, and enterPropose immediately.
	waitForTxs := cs.config.WaitForTxs() && round == 0 && !cs.needProofBlock(height)
	if waitForTxs {
		if cs.config.CreateEmptyBlocksInterval > 0 {
			cs.scheduleTimeout(cs.config.EmptyBlocksInterval(), height, round, cstypes.RoundStepNewRound)
		}
		cs.resetStateMtx.Unlock()
		go cs.proposalHeartbeat(height, round)
	} else {
		cs.resetStateMtx.Unlock()
		// Enter propose may enterPrevote which use resetStateMtx
		cs.enterPropose(height, round)
	}
}

// needProofBlock returns true on the first height (so the genesis app hash is signed right away)
// and where the last block (height-1) caused the app hash to change
func (cs *ConsensusState) needProofBlock(height int64) bool {
	if height == 1 {
		return true
	}

	lastBlockMeta := cs.blockStore.LoadBlockMeta(height - 1)
	return !bytes.Equal(cs.state.AppHash, lastBlockMeta.Header.AppHash)
}

func (cs *ConsensusState) proposalHeartbeat(height int64, round int) {
	counter := 0
	addr := cs.privValidator.GetAddress()
	rs := cs.GetRoundStateAtHeight(height)
	valIndex, v := rs.Validators.GetByAddress(addr)
	if v == nil {
		// not a validator
		valIndex = -1
	}
	chainID := cs.state.ChainID
	for {
		rs := cs.GetRoundStateAtHeight(height)
		// if we've already moved on, no need to send more heartbeats
		if rs.Step > cstypes.RoundStepNewRound || rs.Round > round || rs.Height > height {
			return
		}
		heartbeat := &types.Heartbeat{
			Height:           rs.Height,
			Round:            rs.Round,
			Sequence:         counter,
			ValidatorAddress: addr,
			ValidatorIndex:   valIndex,
		}
		cs.privValidator.SignHeartbeat(chainID, heartbeat)
		cs.eventBus.PublishEventProposalHeartbeat(types.EventDataProposalHeartbeat{heartbeat})
		counter += 1
		time.Sleep(proposalHeartbeatIntervalSeconds * time.Second)
	}
}

// Enter (CreateEmptyBlocks): from enterNewRound(height,round)
// Enter (CreateEmptyBlocks, CreateEmptyBlocksInterval > 0 ): after enterNewRound(height,round), after timeout of CreateEmptyBlocksInterval
// Enter (!CreateEmptyBlocks) : after enterNewRound(height,round), once txs are in the mempool
func (cs *ConsensusState) enterPropose(height int64, round int) {
	rs := cs.GetRoundStateAtHeight(height)
	if round < rs.Round || (rs.Round == round && cstypes.RoundStepPropose <= rs.Step) {
		cs.Logger.Debug(cmn.Fmt("enterPropose(%v/%v): Invalid args. Current step: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))
		return
	}
	types.RcenterPropose(height)
	cs.Logger.Info(cmn.Fmt("enterPropose(%v/%v). Current: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))

	canMoveForward := cs.canEnterPropose(height, round)

	defer func() {
		if canMoveForward {
			// Done enterPropose:
			cs.updateRoundStep(height, round, cstypes.RoundStepPropose)
			cs.newStep(height)

			// If we have the whole proposal + POL, then goto Prevote now.
			// else, we'll enterPrevote when the rest of the proposal is received (in AddProposalBlockPart),
			// or else after timeoutPropose
			if cs.isProposalComplete(height) {
				cs.enterPrevote(height, rs.Round)
			}
		}
	}()

	// Nothing more to do if we're not a validator
	if cs.privValidator == nil {
		cs.Logger.Debug("This node is not a validator")
		return
	}

	if !cs.isProposer(height, round) {
		cs.Logger.Info("enterPropose: Not our turn to propose", "proposer", rs.Validators.GetProposer().Address, "privValidator", cs.privValidator, "height", height, "round", round)
		if rs.Validators.HasAddress(cs.privValidator.GetAddress()) {
			cs.Logger.Debug("This node is a validator")
		} else {
			cs.Logger.Debug("This node is not a validator")
		}

		if canMoveForward {
			// If we don't get the proposal and all block parts quick enough, enterPrevote
			cs.scheduleTimeout(cs.config.Propose(round), height, round, cstypes.RoundStepPropose)
		} else {
			cs.enterWaitToPropose(height, round)
		}
	} else {
		cs.Logger.Info("enterPropose: Our turn to propose", "proposer", rs.Validators.GetProposer().Address, "privValidator", cs.privValidator, "height", height, "round", round)
		cs.Logger.Debug("This node is a validator")
		if canMoveForward {
			// If we don't get the proposal and all block parts quick enough, enterPrevote
			cs.scheduleTimeout(cs.config.Propose(round), height, round, cstypes.RoundStepPropose)
			// create and broadcast proposal
			types.RcstartCreateBlock(height)
			cs.decideProposal(height, round)
			types.RcendCreateBlock(height)
		} else {
			// enter wait-to-propose step, woken up when previous height entered prevote
			cs.enterWaitToPropose(height, round)
		}
	}
}

func (cs *ConsensusState) canEnterPropose(height int64, round int) bool {
	if height == initialHeight {
		return true
	}

	preRs := cs.GetRoundStateAtHeight(height - 1)
	if preRs.Step < cstypes.RoundStepPrevote {
		cs.Logger.Debug("Can't enter propose: preRs.Step < RoundStepPrevote", "height", height, "round", round, "preRs.Step", preRs.Step)
		return false
	}

	// Previous height's proposal not available, wait
	if !preRs.IsProposalComplete() {
		cs.Logger.Debug("Can't enter propose: !preRs.IsProposalComplete()", "height", height, "round", round)
		return false
	}

	// Reached new height timeout?
	if !cs.config.PipelineNonstop() {
		rs := cs.GetRoundStateAtHeight(height)
		if !rs.timeout[cstypes.RoundStepNewHeight] {
			cs.Logger.Debug("Can't enter propose: !rs.timeout[cstypes.RoundStepNewHeight]", "height", height, "round", round)
			return false
		}
	}

	cs.Logger.Debug("Can enter propose: all conditions met")
	return true
}

func (cs *ConsensusState) enterWaitToPropose(height int64, round int) {
	cs.Logger.Info("enterWaitToPropse:", "height", height, "round", round)
	cs.updateRoundStep(height, round, cstypes.RoundStepWaitToPropose)
	cs.newStep(height)
}

func (cs *ConsensusState) isProposer(height int64, round int) bool {
	rsValidators := cs.state.Validators
	// Use height-4 validators
	if height <= 4 {
		rsValidators = cs.GetRoundStateAtHeight(0).state.Validators
	} else {
		rsValidators = cs.GetRoundStateAtHeight(height-4).state.Validators
	}
	return bytes.Equal(rsValidators.GetProposerAtHeight(height, round).Address, cs.privValidator.GetAddress())
}

// TODO: not use now and result maybe not correct
func (cs *ConsensusState) CheckIsProposer(height int64) bool {
	return cs.isProposer(height, 0)
}

func (cs *ConsensusState) GetPrivAddress() data.Bytes {
	if (cs.privValidator != nil) {
		return cs.privValidator.GetAddress()
	}
	return nil
}

func (cs *ConsensusState) defaultDecideProposal(height int64, round int) {
	cs.Logger.Debug("defaultDecideProposal", "height", height, "round", round)

	var block *types.Block
	var blockParts, cmpctBlockParts *types.PartSet

	// Decide on block
	rs := cs.GetRoundStateAtHeight(height)
	if rs.LockedBlock != nil {
		// If we're locked onto a block, just choose that.
		block, blockParts = rs.LockedBlock, rs.LockedBlockParts
		_, cmpctBlockParts = rs.LockedCMPCTBlock, rs.LockedCMPCTBlockParts
	} else {
		// Create a new proposal block from state/txs from the mempool.
		block, blockParts, _, cmpctBlockParts = cs.createProposalBlock(height)
		if block == nil { // on error
			return
		}
	}

	// Make proposal
	polRound, polBlockID := rs.Votes.POLInfo()
	// proposal header is cmpctBlockParts header(if not compact block mode, cmpctBlockParts is equal to blockParts)
	proposal := types.NewProposal(height, round, cmpctBlockParts.Header(), polRound, polBlockID)
	if err := cs.privValidator.SignProposal(cs.state.ChainID, proposal); err == nil {
		// Set fields
		/*  fields set by setProposal and addBlockPart
		cs.Proposal = proposal
		cs.ProposalBlock = block
		cs.ProposalBlockParts = blockParts
		*/

		// send proposal and block parts on internal msg queue
		cs.sendInternalMessage(msgInfo{&ProposalMessage{proposal}, ""})
		if (compactBlock) {
			for i := 0; i < cmpctBlockParts.Total(); i++ {
				part := cmpctBlockParts.GetPart(i)
				cs.sendInternalMessage(msgInfo{&CMPCTBlockPartMessage{rs.Height, rs.Round, part}, ""})
			}
		} else {
			for i := 0; i < blockParts.Total(); i++ {
				part := blockParts.GetPart(i)
				cs.sendInternalMessage(msgInfo{&BlockPartMessage{rs.Height, rs.Round, part}, ""})
			}
		}
		
		cs.Logger.Info("Signed proposal", "height", height, "round", round, "proposal", proposal)
		cs.Logger.Debug(cmn.Fmt("Signed proposal block: %v", block))
	} else {
		if !cs.replayMode {
			cs.Logger.Error("enterPropose: Error signing proposal", "height", height, "round", round, "err", err)
		}
	}
}

// Returns true if the proposal block is complete &&
// (if POLRound was proposed, we have +2/3 prevotes from there).
func (cs *ConsensusState) isProposalComplete(height int64) bool {
	rs := cs.GetRoundStateAtHeight(height)
	return rs.IsProposalComplete()
}

func (cs *ConsensusState) getB4State(height int64) sm.State {
	b4Height := int64(0)
	if (height > 4) {
		b4Height = height - 4
	}

	return cs.GetRoundStateAtHeight(b4Height).state
}

// Create the next block to propose and return it.
// Returns nil block upon error.
// NOTE: keep it side-effect free for clarity.
func (cs *ConsensusState) createProposalBlock(height int64) (block *types.Block, blockParts *types.PartSet,
		cmpctBlock *types.Block, cmpctBlockParts *types.PartSet) {
	var commit *types.Commit
	rs := cs.GetRoundStateAtHeight(height)
	if rs.Height <= 4 {
		// We're creating a proposal for the first block.
		// The commit is empty, but not nil.
		commit = &types.Commit{}
	} else {
		ancestorRs := cs.GetRoundStateAtHeight(height - 4)
		lastCommit := ancestorRs.Votes.Precommits(ancestorRs.CommitRound)
		if lastCommit.HasTwoThirdsMajority() {
			// Make the commit from LastCommit
			commit = lastCommit.MakeCommit()
		} else {
			// Load commit from blockstore if it's not available
			// commit = cs.LoadCommit(height - 4)
			commit = cs.LoadSeenCommit(height - 4)
			if commit == nil {
				// This shouldn't happen.
				cs.Logger.Error("enterPropose: Cannot propose anything: No commit for the previous block.")
				return
			}
		}
	}

	// Mempool validated transactions
	rsB4 := cs.getB4State(height)
	evidence := cs.evpool.PendingEvidence()
	if (compactBlock) {
		txsHash := cs.mempool.Reap(cs.config.MaxBlockSizeTxs)
		cmpctBlock, cmpctBlockParts := cs.state.MakeBlockForProposer(rsB4, cs.privValidator, rs.Height, txsHash, commit, evidence)
		block = cmpctBlock
		blockParts = cmpctBlockParts
		return block, blockParts, cmpctBlock, cmpctBlockParts
	} else {
		txs := cs.mempool.Reap(cs.config.MaxBlockSizeTxs)
		block, blockParts := cs.state.MakeBlockForProposer(rsB4, cs.privValidator, rs.Height, txs, commit, evidence)
		cmpctBlock = block
		cmpctBlockParts = blockParts
		return block, blockParts, cmpctBlock, cmpctBlockParts
	}
	return block, blockParts, cmpctBlock, cmpctBlockParts
}

func (cs *ConsensusState) setStepTimeout(height int64, step cstypes.RoundStepType) {
	rs := cs.GetRoundStateAtHeight(height)
	rs.timeout[step] = true
}

func (cs *ConsensusState) isStepTimeout(height int64, step cstypes.RoundStepType) bool {
	rs := cs.GetRoundStateAtHeight(height)
	to, ok := rs.timeout[step]
	return (ok && to)
}

// Enter: `timeoutPropose` after entering Propose.
// Enter: proposal block and POL is ready.
// Enter: any +2/3 prevotes for future round.
// Prevote for LockedBlock if we're locked, or ProposalBlock if valid.
// Otherwise vote nil.
func (cs *ConsensusState) enterPrevote(height int64, round int) {
	cs.resetStateMtx.Lock()
	defer cs.resetStateMtx.Unlock()
	rs := cs.GetRoundStateAtHeight(height)
	if round < rs.Round || (rs.Round == round && cstypes.RoundStepPrevote <= rs.Step) {
		cs.Logger.Debug(cmn.Fmt("enterPrevote(%v/%v): Invalid args. Current step: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))
		return
	}

	if rs.Step < cstypes.RoundStepPropose {
		cs.Logger.Debug(cmn.Fmt("enterPrevote(%v/%v): Not ready to enter prevote. Current step: %v", height, round, rs.Step))
		return
	}

	types.RcenterPrevote(height)
	cs.Logger.Info(cmn.Fmt("enterPrevote(%v/%v). Current: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))

	canMoveForward := cs.canEnterPrevote(height, round)

	defer func() {
		if canMoveForward {
			// Done enterPrevote:
			cs.updateRoundStep(height, round, cstypes.RoundStepPrevote)
			cs.newStep(height)
			// Notify next height to move forward
			cs.notifyStateTransition(height, cstypes.RoundStepPrevote)
		}
	}()

	// fire event for how we got here
	if cs.isProposalComplete(height) {
		cs.eventBus.PublishEventCompleteProposal(cs.RoundStateEvent(height))
	} else {
		// we received +2/3 prevotes for a future round
		// TODO: catchup event?
	}

	// Sign and broadcast vote as necessary
	if canMoveForward {
		cs.doPrevote(height, round)
	} else {
		// enter wait-to-prevote step, woken up when previous height enters precommit
		cs.enterWaitToPrevote(height, round)
	}

	// Once `addVote` hits any +2/3 prevotes, we will go to PrevoteWait
	// (so we have more time to try and collect +2/3 prevotes for a single block)
}

func (cs *ConsensusState) canEnterPrevote(height int64, round int) bool {
	if height == initialHeight {
		return true
	}

	preRs := cs.GetRoundStateAtHeight(height - 1)
	if preRs.Step < cstypes.RoundStepPrecommit {
		cs.Logger.Debug("Can't enter prevote: preRs.Step < RoundStepPrecommit", "height", height, "round", round, "preRs.Step", preRs.Step)
		return false
	}

	if cs.isStepTimeout(height, cstypes.RoundStepPropose) {
		cs.Logger.Debug("Can enter prevote: step RoundStepPropose timeout")
		return true
	}

	if cs.isProposalComplete(height) {
		cs.Logger.Debug("Can enter prevote: proposal complete")
		return true
	}

	cs.Logger.Debug("Can't enter prevote: none condition met", "height", height, "round", round)
	return false
}

func (cs *ConsensusState) enterWaitToPrevote(height int64, round int) {
	cs.Logger.Info("enterWaitToPrevote:", "height", height, "round", round)
	cs.updateRoundStep(height, round, cstypes.RoundStepWaitToPrevote)
	cs.newStep(height)
}

func (cs *ConsensusState) defaultDoPrevote(height int64, round int) {
	logger := cs.Logger.With("height", height, "round", round)
	// If a block is locked, prevote that.
	rs := cs.GetRoundStateAtHeight(height)
	if rs.LockedBlock != nil {
		logger.Info("enterPrevote: Block was locked")
		cs.signAddVote(rs, types.VoteTypePrevote, rs.LockedBlock.Hash(), rs.LockedBlockParts.Header(),
			rs.LockedCMPCTBlock.Hash(), rs.LockedCMPCTBlockParts.Header())
		return
	}

	// If ProposalBlock is nil, prevote nil.
	if rs.ProposalBlock == nil {
		logger.Info("enterPrevote: ProposalBlock is nil")
		cs.signAddVote(rs, types.VoteTypePrevote, nil, types.PartSetHeader{}, nil, types.PartSetHeader{})
		return
	}

	rsB4 := cs.getB4State(height)
	// Validate proposal block
	err := cs.blockExec.ValidateBlock(rsB4, rs.ProposalBlock)
	if err != nil {
		// ProposalBlock is invalid, prevote nil.
		logger.Error("enterPrevote: ProposalBlock is invalid", "err", err)
		cs.signAddVote(rs, types.VoteTypePrevote, nil, types.PartSetHeader{}, nil, types.PartSetHeader{})
		return
	}

	// Prevote cs.ProposalBlock
	// NOTE: the proposal signature is validated when it is received,
	// and the proposal block parts are validated as they are received (against the merkle hash in the proposal)
	logger.Info("enterPrevote: ProposalBlock is valid")
	cs.signAddVote(rs, types.VoteTypePrevote, rs.ProposalBlock.Hash(), rs.ProposalBlockParts.Header(),
		rs.ProposalCMPCTBlock.Hash(), rs.ProposalCMPCTBlockParts.Header())
}

// Enter: any +2/3 prevotes at next round.
func (cs *ConsensusState) enterPrevoteWait(height int64, round int) {
	cs.resetStateMtx.Lock()
	defer cs.resetStateMtx.Unlock()
	rs := cs.GetRoundStateAtHeight(height)
	if round < rs.Round || (rs.Round == round && cstypes.RoundStepPrevoteWait <= rs.Step) {
		cs.Logger.Debug(cmn.Fmt("enterPrevoteWait(%v/%v): Invalid args. Current step: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))
		return
	}
	if !rs.Votes.Prevotes(round).HasTwoThirdsAny() {
		cs.Logger.Info(cmn.Fmt("enterPrevoteWait(%v/%v), but Prevotes does not have any +2/3 votes", height, round))
		return
	}
	cs.Logger.Info(cmn.Fmt("enterPrevoteWait(%v/%v). Current: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))

	defer func() {
		// Done enterPrevoteWait:
		cs.updateRoundStep(height, round, cstypes.RoundStepPrevoteWait)
		cs.newStep(height)
	}()

	// Wait for some more prevotes; enterPrecommit
	cs.scheduleTimeout(cs.config.Prevote(round), height, round, cstypes.RoundStepPrevoteWait)
}

// Enter: `timeoutPrevote` after any +2/3 prevotes.
// Enter: +2/3 precomits for block or nil.
// Enter: any +2/3 precommits for next round.
// Lock & precommit the ProposalBlock if we have enough prevotes for it (a POL in this round)
// else, unlock an existing lock and precommit nil if +2/3 of prevotes were nil,
// else, precommit nil otherwise.
func (cs *ConsensusState) enterPrecommit(height int64, round int) {
	cs.precommitMtx.Lock()
	defer cs.precommitMtx.Unlock()
	rs := cs.GetRoundStateAtHeight(height)
	if round < rs.Round || (rs.Round == round && cstypes.RoundStepPrecommit <= rs.Step) {
		cs.Logger.Debug(cmn.Fmt("enterPrecommit(%v/%v): Invalid args. Current step: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))
		return
	}

	// Must enter prevote before entering precommit
	if rs.Step < cstypes.RoundStepPrevote && !rs.Votes.Prevotes(rs.Round).HasTwoThirdsAny() {
		cs.Logger.Debug(cmn.Fmt("enterPrecommit(%v/%v): Not ready to enter precommit. Current step: %v", height, round, rs.Step))
		return
	}

	types.RcenterPrecommit(height)
	cs.Logger.Info(cmn.Fmt("enterPrecommit(%v/%v). Current: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))

	canMoveForward := cs.canEnterPrecommit(height, round)

	defer func() {
		if canMoveForward {
			// Done enterPrecommit:
			cs.updateRoundStep(height, round, cstypes.RoundStepPrecommit)
			cs.newStep(height)
			// Notify next height to move forward
			cs.notifyStateTransition(height, cstypes.RoundStepPrecommit)
		}
	}()

	if !canMoveForward {
		cs.enterWaitToPrecommit(height, round)
		return
	}

	blockID, ok := rs.Votes.Prevotes(round).TwoThirdsMajority()

	// If we don't have a polka, we must precommit nil
	if !ok {
		if rs.LockedBlock != nil {
			cs.Logger.Info("enterPrecommit: No +2/3 prevotes during enterPrecommit while we're locked. Precommitting nil")
		} else {
			cs.Logger.Info("enterPrecommit: No +2/3 prevotes during enterPrecommit. Precommitting nil.")
		}
		cs.signAddVote(rs, types.VoteTypePrecommit, nil, types.PartSetHeader{}, nil, types.PartSetHeader{})
		return
	}

	// At this point +2/3 prevoted for a particular block or nil
	cs.eventBus.PublishEventPolka(cs.RoundStateEvent(height))

	// the latest POLRound should be this round
	polRound, _ := rs.Votes.POLInfo()
	if polRound < round {
		cmn.PanicSanity(cmn.Fmt("This POLRound should be %v but got %d", round, polRound))
	}

	// +2/3 prevoted nil. Unlock and precommit nil.
	if len(blockID.Hash) == 0 {
		if rs.LockedBlock == nil {
			cs.Logger.Info("enterPrecommit: +2/3 prevoted for nil.")
		} else {
			cs.Logger.Info("enterPrecommit: +2/3 prevoted for nil. Unlocking")
			rs.LockedRound = 0
			rs.LockedBlock = nil
			rs.LockedBlockParts = nil
			cs.eventBus.PublishEventUnlock(cs.RoundStateEvent(height))
		}
		cs.signAddVote(rs, types.VoteTypePrecommit, nil, types.PartSetHeader{}, nil, types.PartSetHeader{})
		return
	}

	// At this point, +2/3 prevoted for a particular block.

	// If we're already locked on that block, precommit it, and update the LockedRound
	if rs.LockedBlock.HashesTo(blockID.Hash) {
		cs.Logger.Info("enterPrecommit: +2/3 prevoted locked block. Relocking")
		rs.LockedRound = round
		cs.eventBus.PublishEventRelock(cs.RoundStateEvent(height))
		cs.signAddVote(rs, types.VoteTypePrecommit, blockID.Hash, blockID.PartsHeader, 
			rs.LockedCMPCTBlock.Hash(), rs.LockedCMPCTBlockParts.Header())
		return
	}

	// If +2/3 prevoted for proposal block, stage and precommit it
	if rs.ProposalBlock.HashesTo(blockID.Hash) {
		cs.Logger.Info("enterPrecommit: +2/3 prevoted proposal block. Locking", "hash", blockID.Hash)
		rsB4 := cs.getB4State(height)
		// Validate the block.
		if err := cs.blockExec.ValidateBlock(rsB4, rs.ProposalBlock); err != nil {
			cmn.PanicConsensus(cmn.Fmt("enterPrecommit: +2/3 prevoted for an invalid block: %v", err))
		}
		rs.LockedRound = round
		rs.LockedBlock = rs.ProposalBlock
		rs.LockedBlockParts = rs.ProposalBlockParts
		rs.LockedCMPCTBlock = rs.ProposalCMPCTBlock
		rs.LockedCMPCTBlockParts = rs.ProposalCMPCTBlockParts
		cs.eventBus.PublishEventLock(cs.RoundStateEvent(height))
		cs.signAddVote(rs, types.VoteTypePrecommit, blockID.Hash, blockID.PartsHeader,
			rs.ProposalCMPCTBlock.Hash(), rs.ProposalCMPCTBlockParts.Header())
		return
	}

	// There was a polka in this round for a block we don't have.
	// Fetch that block, unlock, and precommit nil.
	// The +2/3 prevotes for this round is the POL for our unlock.
	// TODO: In the future save the POL prevotes for justification.
	rs.LockedRound = 0
	rs.LockedBlock = nil
	rs.LockedBlockParts = nil

	if !rs.ProposalBlockParts.HasHeader(blockID.PartsHeader) {
		cs.Logger.Debug("Dont received committed block, reset ProposalBlockParts to let peer gossip data")
		CMPCTBlockID, ok := rs.Votes.Prevotes(round).GetCMPCTBlockMaj23()
		if compactBlock && !ok {
			cs.Logger.Error("Dont get cmpct block maj23 in precommit, this may not happen")
		}
		rs.ProposalBlock = nil
		rs.ProposalBlockParts = types.NewPartSetFromHeader(blockID.PartsHeader)
		rs.ProposalCMPCTBlock = nil
		rs.ProposalCMPCTBlockParts = types.NewPartSetFromHeader(CMPCTBlockID.PartsHeader)
	}
	cs.eventBus.PublishEventUnlock(cs.RoundStateEvent(height))
	cs.signAddVote(rs, types.VoteTypePrecommit, nil, types.PartSetHeader{}, nil, types.PartSetHeader{})
}

func (cs *ConsensusState) canEnterPrecommit(height int64, round int) bool {
	if height == initialHeight {
		return true
	}

	preRs := cs.GetRoundStateAtHeight(height - 1)
	if preRs.Step < cstypes.RoundStepCommit {
		cs.Logger.Debug("Can't enter precommit: preRs.Step < RoundStepCommit", "height", height, "round", round, "preRs.Step", preRs.Step)
		return false
	}

	if cs.isStepTimeout(height, cstypes.RoundStepPrevoteWait) {
		cs.Logger.Debug("Can enter precommit: step RoundStepPrevoteWait timeout")
		return true
	}

	rs := cs.GetRoundStateAtHeight(height)
	if rs.Votes.Prevotes(round).HasTwoThirdsAny() {
		cs.Logger.Debug("Can enter precommit: got +2/3 votes")
		return true
	}

	cs.Logger.Debug("Can't enter precommit: none condition met", "height", height, "round", round)
	return false
}

func (cs *ConsensusState) enterWaitToPrecommit(height int64, round int) {
	cs.Logger.Info("enterWaitToPrecommit:", "height", height, "round", round)
	cs.updateRoundStep(height, round, cstypes.RoundStepWaitToPrecommit)
	cs.newStep(height)
}

// Enter: any +2/3 precommits for next round.
func (cs *ConsensusState) enterPrecommitWait(height int64, round int) {
	rs := cs.GetRoundStateAtHeight(height)
	if round < rs.Round || (rs.Round == round && cstypes.RoundStepPrecommitWait <= rs.Step) {
		cs.Logger.Debug(cmn.Fmt("enterPrecommitWait(%v/%v): Invalid args. Current step: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))
		return
	}
	if !rs.Votes.Precommits(round).HasTwoThirdsAny() {
		cmn.PanicSanity(cmn.Fmt("enterPrecommitWait(%v/%v), but Precommits does not have any +2/3 votes", height, round))
	}
	cs.Logger.Info(cmn.Fmt("enterPrecommitWait(%v/%v). Current: %v/%v/%v", height, round, rs.Height, rs.Round, rs.Step))

	defer func() {
		// Done enterPrecommitWait:
		cs.updateRoundStep(height, round, cstypes.RoundStepPrecommitWait)
		cs.newStep(height)
	}()

	// Wait for some more precommits; enterNewRound
	cs.scheduleTimeout(cs.config.Precommit(round), height, round, cstypes.RoundStepPrecommitWait)

}

// Enter: +2/3 precommits for block
func (cs *ConsensusState) enterCommit(height int64, commitRound int) {
	cs.commitMtx.Lock()
	defer cs.commitMtx.Unlock()
	rs := cs.GetRoundStateAtHeight(height)
	if cstypes.RoundStepCommit <= rs.Step {
		cs.Logger.Debug(cmn.Fmt("enterCommit(%v/%v): Invalid args. Current step: %v/%v/%v", height, commitRound, rs.Height, rs.Round, rs.Step))
		return
	}

	if rs.Step < cstypes.RoundStepPrecommit {
		cs.Logger.Debug(cmn.Fmt("enterCommit(%v/%v): Not ready to enter commit. Current step: %v", height, commitRound, rs.Step))
		return
	}

	types.RcenterCommit(height)
	cs.Logger.Info(cmn.Fmt("enterCommit(%v/%v). Current: %v/%v/%v", height, commitRound, rs.Height, rs.Round, rs.Step))

	canMoveForward := cs.canEnterCommit(height, commitRound)

	defer func() {
		if canMoveForward {
			// Done enterCommit:
			// keep cs.Round the same, commitRound points to the right Precommits set.
			cs.updateRoundStep(height, rs.Round, cstypes.RoundStepCommit)
			rs.CommitRound = commitRound
			rs.CommitTime = time.Now()
			cs.newStep(height)

			// Maybe finalize immediately.
			cs.tryFinalizeCommit(height)

			// Notify next height to move forward
			cs.notifyStateTransition(height, cstypes.RoundStepCommit)
		}
	}()

	if !canMoveForward {
		cs.enterWaitToCommit(height, commitRound)
		return
	}

	blockID, ok := rs.Votes.Precommits(commitRound).TwoThirdsMajority()
	if !ok {
		cmn.PanicSanity("RunActionCommit() expects +2/3 precommits")
	}

	// The Locked* fields no longer matter.
	// Move them over to ProposalBlock if they match the commit hash,
	// otherwise they'll be cleared in updateToState.
	if rs.LockedBlock.HashesTo(blockID.Hash) {
		rs.ProposalBlock = rs.LockedBlock
		rs.ProposalBlockParts = rs.LockedBlockParts
		rs.ProposalCMPCTBlock = rs.LockedCMPCTBlock
		rs.ProposalCMPCTBlockParts = rs.LockedCMPCTBlockParts
	}

	// If we don't have the block being committed, set up to get it.
	if !rs.ProposalBlock.HashesTo(blockID.Hash) {
		if !rs.ProposalBlockParts.HasHeader(blockID.PartsHeader) {
			// We're getting the wrong block.
			// Set up ProposalBlockParts and keep waiting.
			CMPCTBlockID, ok := rs.Votes.Precommits(commitRound).GetCMPCTBlockMaj23()
			if compactBlock && !ok {
				cs.Logger.Error("Dont get cmpct block maj23, this may not happen")
			}
			rs.ProposalBlock = nil
			rs.ProposalBlockParts = types.NewPartSetFromHeader(blockID.PartsHeader)
			rs.ProposalCMPCTBlock = nil
			rs.ProposalCMPCTBlockParts = types.NewPartSetFromHeader(CMPCTBlockID.PartsHeader)
		} else {
			// We just need to keep waiting.
		}
	}
}

func (cs *ConsensusState) canEnterCommit(height int64, round int) bool {
	if height == initialHeight {
		return true
	}

	if cs.state.LastBlockHeight != height - 1 {
		cs.Logger.Debug("Can't enter commit: LastBlockHeight != height-1", "height", height, "round", round, "LastBlockHeight", cs.state.LastBlockHeight)
		return false
	}

	// No need to handle timeout here,
	// since precommit timeout will lead to pipeline rollback

	rs := cs.GetRoundStateAtHeight(height)

	if !rs.Votes.Precommits(round).HasTwoThirdsMajority() {
		cs.Logger.Debug("Can't enter commit: not reached +2/3 majority")
		return false
	}

	cs.Logger.Debug("Can enter commit: all conditions met")
	return true
}

func (cs *ConsensusState) enterWaitToCommit(height int64, round int) {
	cs.Logger.Info("enterWaitToCommit:", "height", height, "round", round)
	cs.updateRoundStep(height, round, cstypes.RoundStepWaitToCommit)
	cs.newStep(height)
}

// If we have the block AND +2/3 commits for it, finalize.
func (cs *ConsensusState) tryFinalizeCommit(height int64) {
	// if cs.Height != height {
	// 	cmn.PanicSanity(cmn.Fmt("tryFinalizeCommit() cs.Height: %v vs height: %v", cs.Height, height))
	// }

	rs := cs.GetRoundStateAtHeight(height)
	blockID, ok := rs.Votes.Precommits(rs.CommitRound).TwoThirdsMajority()
	if !ok || len(blockID.Hash) == 0 {
		cs.Logger.Error("Attempt to finalize failed. There was no +2/3 majority, or +2/3 was for <nil>.", "height", height)
		return
	}
	if !rs.ProposalBlock.HashesTo(blockID.Hash) {
		// TODO: this happens every time if we're not a validator (ugly logs)
		// TODO: ^^ wait, why does it matter that we're a validator?
		cs.Logger.Info("Attempt to finalize failed. We don't have the commit block.", "height", height, "proposal-block", rs.ProposalBlock.Hash(), "commit-block", blockID.Hash)
		return
	}

	//	go
	cs.finalizeCommit(height)
}

// Increment height and goto cstypes.RoundStepNewHeight
func (cs *ConsensusState) finalizeCommit(height int64) {
	// if cs.Height != height || cs.Step != cstypes.RoundStepCommit {
	// 	cs.Logger.Debug(cmn.Fmt("finalizeCommit(%v): Invalid args. Current step: %v/%v/%v", height, cs.Height, cs.Round, cs.Step))
	// 	return
	// }

	rs := cs.GetRoundStateAtHeight(height)
	if rs.Step != cstypes.RoundStepCommit {
		cs.Logger.Debug(cmn.Fmt("finalizeCommit(%v): Invalid args. Current step: %v/%v/%v", height, rs.Height, rs.Round, rs.Step))
		return
	}

	blockID, ok := rs.Votes.Precommits(rs.CommitRound).TwoThirdsMajority()
	cmpctBlk, block, blockParts := rs.ProposalCMPCTBlock, rs.ProposalBlock, rs.ProposalBlockParts

	if !ok {
		cmn.PanicSanity(cmn.Fmt("Cannot finalizeCommit, commit does not have two thirds majority"))
	}
	if !blockParts.HasHeader(blockID.PartsHeader) {
		cmn.PanicSanity(cmn.Fmt("Expected ProposalBlockParts header to be commit header"))
	}
	if !block.HashesTo(blockID.Hash) {
		cmn.PanicSanity(cmn.Fmt("Cannot finalizeCommit, ProposalBlock does not hash to commit hash"))
	}
	rsB4 := cs.getB4State(height)
	if err := cs.blockExec.ValidateBlock(rsB4, block); err != nil {
		cmn.PanicConsensus(cmn.Fmt("+2/3 committed an invalid block: %v", err))
	}

	cs.Logger.Info(cmn.Fmt("Finalizing commit of block with %d txs", block.NumTxs),
		"height", block.Height, "hash", block.Hash(), "root", block.AppHash)
	cs.Logger.Debug(cmn.Fmt("%v", block))

	fail.Fail() // XXX

	// Save to blockStore.
	if cs.blockStore.Height() < block.Height {
		// NOTE: the seenCommit is local justification to commit this block,
		// but may differ from the LastCommit included in the next block
		precommits := rs.Votes.Precommits(rs.CommitRound)
		seenCommit := precommits.MakeCommit()
		cs.blockStore.SaveBlock(block, blockParts, seenCommit)
	} else {
		// Happens during replay if we already saved the block but didn't commit
		cs.Logger.Info("Calling finalizeCommit on already stored block", "height", block.Height)
	}

	fail.Fail() // XXX

	// Finish writing to the WAL for this height.
	// NOTE: If we fail before writing this, we'll never write it,
	// and just recover by running ApplyBlock in the Handshake.
	// If we moved it before persisting the block, we'd have to allow
	// WAL replay for blocks with an #ENDHEIGHT
	// As is, ConsensusState should not be started again
	// until we successfully call ApplyBlock (ie. here or in Handshake after restart)
	cs.wal.Save(EndHeightMessage{height})

	fail.Fail() // XXX

	// Create a copy of the state for staging
	// and an event cache for txs
	stateCopy := cs.state.Copy()

	// Execute and commit the block, update and save the state, and update the mempool.
	// NOTE: the block.AppHash wont reflect these txs until the next block
	var err error
	stateCopy, err = cs.blockExec.ApplyBlock(stateCopy, rsB4, types.BlockID{block.Hash(), blockParts.Header()}, block, cmpctBlk)
	if err != nil {
		cs.Logger.Error("Error on ApplyBlock. Did the application crash? Please restart tendermint", "err", err)
		err := cmn.Kill()
		if err != nil {
			cs.Logger.Error("Failed to kill this process - please do so manually", "err", err)
		}
		return
	}

	fail.Fail() // XXX

	// NewHeightStep!
	cs.updateToState(stateCopy)

	fail.Fail() // XXX

	// stop round state timer
	rs.timeoutTicker.Stop()

	// clear useless round states
	cs.truncateRoundStates()

	cs.updateRoundStateAtHeight(height)

	// cs.StartTime is already set.
	// Schedule Round0 to start soon.
	cs.scheduleRound0(cs.GetRoundStateAtHeight(height + 4))

	// By here,
	// * cs.Height has been increment to height+1
	// * cs.Step is now cstypes.RoundStepNewHeight
	// * cs.StartTime is set to when we will start round0.

	types.RccommitOver(height)
}

func (cs *ConsensusState) truncateRoundStates() {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()

	currentHeight := cs.state.LastBlockHeight
	maxLen := int64(8)

	for h, rs := range cs.roundStates {
		if currentHeight - rs.Height > maxLen {
			delete(cs.roundStates, h)
		}
	}
}

//-----------------------------------------------------------------------------

func (cs *ConsensusState) defaultSetProposal(proposal *types.Proposal, force bool) error {
	// Already have one
	// TODO: possibly catch double proposals
	rs := cs.GetRoundStateAtHeight(proposal.Height)
	if !force && rs.Proposal != nil {
		return nil
	}

	// Does not apply
	if proposal.Height != rs.Height || proposal.Round != rs.Round {
		return nil
	}

	// We don't care about the proposal if we're already in cstypes.RoundStepCommit.
	if cstypes.RoundStepCommit <= rs.Step {
		return nil
	}

	// Verify POLRound, which must be -1 or between 0 and proposal.Round exclusive.
	if proposal.POLRound != -1 &&
		(proposal.POLRound < 0 || proposal.Round <= proposal.POLRound) {
		return ErrInvalidProposalPOLRound
	}

	// Verify signature
	rsB4 := cs.getB4State(proposal.Height)
	if !rsB4.Validators.GetProposerAtHeight(rs.Height, rs.Round).PubKey.VerifyBytes(types.SignBytes(cs.state.ChainID, proposal), proposal.Signature) {
		return ErrInvalidProposalSignature
	}

	rs.Proposal = proposal
	// if cmpct block, proposal.BlockPartsHeader is cmpct block header
	if compactBlock {
		rs.ProposalCMPCTBlockParts = types.NewPartSetFromHeader(proposal.BlockPartsHeader)
	} else {
		rs.ProposalBlockParts = types.NewPartSetFromHeader(proposal.BlockPartsHeader)
	}
	return nil
}

// Move txs in this block into uncommited txs map
func (cs *ConsensusState) updateMemPool(height int64, rs *RoundStateWrapper) {
	cs.mempool.Lock()
	if compactBlock {
		if rs.ProposalCMPCTBlock != nil && rs.ProposalCMPCTBlock.Data != nil {
			cs.mempool.Update(height, rs.ProposalCMPCTBlock.Data.Txs)
		} else {
			cs.mempool.Update(height, rs.ProposalBlock.Data.Txs)
		}
	} else {
		cs.mempool.Update(height, rs.ProposalBlock.Data.Txs)
	}
	cs.mempool.Unlock()
}

// NOTE: block is not necessarily valid.
// Asynchronously triggers either enterPrevote (before we timeout of propose) or tryFinalizeCommit, once we have the full block.
func (cs *ConsensusState) addProposalBlockPart(height int64, part *types.Part, verify bool) (added bool, err error) {
	cs.mtx.Lock()
	// Blocks might be reused, so round mismatch is OK
	// if cs.Height != height {
	// 	return false, nil
	// }

	rs := cs.getRoundStateAtHeight(height)

	// We're not expecting a block part.
	if rs.ProposalBlockParts == nil {
		cs.mtx.Unlock()
		return false, nil // TODO: bad peer? Return error?
	}
	added, err = rs.ProposalBlockParts.AddPart(part, verify)
	if err != nil {
		cs.mtx.Unlock()
		return added, err
	}
	cs.mtx.Unlock()
	if rs.ProposalBlockParts == nil {
		return false, nil // TODO: bad peer? Return error?
	}
	if added && rs.ProposalBlockParts.IsComplete() {
		// Added and completed!
		var n int
		var err error
		types.RcreceiveBlock(height)
		rs.ProposalBlock = wire.ReadBinary(&types.Block{}, rs.ProposalBlockParts.GetReader(),
			cs.state.ConsensusParams.BlockSize.MaxBytes, &n, &err).(*types.Block)
		// NOTE: it's possible to receive complete proposal blocks for future rounds without having the proposal
		cs.Logger.Info("Received complete proposal block", "height", rs.ProposalBlock.Height, "round", rs.Round, "hash", rs.ProposalBlock.Hash())

		cs.updateMemPool(height, rs)
		types.RcBlock(height, rs.ProposalBlock, rs.ProposalBlockParts)

		if cs.isProposalComplete(height) &&
			(rs.Step == cstypes.RoundStepPropose || rs.Step == cstypes.RoundStepWaitToPrevote) {
			// Move onto the next step
			cs.enterPrevote(height, rs.Round)
		} else if rs.Step == cstypes.RoundStepCommit {
			// If we're waiting on the proposal block...
			cs.tryFinalizeCommit(height)
			// Trigger next height to enter propose step
			cs.notifyStateTransition(height, cstypes.RoundStepPropose)
		} else if cs.isProposalComplete(height) {
			// Trigger next height to enter propose step
			cs.notifyStateTransition(height, cstypes.RoundStepPropose)
		}
		return true, err
	}
	return added, nil
}

// NOTE: block is not necessarily valid.
func (cs *ConsensusState) addProposalCMPCTBlockPart(height int64, part *types.Part, verify bool) (added bool, err error) {
	cs.mtx.Lock()

	// Blocks might be reused, so round mismatch is OK
	// if cs.Height != height {
	// 	return false, nil
	// }

	// We're not expecting a block part.
	// if cs.ProposalBlockParts == nil || cs.ProposalCMPCTBlockParts == nil {
	// 	return false, nil // TODO: bad peer? Return error?
	// }
	rs := cs.getRoundStateAtHeight(height)
	// added, err = cs.ProposalBlockParts.AddPart(part, verify)
	// We're not expecting a block part.
	if rs.ProposalCMPCTBlockParts == nil {
		cs.mtx.Unlock()
		return false, nil // TODO: bad peer? Return error?
	}
	added, err = rs.ProposalCMPCTBlockParts.AddPart(part, verify)
	if err != nil {
		cs.mtx.Unlock()
		return added, err
	}
	cs.mtx.Unlock()
	if rs.ProposalCMPCTBlockParts == nil {
		return false, nil // TODO: bad peer? Return error?
	}
	if added && rs.ProposalCMPCTBlockParts.IsComplete() {
		types.RcreceiveBlock(height)
		// Added and completed!
		var n int
		var err error
		// TODO: proposal block and proposal cmpct block parts need rebuild
		rs.ProposalCMPCTBlock = wire.ReadBinary(&types.Block{}, rs.ProposalCMPCTBlockParts.GetReader(),
			cs.state.ConsensusParams.BlockSize.MaxBytes, &n, &err).(*types.Block)
		cs.Logger.Info("Received complete proposal cmpct block", "height", rs.ProposalCMPCTBlock.Height, "round", rs.Round, "hash", rs.ProposalCMPCTBlock.Hash())
		cs.cmpctBlockHeight = height
		types.RcCMPCTBlock(height, rs.ProposalCMPCTBlock, rs.ProposalCMPCTBlockParts)
		// assign cmpctblock to block directly
		// filter the tx in cmpct block to ensure all tx app have, if dont, need get from other peer
		missTxBool := false
		if verify && len(rs.ProposalCMPCTBlock.Txs) != 0 {
			for _, tx := range rs.ProposalCMPCTBlock.Txs {
				missTxs := false
				if disablePtx {
					missTxs, _ = cs.mempool.GetTx(tx, types.RawTxHash, types.RawTxHash)
				} else if compactBlock {
					missTxs, _ = cs.mempool.GetTx(tx, types.ParallelTxHash, types.RawTxHash)
				}
				if missTxs {
					missTxBool = true
					cs.Logger.Info("There miss some tx in cmpct block")
				}
			}
			cs.Logger.Info("verify GetTx over")
		}
		// if do not need verify(is proposer) or all tx have in local,
		// Assign ProposalCMPCTBlock to ProposalBlock direclty
		if !verify || !missTxBool {
			if ((broadcastPtxHash && buildFullBlock) || disablePtx) && len(rs.ProposalCMPCTBlock.Txs) != 0 {
				cs.buildFullBlockFromCMPCTBlock(height)
			} else {
				rs.ProposalBlock = rs.ProposalCMPCTBlock
				rs.ProposalBlockParts = rs.ProposalCMPCTBlockParts
				cs.Logger.Info("Assign cmpct block to ProposalBlock directly", "height", rs.ProposalBlock.Height, "hash", rs.ProposalBlock.Hash())
				cs.updateMemPool(height, rs)
				types.RcBlock(height, rs.ProposalBlock, rs.ProposalBlockParts)
				if cs.isProposalComplete(height) &&
					(rs.Step == cstypes.RoundStepPropose || rs.Step == cstypes.RoundStepWaitToPrevote) {
					// Move onto the next step
					cs.enterPrevote(height, rs.Round)
				} else if rs.Step == cstypes.RoundStepCommit {
					// If we're waiting on the proposal block...
					cs.tryFinalizeCommit(height)
					// Trigger next height to enter propose step
					cs.notifyStateTransition(height, cstypes.RoundStepPropose)
				} else if cs.isProposalComplete(height) {
					// Trigger next height to enter propose step
					cs.notifyStateTransition(height, cstypes.RoundStepPropose)
				}
			}
		}
		return true, err
	}
	return added, nil
}

// Attempt to add the vote. if its a duplicate signature, dupeout the validator
func (cs *ConsensusState) tryAddVote(vote *types.Vote, peerKey string) error {
	_, err := cs.addVote(vote, peerKey)
	if err != nil {
		// If the vote height is off, we'll just ignore it,
		// But if it's a conflicting sig, add it to the cs.evpool.
		// If it's otherwise invalid, punish peer.
		if err == ErrVoteHeightMismatch {
			return err
		} else if voteErr, ok := err.(*types.ErrVoteConflictingVotes); ok {
			if bytes.Equal(vote.ValidatorAddress, cs.privValidator.GetAddress()) {
				cs.Logger.Error("Found conflicting vote from ourselves. Did you unsafe_reset a validator?", "height", vote.Height, "round", vote.Round, "type", vote.Type)
				return err
			}
			cs.evpool.AddEvidence(voteErr.DuplicateVoteEvidence)
			return err
		} else {
			// Probably an invalid signature / Bad peer.
			// Seems this can also err sometimes with "Unexpected step" - perhaps not from a bad peer ?
			cs.Logger.Error("Error attempting to add vote", "err", err)
			return ErrAddingVote
		}
	}
	return nil
}

//-----------------------------------------------------------------------------

func (cs *ConsensusState) addVote(vote *types.Vote, peerKey string) (added bool, err error) {
	cs.Logger.Debug("addVote", "voteHeight", vote.Height, "voteRound", vote.Round, "voteType", vote.Type, "valIndex", vote.ValidatorIndex)

	rs := cs.GetRoundStateAtHeight(vote.Height)
	// TODO: should consider below scenario?
	// A precommit for the previous height?
	// These come in while we wait timeoutCommit
	// if vote.Height+1 == cs.Height {
	// 	if !(cs.Step == cstypes.RoundStepNewHeight && vote.Type == types.VoteTypePrecommit) {
	// 		// TODO: give the reason ..
	// 		// fmt.Errorf("tryAddVote: Wrong height, not a LastCommit straggler commit.")
	// 		return added, ErrVoteHeightMismatch
	// 	}
	// 	added, err = cs.LastCommit.AddVote(vote)
	// 	if added {
	// 		cs.Logger.Info(cmn.Fmt("Added to lastPrecommits: %v", cs.LastCommit.StringShort()))
	// 		cs.eventBus.PublishEventVote(types.EventDataVote{vote})

	// 		// if we can skip timeoutCommit and have all the votes now,
	// 		if cs.config.SkipTimeoutCommit && cs.LastCommit.HasAll() {
	// 			// go straight to new round (skip timeout commit)
	// 			// cs.scheduleTimeout(time.Duration(0), cs.Height, 0, cstypes.RoundStepNewHeight)
	// 			cs.enterNewRound(cs.Height, 0)
	// 		}
	// 	}

	// 	return
	// }

	// Ignore votes for committed block
	if vote.Height <= cs.state.LastBlockHeight {
        err = ErrVoteHeightMismatch
        cs.Logger.Info("Vote ignored and not added", "voteHeight", vote.Height, "csHeight", cs.state.LastBlockHeight, "err", err)
        return
    }

	// A prevote/precommit for this height?
	height := rs.Height
	added, err = rs.Votes.AddVote(vote, peerKey)
	if added {
		cs.eventBus.PublishEventVote(types.EventDataVote{vote})

		switch vote.Type {
		case types.VoteTypePrevote:
			prevotes := rs.Votes.Prevotes(vote.Round)
			cs.Logger.Info("Added to prevote", "vote", vote, "prevotes", prevotes.StringShort())
			// First, unlock if prevotes is a valid POL.
			// >> lockRound < POLRound <= unlockOrChangeLockRound (see spec)
			// NOTE: If (lockRound < POLRound) but !(POLRound <= unlockOrChangeLockRound),
			// we'll still enterNewRound(H,vote.R) and enterPrecommit(H,vote.R) to process it
			// there.
			if (rs.LockedBlock != nil) && (rs.LockedRound < vote.Round) && (vote.Round <= rs.Round) {
				blockID, ok := prevotes.TwoThirdsMajority()
				if ok && !rs.LockedBlock.HashesTo(blockID.Hash) {
					cs.Logger.Info("Unlocking because of POL.", "lockedRound", rs.LockedRound, "POLRound", vote.Round)
					rs.LockedRound = 0
					rs.LockedBlock = nil
					rs.LockedBlockParts = nil
					rs.LockedCMPCTBlock = nil
					rs.LockedCMPCTBlockParts = nil
					cs.eventBus.PublishEventUnlock(cs.RoundStateEvent(height))
				}
			}
			if rs.Round <= vote.Round && prevotes.HasTwoThirdsAny() {
				// Round-skip over to PrevoteWait or goto Precommit.
				cs.enterNewRound(height, vote.Round) // if the vote is ahead of us
				if prevotes.HasTwoThirdsMajority() {
					cs.enterPrecommit(height, vote.Round)
				} else {
					cs.enterPrevote(height, vote.Round) // if the vote is ahead of us
					cs.enterPrevoteWait(height, vote.Round)
				}
			} else if rs.Proposal != nil && 0 <= rs.Proposal.POLRound && rs.Proposal.POLRound == vote.Round {
				// If the proposal is now complete, enter prevote of cs.Round.
				if cs.isProposalComplete(height) {
					cs.enterPrevote(height, rs.Round)
				}
			}
		case types.VoteTypePrecommit:
			precommits := rs.Votes.Precommits(vote.Round)
			cs.Logger.Info("Added to precommit", "vote", vote, "precommits", precommits.StringShort())
			blockID, ok := precommits.TwoThirdsMajority()
			if ok {
				if len(blockID.Hash) == 0 {
					// reset pbft pipeline
					cs.Logger.Info("precommit +2/3 nil")
					cs.resetRoundState(height, vote.Round)
					cs.startNewRound(height, vote.Round)
				} else {
					cs.enterNewRound(height, vote.Round)
					cs.enterPrecommit(height, vote.Round)
					cs.enterCommit(height, vote.Round)

					if cs.config.SkipTimeoutCommit && precommits.HasAll() {
						// if we have all the votes now,
						// go straight to new round (skip timeout commit)
						// cs.scheduleTimeout(time.Duration(0), cs.Height, 0, cstypes.RoundStepNewHeight)
						cs.enterNewRound(rs.Height+1, 0)
					}

				}
			} else if rs.Round <= vote.Round && precommits.HasTwoThirdsAny() {
				cs.enterNewRound(height, vote.Round)
				cs.enterPrecommit(height, vote.Round)
				cs.enterPrecommitWait(height, vote.Round)
			}
		default:
			cmn.PanicSanity(cmn.Fmt("Unexpected vote type %X", vote.Type)) // Should not happen.
		}
	}

	return
}

func (cs *ConsensusState) signVote(rs *RoundStateWrapper, type_ byte, hash []byte, header types.PartSetHeader,
		cmpctHash []byte, cmpctHeader types.PartSetHeader) (*types.Vote, error) {
	addr := cs.privValidator.GetAddress()
	valIndex, _ := rs.Validators.GetByAddress(addr)
	vote := &types.Vote{
		ValidatorAddress: addr,
		ValidatorIndex:   valIndex,
		Height:           rs.Height,
		Round:            rs.Round,
		Timestamp:        time.Now().UTC(),
		Type:             type_,
		BlockID:          types.BlockID{hash, header},
		CMPCTBlockID:     types.BlockID{cmpctHash, cmpctHeader},
	}
	err := cs.privValidator.SignVote(cs.state.ChainID, vote)
	return vote, err
}

// sign the vote and publish on internalMsgQueue
func (cs *ConsensusState) signAddVote(rs *RoundStateWrapper, type_ byte, 
		hash []byte, header types.PartSetHeader, cmpctHash []byte, cmpctHeader types.PartSetHeader) *types.Vote {
	// if we don't have a key or we're not in the validator set, do nothing
	if cs.privValidator == nil || !rs.Validators.HasAddress(cs.privValidator.GetAddress()) {
		return nil
	}
	vote, err := cs.signVote(rs, type_, hash, header, cmpctHash, cmpctHeader)
	if err == nil {
		cs.sendInternalMessage(msgInfo{&VoteMessage{vote}, ""})
		cs.Logger.Info("Signed and pushed vote", "height", rs.Height, "round", rs.Round, "vote", vote, "err", err)
		return vote
	} else {
		//if !cs.replayMode {
		cs.Logger.Error("Error signing vote", "height", rs.Height, "round", rs.Round, "vote", vote, "err", err)
		//}
		return nil
	}
}

func (cs *ConsensusState) StringIndented(indent string) string {
	rs := cs.GetRoundStateAtHeight(cs.state.LastBlockHeight)
	return rs.StringIndented(indent)
}

//---------------------------------------------------------

func CompareHRS(h1 int64, r1 int, s1 cstypes.RoundStepType, h2 int64, r2 int, s2 cstypes.RoundStepType) int {
	if h1 < h2 {
		return -1
	} else if h1 > h2 {
		return 1
	}
	if r1 < r2 {
		return -1
	} else if r1 > r2 {
		return 1
	}
	if s1 < s2 {
		return -1
	} else if s1 > s2 {
		return 1
	}
	return 0
}
