package consensus

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"

	wire "github.com/tendermint/go-wire"
	cmn "github.com/tendermint/tmlibs/common"
	"github.com/tendermint/tmlibs/log"

	cstypes "github.com/tendermint/tendermint/consensus/types"
	"github.com/tendermint/tendermint/p2p"
	sm "github.com/tendermint/tendermint/state"
	"github.com/tendermint/tendermint/types"
	mempoolR "github.com/tendermint/tendermint/mempool"
)

const (
	StateChannel       = byte(0x20)
	DataChannel        = byte(0x21)
	VoteChannel        = byte(0x22)
	VoteSetBitsChannel = byte(0x23)

	maxConsensusMessageSize = 1048576 // 1MB; NOTE/TODO: keep in sync with types.PartSet sizes.
)

//-----------------------------------------------------------------------------

// ConsensusReactor defines a reactor for the consensus service.
type ConsensusReactor struct {
	p2p.BaseReactor // BaseService + p2p.Switch

	conS *ConsensusState

	mtx      sync.RWMutex
	fastSync bool
	eventBus *types.EventBus

	pmtx	sync.RWMutex
	peers  map[string]*p2p.Peer
}

// NewConsensusReactor returns a new ConsensusReactor with the given consensusState.
func NewConsensusReactor(consensusState *ConsensusState, fastSync bool) *ConsensusReactor {
	conR := &ConsensusReactor{
		conS:     consensusState,
		fastSync: fastSync,
	}
	conR.BaseReactor = *p2p.NewBaseReactor("ConsensusReactor", conR)
	conR.peers = make(map[string]*p2p.Peer)
	return conR
}

// OnStart implements BaseService.
func (conR *ConsensusReactor) OnStart() error {
	conR.Logger.Info("ConsensusReactor ", "fastSync", conR.FastSync())
	if err := conR.BaseReactor.OnStart(); err != nil {
		return err
	}

	err := conR.startBroadcastRoutine()
	if err != nil {
		return err
	}

	if !conR.FastSync() {
		err := conR.conS.Start()
		if err != nil {
			return err
		}
	}

	go conR.broadcastGetTxRoutine()

	return nil
}

// broadcastGetTxRoutine broadcast GetTxMessage to remote peer
// Note: Send GetTxMessage should be in mempool/reactor,
//       but it can not get the peer which contain the tx, so put it here, not good
func (conR *ConsensusReactor) broadcastGetTxRoutine() {
	for {
		select {
		case txMsg := <- conR.conS.mempool.TxsFetching():
			msg := &mempoolR.GetTxMessage{Hash: txMsg.TxHash}
			for _, peer := range conR.peers {
				ps := (*peer).Get(types.PeerStateKey).(*PeerState)
				prs := ps.GetRoundStateAtHeight(txMsg.Height)
				// send GetTxMessage to have proposalBlock one
				if prs.ProposalCMPCTBlockParts.IsFull() {
					conR.Logger.Info("broadcastGetTxRoutine Send GetTxMessage to peer", (*peer).Key())
					success := (*peer).Send(mempoolR.MempoolChannel,
						struct{ mempoolR.MempoolMessage }{msg})
					if !success {
						conR.Logger.Error("consensus send GetTxMessage not success")
					}
				}
			}
		}
	}
}

// OnStop implements BaseService
func (conR *ConsensusReactor) OnStop() {
	conR.BaseReactor.OnStop()
	conR.conS.Stop()
}

// SwitchToConsensus switches from fast_sync mode to consensus mode.
// It resets the state, turns off fast_sync, and starts the consensus state-machine
func (conR *ConsensusReactor) SwitchToConsensus(state sm.State, blocksSynced int) {
	conR.Logger.Info("SwitchToConsensus")
	conR.conS.reconstructLastCommit(state)
	// NOTE: The line below causes broadcastNewRoundStepRoutine() to
	// broadcast a NewRoundStepMessage.
	conR.conS.updateToState(state)

	conR.mtx.Lock()
	conR.fastSync = false
	conR.mtx.Unlock()

	if blocksSynced > 0 {
		// dont bother with the WAL if we fast synced
		conR.conS.doWALCatchup = false
	}
	err := conR.conS.Start()
	if err != nil {
		conR.Logger.Error("Error starting conS", "err", err)
	}
}

// GetChannels implements Reactor
func (conR *ConsensusReactor) GetChannels() []*p2p.ChannelDescriptor {
	// TODO optimize
	return []*p2p.ChannelDescriptor{
		{
			ID:                StateChannel,
			Priority:          5,
			SendQueueCapacity: 100,
		},
		{
			ID:                 DataChannel, // maybe split between gossiping current block and catchup stuff
			Priority:           10,          // once we gossip the whole block there's nothing left to send until next height or round
			SendQueueCapacity:  100,
			RecvBufferCapacity: 50 * 4096,
		},
		{
			ID:                 VoteChannel,
			Priority:           5,
			SendQueueCapacity:  100,
			RecvBufferCapacity: 100 * 100,
		},
		{
			ID:                 VoteSetBitsChannel,
			Priority:           1,
			SendQueueCapacity:  2,
			RecvBufferCapacity: 1024,
		},
	}
}

// AddPeer implements Reactor
func (conR *ConsensusReactor) AddPeer(peer p2p.Peer) {
	if !conR.IsRunning() {
		return
	}

	conR.pmtx.Lock()
	if _, ok := conR.peers[peer.Key()]; !ok {
		conR.peers[peer.Key()] = &peer
	}
	conR.pmtx.Unlock()

	// Create peerState for peer
	peerState := NewPeerState(peer).SetLogger(conR.Logger)
	peer.Set(types.PeerStateKey, peerState)

	// Begin routines for this peer.
	go conR.gossipDataRoutine(peer, peerState)
	go conR.gossipVotesRoutine(peer, peerState)
	go conR.queryMaj23Routine(peer, peerState)

	// Send our state to peer.
	// If we're fast_syncing, broadcast a RoundStepMessage later upon SwitchToConsensus().
	if !conR.FastSync() {
		conR.sendNewRoundStepMessages(peer)
	}
}

// RemovePeer implements Reactor
func (conR *ConsensusReactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	if !conR.IsRunning() {
		return
	}
	conR.pmtx.Lock()
	if _, ok := conR.peers[peer.Key()]; ok {
		delete(conR.peers, peer.Key())
	}
	conR.pmtx.Unlock()
	// TODO
	//peer.Get(PeerStateKey).(*PeerState).Disconnect()
}

// Receive implements Reactor
// NOTE: We process these messages even when we're fast_syncing.
// Messages affect either a peer state or the consensus state.
// Peer state updates can happen in parallel, but processing of
// proposals, block parts, and votes are ordered by the receiveRoutine
// NOTE: blocks on consensus state for proposals, block parts, and votes
func (conR *ConsensusReactor) Receive(chID byte, src p2p.Peer, msgBytes []byte) {
	if !conR.IsRunning() {
		conR.Logger.Debug("Receive", "src", src, "chId", chID, "bytes", msgBytes)
		return
	}

	_, msg, err := DecodeMessage(msgBytes)
	if err != nil {
		conR.Logger.Error("Error decoding message", "src", src, "chId", chID, "msg", msg, "err", err, "bytes", msgBytes)
		// TODO punish peer?
		return
	}
	conR.Logger.Debug("Receive", "src", src, "chId", chID, "msg", msg)

	// Get peer states
	ps := src.Get(types.PeerStateKey).(*PeerState)

	switch chID {
	case StateChannel:
		switch msg := msg.(type) {
		case *NewRoundStepMessage:
			ps.ApplyNewRoundStepMessage(msg)
		case *CommitStepMessage:
			ps.ApplyCommitStepMessage(msg)
		case *HasVoteMessage:
			ps.ApplyHasVoteMessage(msg)
		case *VoteSetMaj23Message:
			cs := conR.conS
			votes := cs.GetVotesAtHeight(msg.Height)
			// Peer claims to have a maj23 for some BlockID at H,R,S,
			votes.SetPeerMaj23(msg.Round, msg.Type, ps.Peer.Key(), msg.BlockID)
			// Respond with a VoteSetBitsMessage showing which votes we have.
			// (and consequently shows which we don't have)
			var ourVotes *cmn.BitArray
			switch msg.Type {
			case types.VoteTypePrevote:
				ourVotes = votes.Prevotes(msg.Round).BitArrayByBlockID(msg.BlockID)
			case types.VoteTypePrecommit:
				ourVotes = votes.Precommits(msg.Round).BitArrayByBlockID(msg.BlockID)
			default:
				conR.Logger.Error("Bad VoteSetBitsMessage field Type")
				return
			}

			if ourVotes == nil {
				// Rollback may have happened so related round state has been reseted
				conR.Logger.Debug("Unable to find local votes", "height", msg.Height, "round", msg.Round)
				return
			}
			src.TrySend(VoteSetBitsChannel, struct{ ConsensusMessage }{&VoteSetBitsMessage{
				Height:  msg.Height,
				Round:   msg.Round,
				Type:    msg.Type,
				BlockID: msg.BlockID,
				Votes:   ourVotes,
			}})
		case *ProposalHeartbeatMessage:
			hb := msg.Heartbeat
			conR.Logger.Debug("Received proposal heartbeat message",
				"height", hb.Height, "round", hb.Round, "sequence", hb.Sequence,
				"valIdx", hb.ValidatorIndex, "valAddr", hb.ValidatorAddress)
		default:
			conR.Logger.Error(cmn.Fmt("Unknown message type %v", reflect.TypeOf(msg)))
		}

	case DataChannel:
		if conR.FastSync() {
			conR.Logger.Info("Ignoring message received during fastSync", "msg", msg)
			return
		}
		switch msg := msg.(type) {
		case *ProposalMessage:
			ps.SetHasProposal(msg.Proposal)
			conR.conS.peerMsgQueue <- msgInfo{msg, src.Key()}
		case *ProposalPOLMessage:
			ps.ApplyProposalPOLMessage(msg)
		case *BlockPartMessage:
			ps.SetHasProposalBlockPart(msg.Height, msg.Round, msg.Part.Index)
			conR.conS.peerMsgQueue <- msgInfo{msg, src.Key()}
		case *CMPCTBlockPartMessage:
			ps.SetHasProposalCMPCTBlockPart(msg.Height, msg.Round, msg.Part.Index)
			conR.conS.peerMsgQueue <- msgInfo{msg, src.Key()}
		default:
			conR.Logger.Error(cmn.Fmt("Unknown message type %v", reflect.TypeOf(msg)))
		}

	case VoteChannel:
		if conR.FastSync() {
			conR.Logger.Info("Ignoring message received during fastSync", "msg", msg)
			return
		}
		switch msg := msg.(type) {
		case *VoteMessage:
			conR.Logger.Debug("Received VoteMessage", "peer", src.Key(), "msg", msg)
			cs := conR.conS
			height := msg.Vote.Height
			valSize := cs.GetValidatorSize(height)
			lastCommitSize := cs.GetLastCommitSize(height)
			ps.EnsureVoteBitArrays(height, valSize)
			ps.EnsureVoteBitArrays(height-1, lastCommitSize)
			ps.SetHasVote(msg.Vote)

			cs.peerMsgQueue <- msgInfo{msg, src.Key()}

		default:
			// don't punish (leave room for soft upgrades)
			conR.Logger.Error(cmn.Fmt("Unknown message type %v", reflect.TypeOf(msg)))
		}

	case VoteSetBitsChannel:
		if conR.FastSync() {
			conR.Logger.Info("Ignoring message received during fastSync", "msg", msg)
			return
		}
		switch msg := msg.(type) {
		case *VoteSetBitsMessage:
			cs := conR.conS
			votes := cs.GetVotesAtHeight(msg.Height)

			var ourVotes *cmn.BitArray
			switch msg.Type {
			case types.VoteTypePrevote:
				ourVotes = votes.Prevotes(msg.Round).BitArrayByBlockID(msg.BlockID)
			case types.VoteTypePrecommit:
				ourVotes = votes.Precommits(msg.Round).BitArrayByBlockID(msg.BlockID)
			default:
				conR.Logger.Error("Bad VoteSetBitsMessage field Type")
				return
			}
			ps.ApplyVoteSetBitsMessage(msg, ourVotes)
		default:
			// don't punish (leave room for soft upgrades)
			conR.Logger.Error(cmn.Fmt("Unknown message type %v", reflect.TypeOf(msg)))
		}

	default:
		conR.Logger.Error(cmn.Fmt("Unknown chId %X", chID))
	}

	if err != nil {
		conR.Logger.Error("Error in Receive()", "err", err)
	}
}

// SetEventBus sets event bus.
func (conR *ConsensusReactor) SetEventBus(b *types.EventBus) {
	conR.eventBus = b
	conR.conS.SetEventBus(b)
}

// FastSync returns whether the consensus reactor is in fast-sync mode.
func (conR *ConsensusReactor) FastSync() bool {
	conR.mtx.RLock()
	defer conR.mtx.RUnlock()
	return conR.fastSync
}

//--------------------------------------

// startBroadcastRoutine subscribes for new round steps, votes and proposal
// heartbeats using the event bus and starts a go routine to broadcasts events
// to peers upon receiving them.
func (conR *ConsensusReactor) startBroadcastRoutine() error {
	const subscriber = "consensus-reactor"
	ctx := context.Background()

	// new round steps
	stepsCh := make(chan interface{})
	err := conR.eventBus.Subscribe(ctx, subscriber, types.EventQueryNewRoundStep, stepsCh)
	if err != nil {
		return errors.Wrapf(err, "failed to subscribe %s to %s", subscriber, types.EventQueryNewRoundStep)
	}

	// votes
	votesCh := make(chan interface{})
	err = conR.eventBus.Subscribe(ctx, subscriber, types.EventQueryVote, votesCh)
	if err != nil {
		return errors.Wrapf(err, "failed to subscribe %s to %s", subscriber, types.EventQueryVote)
	}

	// proposal heartbeats
	heartbeatsCh := make(chan interface{})
	err = conR.eventBus.Subscribe(ctx, subscriber, types.EventQueryProposalHeartbeat, heartbeatsCh)
	if err != nil {
		return errors.Wrapf(err, "failed to subscribe %s to %s", subscriber, types.EventQueryProposalHeartbeat)
	}

	go func() {
		for {
			select {
			case data, ok := <-stepsCh:
				if ok { // a receive from a closed channel returns the zero value immediately
					edrs := data.(types.TMEventData).Unwrap().(types.EventDataRoundState)
					conR.broadcastNewRoundStep(edrs.RoundState.(*cstypes.RoundState))
				}
			case data, ok := <-votesCh:
				if ok {
					edv := data.(types.TMEventData).Unwrap().(types.EventDataVote)
					conR.broadcastHasVoteMessage(edv.Vote)
				}
			case data, ok := <-heartbeatsCh:
				if ok {
					edph := data.(types.TMEventData).Unwrap().(types.EventDataProposalHeartbeat)
					conR.broadcastProposalHeartbeatMessage(edph)
				}
			case <-conR.Quit:
				conR.eventBus.UnsubscribeAll(ctx, subscriber)
				return
			}
		}
	}()

	return nil
}

func (conR *ConsensusReactor) broadcastProposalHeartbeatMessage(heartbeat types.EventDataProposalHeartbeat) {
	hb := heartbeat.Heartbeat
	conR.Logger.Debug("Broadcasting proposal heartbeat message",
		"height", hb.Height, "round", hb.Round, "sequence", hb.Sequence)
	msg := &ProposalHeartbeatMessage{hb}
	conR.Switch.Broadcast(StateChannel, struct{ ConsensusMessage }{msg})
}

func (conR *ConsensusReactor) broadcastNewRoundStep(rs *cstypes.RoundState) {
	nrsMsg, csMsg := makeRoundStepMessages(rs)
	if nrsMsg != nil {
		conR.Switch.Broadcast(StateChannel, struct{ ConsensusMessage }{nrsMsg})
	}
	if csMsg != nil {
		conR.Switch.Broadcast(StateChannel, struct{ ConsensusMessage }{csMsg})
	}
}

// Broadcasts HasVoteMessage to peers that care.
func (conR *ConsensusReactor) broadcastHasVoteMessage(vote *types.Vote) {
	msg := &HasVoteMessage{
		Height: vote.Height,
		Round:  vote.Round,
		Type:   vote.Type,
		Index:  vote.ValidatorIndex,
	}
	conR.Switch.Broadcast(StateChannel, struct{ ConsensusMessage }{msg})
	/*
		// TODO: Make this broadcast more selective.
		for _, peer := range conR.Switch.Peers().List() {
			ps := peer.Get(PeerStateKey).(*PeerState)
			prs := ps.GetRoundState()
			if prs.Height == vote.Height {
				// TODO: Also filter on round?
				peer.TrySend(StateChannel, struct{ ConsensusMessage }{msg})
			} else {
				// Height doesn't match
				// TODO: check a field, maybe CatchupCommitRound?
				// TODO: But that requires changing the struct field comment.
			}
		}
	*/
}

func makeRoundStepMessages(rs *cstypes.RoundState) (nrsMsg *NewRoundStepMessage, csMsg *CommitStepMessage) {
	nrsMsg = &NewRoundStepMessage{
		Height: rs.Height,
		Round:  rs.Round,
		Step:   rs.Step,
		SecondsSinceStartTime: int(time.Since(rs.StartTime).Seconds()),
		LastCommitRound:       rs.LastCommit.Round(),
	}
	if rs.Step == cstypes.RoundStepCommit {
		if compactBlock {
			csMsg = &CommitStepMessage{
				Height:           rs.Height,
				BlockPartsHeader: rs.ProposalCMPCTBlockParts.Header(),
				BlockParts:       rs.ProposalCMPCTBlockParts.BitArray(),
			}
		} else {
			csMsg = &CommitStepMessage{
				Height:           rs.Height,
				BlockPartsHeader: rs.ProposalBlockParts.Header(),
				BlockParts:       rs.ProposalBlockParts.BitArray(),
			}
		}
	}
	return
}

func (conR *ConsensusReactor) sendNewRoundStepMessages(peer p2p.Peer) {
	rs := conR.conS.GetRoundState()
	nrsMsg, csMsg := makeRoundStepMessages(rs)
	if nrsMsg != nil {
		peer.Send(StateChannel, struct{ ConsensusMessage }{nrsMsg})
	}
	if csMsg != nil {
		peer.Send(StateChannel, struct{ ConsensusMessage }{csMsg})
	}
}

// Send Proposal && ProposalPOL BitArray?
func (conR *ConsensusReactor) pushProposal(rs *cstypes.RoundState, prs *cstypes.PeerRoundState,
	peer p2p.Peer, ps *PeerState, logger log.Logger) bool {
	if rs == nil {
		return false
	}

	if (rs.Round == prs.Round) && (rs.Proposal != nil && !prs.Proposal) {
		// Proposal: share the proposal metadata with peer.
		{
			msg := &ProposalMessage{Proposal: rs.Proposal}
			logger.Debug("Sending proposal", "peer", peer.Key(), "height", rs.Height, "round", rs.Round)
			if peer.Send(DataChannel, struct{ ConsensusMessage }{msg}) {
				ps.SetHasProposal(rs.Proposal)
			}
		}
		// ProposalPOL: lets peer know which POL votes we have so far.
		// Peer must receive ProposalMessage first.
		// rs.Proposal was validated, so rs.Proposal.POLRound <= rs.Round,
		// so we definitely have rs.Votes.Prevotes(rs.Proposal.POLRound).
		if 0 <= rs.Proposal.POLRound {
			msg := &ProposalPOLMessage{
				Height:           rs.Height,
				ProposalPOLRound: rs.Proposal.POLRound,
				ProposalPOL:      rs.Votes.Prevotes(rs.Proposal.POLRound).BitArray(),
			}
			logger.Debug("Sending POL", "peer", peer.Key(), "height", rs.Height, "round", rs.Round)
			peer.Send(DataChannel, struct{ ConsensusMessage }{msg})
		}
		return true
	}

	return false
}

func (conR *ConsensusReactor) pushProposalBlockParts(rs *cstypes.RoundState, prs *cstypes.PeerRoundState,
	peer p2p.Peer, ps *PeerState, logger log.Logger) bool {
	heightLogger := logger.With("height", prs.Height)
	if rs != nil {
		if prs.ProposalBlockParts == nil && rs.Proposal != nil {
			ps.InitProposalBlockParts(prs.Height, rs.Proposal.BlockPartsHeader)
			// continue the loop since prs is a copy and not effected by this initialization
			return true
		}

		conR.gossipCacheDataForCatchup(rs, prs, peer, ps, heightLogger)
	} else {
		if prs.ProposalBlockParts == nil {
			// if we never received the commit message from the peer, the block parts wont be initialized
			blockMeta := conR.conS.blockStore.LoadBlockMeta(prs.Height)
			if blockMeta == nil {
				cmn.PanicCrisis(cmn.Fmt("Failed to load block %d when blockStore is at %d",
					prs.Height, conR.conS.blockStore.Height()))
			}
			ps.InitProposalBlockParts(prs.Height, blockMeta.BlockID.PartsHeader)
			// continue the loop since prs is a copy and not effected by this initialization
			return true
		}

		conR.gossipDataForCatchup(heightLogger, rs, prs, ps, peer)
	}

	return false
}

func (conR *ConsensusReactor) calcGossipRange(rsHeight, prsHeight int64) (start, end int64) {
	// if late 4 of prs height, dont gossip
	if prsHeight >= rsHeight + 4 {
		start = -1
		end = -1
		return
	}
	if prsHeight <= rsHeight {
		start = prsHeight
		end = prsHeight + 4
	} else {
		start = rsHeight
		end = rsHeight + 4
	}
	return
}

func (conR *ConsensusReactor) gossipDataRoutine(peer p2p.Peer, ps *PeerState) {
	logger := conR.Logger.With("peer", peer)

OUTER_LOOP:
	for {
		// Manage disconnects from self or peer.
		if !peer.IsRunning() || !conR.IsRunning() {
			logger.Info("Stopping gossipDataRoutine for peer")
			return
		}

		rs := conR.conS.GetRoundState()
		prs := ps.GetRoundState()

		// logger.Debug("gossipDataRoutine", "rsHeight", rs.Height, "rsRound", rs.Round,
		// 	"prsHeight", prs.Height, "prsRound", prs.Round, "prsStep", prs.Step)

        // calculate gossip range
		start, end := conR.calcGossipRange(rs.Height, prs.Height)
		if start == -1 && end == -1 {
			// Nothing to do. Sleep.
			time.Sleep(conR.conS.config.PeerGossipSleep())
			continue OUTER_LOOP
		}

        // gossip missing proposal/block parts
        for i := start; i < end; i++ {
            // load from cache
            selfRs := conR.conS.TryGetRoundStateAtHeight(i)
            if selfRs == nil && i >= rs.Height {
                continue
            }

            peerRs := ps.GetRoundStateAtHeight(i)

            if conR.pushProposal(selfRs, peerRs, peer, ps, logger) {
                continue OUTER_LOOP
            }

            if conR.pushProposalBlockParts(selfRs, peerRs, peer, ps, logger) {
                continue OUTER_LOOP
            }
        }

		// Nothing to do. Sleep.
		time.Sleep(conR.conS.config.PeerGossipSleep())
		continue OUTER_LOOP
	}
}

func (conR *ConsensusReactor) gossipCacheDataForCatchup(rs *cstypes.RoundState, prs *cstypes.PeerRoundState,
	peer p2p.Peer, ps *PeerState, logger log.Logger) {
	if !compactBlock {
		if rs.ProposalBlockParts.HasHeader(prs.ProposalBlockPartsHeader) {
			if index, ok := prs.ProposalBlockParts.Sub(prs.ProposalBlockParts.Copy()).PickRandom(); ok {
				// Send the part
				msg := &BlockPartMessage{
					Height: prs.Height,
					Round:  prs.Round,
					Part:   rs.ProposalBlockParts.GetPart(index),
				}
				logger.Debug("Sending pipeline block part for catchup", "peer", peer.Key(), "height", prs.Height, "round", prs.Round, "index", index)
				if peer.Send(DataChannel, struct{ ConsensusMessage }{msg}) {
					ps.SetHasProposalBlockPart(prs.Height, prs.Round, index)
				} else {
					logger.Debug("Sending pipeline block part for catchup failed")
				}
			}
		}
	} else {
		if rs.ProposalCMPCTBlockParts.HasHeader(prs.ProposalBlockPartsHeader) {
			if index, ok := rs.ProposalCMPCTBlockParts.BitArray().Sub(prs.ProposalCMPCTBlockParts.Copy()).PickRandom(); ok {
				part := rs.ProposalCMPCTBlockParts.GetPart(index)
				// Send the part
				msg := &CMPCTBlockPartMessage{
					Height: prs.Height,
					Round:  prs.Round,
					Part:   part,
				}
				logger.Debug("Sending pipeline cmpct block part for catchup", "peer", peer.Key(), "height", prs.Height, "round", prs.Round, "index", index)
				if peer.Send(DataChannel, struct{ ConsensusMessage }{msg}) {
					ps.SetHasProposalCMPCTBlockPart(prs.Height, prs.Round, index)
				} else {
					logger.Debug("Sending pipeline cmpct block part for catchup failed")
				}
			}
		}
	}
}

func (conR *ConsensusReactor) gossipDataForCatchup(logger log.Logger, rs *cstypes.RoundState,
	prs *cstypes.PeerRoundState, ps *PeerState, peer p2p.Peer) {
	if index, ok := prs.ProposalBlockParts.Not().PickRandom(); ok {
		// Ensure that the peer's PartSetHeader is correct
		blockMeta := conR.conS.blockStore.LoadBlockMeta(prs.Height)
		if blockMeta == nil {
			logger.Error("Failed to load block meta",
				"ourHeight", prs.Height, "blockstoreHeight", conR.conS.blockStore.Height())
			time.Sleep(conR.conS.config.PeerGossipSleep())
			return
		} else if !blockMeta.BlockID.PartsHeader.Equals(prs.ProposalBlockPartsHeader) {
			logger.Info("Peer ProposalBlockPartsHeader mismatch, sleeping",
				"blockPartsHeader", blockMeta.BlockID.PartsHeader, "peerBlockPartsHeader", prs.ProposalBlockPartsHeader)
			time.Sleep(conR.conS.config.PeerGossipSleep())
			return
		}
		// Load the part
		part := conR.conS.blockStore.LoadBlockPart(prs.Height, index)
		if part == nil {
			logger.Error("Could not load part", "index", index,
				"blockPartsHeader", blockMeta.BlockID.PartsHeader, "peerBlockPartsHeader", prs.ProposalBlockPartsHeader)
			time.Sleep(conR.conS.config.PeerGossipSleep())
			return
		}
		// Send the part
		msg := &BlockPartMessage{
			Height: prs.Height, // Not our height, so it doesn't matter.
			Round:  prs.Round,  // Not our height, so it doesn't matter.
			Part:   part,
		}
		logger.Debug("Sending block part for catchup", "peer", peer.Key(), "height", prs.Height, "round", prs.Round, "index", index)
		if peer.Send(DataChannel, struct{ ConsensusMessage }{msg}) {
			ps.SetHasProposalBlockPart(prs.Height, prs.Round, index)
		} else {
			logger.Debug("Sending block part for catchup failed")
		}
		return
	} else {
		//logger.Info("No parts to send in catch-up, sleeping")
		time.Sleep(conR.conS.config.PeerGossipSleep())
		return
	}
}

func (conR *ConsensusReactor) gossipVotesRoutine(peer p2p.Peer, ps *PeerState) {
	logger := conR.Logger.With("peer", peer)

	// Simple hack to throttle logs upon sleep.
	var sleeping = 0

OUTER_LOOP:
	for {
		// Manage disconnects from self or peer.
		if !peer.IsRunning() || !conR.IsRunning() {
			logger.Info("Stopping gossipVotesRoutine for peer")
			return
		}

		switch sleeping {
		case 1: // First sleep
			sleeping = 2
		case 2: // No more sleep
			sleeping = 0
		}

		// logger.Debug("gossipVotesRoutine", "rsHeight", rs.Height, "rsRound", rs.Round,
		// 	"prsHeight", prs.Height, "prsRound", prs.Round, "prsStep", prs.Step)

		rs := conR.conS.GetRoundState()
		prs := ps.GetRoundState()

		// calculate gossip range
		start, end := conR.calcGossipRange(rs.Height, prs.Height)
		if start == -1 && end == -1 {
			// Nothing to do. Sleep.
			time.Sleep(conR.conS.config.PeerGossipSleep())
			continue OUTER_LOOP
		}

		// gossip missing votes
		for i := start; i < end; i++ {
			selfRs := conR.conS.TryGetRoundStateAtHeight(i)
			peerRs := ps.GetRoundStateAtHeight(i)
			heightLogger := logger.With("height", rs.Height)

			// only gossip votes the height dont lower than self,
			// if height lower than self, gossip lastCommit to let peer catch up
			if selfRs != nil && i >= rs.Height  {
				// votes are in cache
				if conR.gossipVotesForHeight(heightLogger, selfRs, peerRs, ps) {
					continue OUTER_LOOP
				}
			} else {
				if i >= rs.Height {
					continue
				}
				Lastcommit := (*types.Commit)(nil)
				if selfRs != nil && selfRs.Votes != nil {
					precommits := selfRs.Votes.Precommits(selfRs.CommitRound)
					if precommits != nil && precommits.IsCommit() {
						Lastcommit = precommits.MakeCommit()
					} else {
						// votes are not in cache, load from commit
						if i <= conR.conS.blockStore.Height() {
							Lastcommit = conR.conS.LoadCommit(i)
						}
					}
				} else {
					// votes are not in cache, load from commit
					if i <= conR.conS.blockStore.Height() {
						Lastcommit = conR.conS.LoadCommit(i)
					}
				}
				if Lastcommit != nil && ps.PickSendVote(Lastcommit) {
					logger.Debug("Picked Catchup last commit to send", "height", i)
					continue OUTER_LOOP
				}
			}
		}

		if sleeping == 0 {
			// We sent nothing. Sleep...
			sleeping = 1
			logger.Debug("No votes to send, sleeping", "rs.Height", rs.Height, "prs.Height", prs.Height,
				"localPV", rs.Votes.Prevotes(rs.Round).BitArray(), "peerPV", prs.Prevotes,
				"localPC", rs.Votes.Precommits(rs.Round).BitArray(), "peerPC", prs.Precommits)
		} else if sleeping == 2 {
			// Continued sleep...
			sleeping = 1
		}

		time.Sleep(conR.conS.config.PeerGossipSleep())
		continue OUTER_LOOP
	}
}

func (conR *ConsensusReactor) gossipVotesForHeight(logger log.Logger, rs *cstypes.RoundState, prs *cstypes.PeerRoundState, ps *PeerState) bool {
	// If there are lastCommits to send...
	if prs.Step == cstypes.RoundStepNewHeight {
		if ps.PickSendVote(rs.Votes.Precommits(rs.CommitRound)) {
			logger.Debug("Picked rs.LastCommit to send")
			return true
		}
	}
	// If there are prevotes to send...
	if prs.Step <= cstypes.RoundStepWaitToPrecommit && prs.Round != -1 && prs.Round <= rs.Round {
		if ps.PickSendVote(rs.Votes.Prevotes(prs.Round)) {
			logger.Debug("Picked rs.Prevotes(prs.Round) to send", "round", prs.Round)
			return true
		}
	}
	// If there are precommits to send...
	if prs.Step <= cstypes.RoundStepWaitToCommit && prs.Round != -1 && prs.Round <= rs.Round {
		if ps.PickSendVote(rs.Votes.Precommits(prs.Round)) {
			logger.Debug("Picked rs.Precommits(prs.Round) to send", "round", prs.Round)
			return true
		}
	}
	// If there are POLPrevotes to send...
	if prs.ProposalPOLRound != -1 {
		if polPrevotes := rs.Votes.Prevotes(prs.ProposalPOLRound); polPrevotes != nil {
			if ps.PickSendVote(polPrevotes) {
				logger.Debug("Picked rs.Prevotes(prs.ProposalPOLRound) to send",
					"round", prs.ProposalPOLRound)
				return true
			}
		}
	}
	return false
}

// NOTE: `queryMaj23Routine` has a simple crude design since it only comes
// into play for liveness when there's a signature DDoS attack happening.
func (conR *ConsensusReactor) queryMaj23Routine(peer p2p.Peer, ps *PeerState) {
	logger := conR.Logger.With("peer", peer)

OUTER_LOOP:
	for {
		// Manage disconnects from self or peer.
		if !peer.IsRunning() || !conR.IsRunning() {
			logger.Info("Stopping queryMaj23Routine for peer")
			return
		}

		// Maybe send Height/Round/Prevotes
		{
			rs := conR.conS.GetRoundState()
			prs := ps.GetRoundState()
			if rs.Height == prs.Height {
				maj23, ok := rs.Votes.Prevotes(prs.Round).TwoThirdsMajority()
				if ok {
					peer.TrySend(StateChannel, struct{ ConsensusMessage }{&VoteSetMaj23Message{
						Height:  prs.Height,
						Round:   prs.Round,
						Type:    types.VoteTypePrevote,
						BlockID: maj23,
					}})
					time.Sleep(conR.conS.config.PeerQueryMaj23Sleep())
				}
			}
		}

		// Maybe send Height/Round/Precommits
		{
			rs := conR.conS.GetRoundState()
			prs := ps.GetRoundState()
			if rs.Height == prs.Height {
				maj23, ok := rs.Votes.Precommits(prs.Round).TwoThirdsMajority()
				if ok {
					peer.TrySend(StateChannel, struct{ ConsensusMessage }{&VoteSetMaj23Message{
						Height:  prs.Height,
						Round:   prs.Round,
						Type:    types.VoteTypePrecommit,
						BlockID: maj23,
					}})
					time.Sleep(conR.conS.config.PeerQueryMaj23Sleep())
				}
			}
		}

		// Maybe send Height/Round/ProposalPOL
		{
			rs := conR.conS.GetRoundState()
			prs := ps.GetRoundState()
			if rs.Height == prs.Height && prs.ProposalPOLRound >= 0 {
				maj23, ok := rs.Votes.Prevotes(prs.ProposalPOLRound).TwoThirdsMajority()
				if ok {
					peer.TrySend(StateChannel, struct{ ConsensusMessage }{&VoteSetMaj23Message{
						Height:  prs.Height,
						Round:   prs.ProposalPOLRound,
						Type:    types.VoteTypePrevote,
						BlockID: maj23,
					}})
					time.Sleep(conR.conS.config.PeerQueryMaj23Sleep())
				}
			}
		}

		// Little point sending LastCommitRound/LastCommit,
		// These are fleeting and non-blocking.

		// Maybe send Height/CatchupCommitRound/CatchupCommit.
		{
			prs := ps.GetRoundState()
			if prs.CatchupCommitRound != -1 && 4 < prs.Height && prs.Height <= conR.conS.blockStore.Height() {
				commit := conR.conS.LoadCommit(prs.Height)
				peer.TrySend(StateChannel, struct{ ConsensusMessage }{&VoteSetMaj23Message{
					Height:  prs.Height,
					Round:   commit.Round(),
					Type:    types.VoteTypePrecommit,
					BlockID: commit.BlockID,
				}})
				time.Sleep(conR.conS.config.PeerQueryMaj23Sleep())
			}
		}

		time.Sleep(conR.conS.config.PeerQueryMaj23Sleep())

		continue OUTER_LOOP
	}
}

// String returns a string representation of the ConsensusReactor.
// NOTE: For now, it is just a hard-coded string to avoid accessing unprotected shared variables.
// TODO: improve!
func (conR *ConsensusReactor) String() string {
	// better not to access shared variables
	return "ConsensusReactor" // conR.StringIndented("")
}

// StringIndented returns an indented string representation of the ConsensusReactor
func (conR *ConsensusReactor) StringIndented(indent string) string {
	s := "ConsensusReactor{\n"
	s += indent + "  " + conR.conS.StringIndented(indent+"  ") + "\n"
	for _, peer := range conR.Switch.Peers().List() {
		ps := peer.Get(types.PeerStateKey).(*PeerState)
		s += indent + "  " + ps.StringIndented(indent+"  ") + "\n"
	}
	s += indent + "}"
	return s
}

//-----------------------------------------------------------------------------

var (
	ErrPeerStateHeightRegression = errors.New("Error peer state height regression")
	ErrPeerStateInvalidStartTime = errors.New("Error peer state invalid startTime")
)

// PeerState contains the known state of a peer, including its connection
// and threadsafe access to its PeerRoundState.
type PeerState struct {
	Peer   p2p.Peer
	logger log.Logger

	mtx sync.Mutex
	// peer's round states on each pipeline
	roundStates map[int64]*cstypes.PeerRoundState
	// peer's last_commit_height + 1
	latestHeight int64
}

// NewPeerState returns a new PeerState for the given Peer
func NewPeerState(peer p2p.Peer) *PeerState {
	return &PeerState{
		Peer:   peer,
		logger: log.NewNopLogger(),
		roundStates: make(map[int64]*cstypes.PeerRoundState),
		latestHeight: 1,
	}
}

func (ps *PeerState) SetLogger(logger log.Logger) *PeerState {
	ps.logger = logger
	return ps
}

// GetRoundState returns an atomic snapshot of the PeerRoundState.
// There's no point in mutating it since it won't change PeerState.
func (ps *PeerState) GetRoundState() *cstypes.PeerRoundState {
	return ps.GetRoundStateAtHeight(ps.latestHeight)
}

func (ps *PeerState) TryGetRoundStateAtHeight(height int64) *cstypes.PeerRoundState {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	if rs, ok := ps.roundStates[height]; ok {
		 rsCopy := *rs // Copy
		 return &rsCopy
	}

	return nil
}

func (ps *PeerState) GetRoundStateAtHeight(height int64) *cstypes.PeerRoundState {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	prsCopy := *ps.getRoundStateAtHeight(height) // Copy
	return &prsCopy
}

func (ps *PeerState) truncateRoundStates(height int64) {
	maxLen := int64(8)

	for h, prs := range ps.roundStates {
		if height - prs.Height > maxLen {
			delete(ps.roundStates, h)
		}
	}
}

func (ps *PeerState) getRoundStateAtHeight(height int64) *cstypes.PeerRoundState {
	if _, ok := ps.roundStates[height]; !ok {
		ps.roundStates[height] = &cstypes.PeerRoundState{
			Height:             height,
			Round:              0,
			ProposalPOLRound:   -1,
			LastCommitRound:    -1,
			CatchupCommitRound: -1,
		}
	}

	ps.truncateRoundStates(height)

	return ps.roundStates[height]
}

// GetHeight returns an atomic snapshot of the PeerRoundState's height
// used by the mempool to ensure peers are caught up before broadcasting new txs
func (ps *PeerState) GetHeight() int64 {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	return ps.latestHeight
}

// SetHasProposal sets the given proposal as known for the peer.
func (ps *PeerState) SetHasProposal(proposal *types.Proposal) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	// if ps.Height != proposal.Height || ps.Round != proposal.Round {
	// 	return
	// }
	prs := ps.getRoundStateAtHeight(proposal.Height)
	if prs.Proposal {
		return
	}

	prs.Proposal = true
	prs.ProposalBlockPartsHeader = proposal.BlockPartsHeader
	prs.ProposalBlockParts = cmn.NewBitArray(proposal.BlockPartsHeader.Total)
	prs.ProposalCMPCTBlockParts = cmn.NewBitArray(proposal.BlockPartsHeader.Total)
	prs.ProposalPOLRound = proposal.POLRound
	prs.ProposalPOL = nil // Nil until ProposalPOLMessage received.
}

// InitProposalBlockParts initializes the peer's proposal block parts header and bit array.
func (ps *PeerState) InitProposalBlockParts(height int64, partsHeader types.PartSetHeader) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	prs := ps.getRoundStateAtHeight(height)
	if prs.ProposalBlockParts != nil {
		return
	}

	prs.ProposalBlockPartsHeader = partsHeader
	prs.ProposalBlockParts = cmn.NewBitArray(partsHeader.Total)
	prs.ProposalCMPCTBlockParts = cmn.NewBitArray(partsHeader.Total)
}

// SetHasProposalBlockPart sets the given block part index as known for the peer.
func (ps *PeerState) SetHasProposalBlockPart(height int64, round int, index int) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	prs := ps.getRoundStateAtHeight(height)
	if prs.Height != height || prs.Round != round {
		return
	}

	prs.ProposalBlockParts.SetIndex(index, true)
}

// SetHasProposalCMPCTBlockPart sets the given block part index as known for the peer.
func (ps *PeerState) SetHasProposalCMPCTBlockPart(height int64, round int, index int) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	prs := ps.getRoundStateAtHeight(height)
	if prs.Height != height || prs.Round != round {
		return
	}

	prs.ProposalCMPCTBlockParts.SetIndex(index, true)
}

// PickSendVote picks a vote and sends it to the peer.
// Returns true if vote was sent.
func (ps *PeerState) PickSendVote(votes types.VoteSetReader) bool {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if vote, ok := ps.PickVoteToSend(votes); ok {
		msg := &VoteMessage{vote}
		prs := ps.getRoundStateAtHeight(votes.Height())
		if prs.CatchupCommitRound >= 0 {
			ps.logger.Debug("Sending vote message for catchup", "ps", ps, "vote", vote, "catchup_round", prs.CatchupCommitRound)
		} else {
			ps.logger.Debug("Sending vote message", "ps", ps, "vote", vote)
		}
		return ps.Peer.Send(VoteChannel, struct{ ConsensusMessage }{msg})
	}
	return false
}

// PickVoteToSend picks a vote to send to the peer.
// Returns true if a vote was picked.
// NOTE: `votes` must be the correct Size() for the Height().
func (ps *PeerState) PickVoteToSend(votes types.VoteSetReader) (vote *types.Vote, ok bool) {
	if votes.Size() == 0 {
		return nil, false
	}

	height, round, type_, size := votes.Height(), votes.Round(), votes.Type(), votes.Size()

	// Lazily set data using 'votes'.
	if votes.IsCommit() {
		ps.ensureCatchupCommitRound(height, round, size)
	}
	ps.ensureVoteBitArrays(height, size)

	psVotes := ps.getVoteBitArray(height, round, type_)
	if psVotes == nil {
		return nil, false // Not something worth sending
	}
	if index, ok := votes.BitArray().Sub(psVotes).PickRandom(); ok {
		ps.setHasVote(height, round, type_, index)
		return votes.GetByIndex(index), true
	}
	return nil, false
}

func (ps *PeerState) getVoteBitArray(height int64, round int, type_ byte) *cmn.BitArray {
	if !types.IsVoteTypeValid(type_) {
		return nil
	}

	prs := ps.getRoundStateAtHeight(height)
	if prs.Round == round {
		switch type_ {
		case types.VoteTypePrevote:
			return prs.Prevotes
		case types.VoteTypePrecommit:
			return prs.Precommits
		}
	}
	if prs.CatchupCommitRound == round {
		switch type_ {
		case types.VoteTypePrevote:
			return nil
		case types.VoteTypePrecommit:
			return prs.CatchupCommit
		}
	}

	// if ps.Height == height {
	// 	if ps.Round == round {
	// 		switch type_ {
	// 		case types.VoteTypePrevote:
	// 			return ps.Prevotes
	// 		case types.VoteTypePrecommit:
	// 			return ps.Precommits
	// 		}
	// 	}
	// 	if ps.CatchupCommitRound == round {
	// 		switch type_ {
	// 		case types.VoteTypePrevote:
	// 			return nil
	// 		case types.VoteTypePrecommit:
	// 			return ps.CatchupCommit
	// 		}
	// 	}
	// 	if ps.ProposalPOLRound == round {
	// 		switch type_ {
	// 		case types.VoteTypePrevote:
	// 			return ps.ProposalPOL
	// 		case types.VoteTypePrecommit:
	// 			return nil
	// 		}
	// 	}
	// 	return nil
	// }
	// if ps.Height == height+1 {
	// 	if ps.LastCommitRound == round {
	// 		switch type_ {
	// 		case types.VoteTypePrevote:
	// 			return nil
	// 		case types.VoteTypePrecommit:
	// 			return ps.LastCommit
	// 		}
	// 	}
	// 	return nil
	// }
	return nil
}

// 'round': A round for which we have a +2/3 commit.
func (ps *PeerState) ensureCatchupCommitRound(height int64, round int, numValidators int) {
	// if ps.Height != height {
	// 	return
	// }
	/*
		NOTE: This is wrong, 'round' could change.
		e.g. if orig round is not the same as block LastCommit round.
		if ps.CatchupCommitRound != -1 && ps.CatchupCommitRound != round {
			cmn.PanicSanity(cmn.Fmt("Conflicting CatchupCommitRound. Height: %v, Orig: %v, New: %v", height, ps.CatchupCommitRound, round))
		}
	*/

	prs := ps.getRoundStateAtHeight(height)
	if prs.CatchupCommitRound == round {
		return // Nothing to do!
	}
	prs.CatchupCommitRound = round
	if round == prs.Round {
		prs.CatchupCommit = prs.Precommits
	} else {
		prs.CatchupCommit = cmn.NewBitArray(numValidators)
	}
}

// EnsureVoteVitArrays ensures the bit-arrays have been allocated for tracking
// what votes this peer has received.
// NOTE: It's important to make sure that numValidators actually matches
// what the node sees as the number of validators for height.
func (ps *PeerState) EnsureVoteBitArrays(height int64, numValidators int) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	ps.ensureVoteBitArrays(height, numValidators)
}

func (ps *PeerState) ensureVoteBitArrays(height int64, numValidators int) {
	prs := ps.getRoundStateAtHeight(height)
	if prs.Prevotes == nil {
		prs.Prevotes = cmn.NewBitArray(numValidators)
	}
	if prs.Precommits == nil {
		prs.Precommits = cmn.NewBitArray(numValidators)
	}

	// if ps.Height == height {
	// 	if ps.Prevotes == nil {
	// 		ps.Prevotes = cmn.NewBitArray(numValidators)
	// 	}
	// 	if ps.Precommits == nil {
	// 		ps.Precommits = cmn.NewBitArray(numValidators)
	// 	}
	// 	if ps.CatchupCommit == nil {
	// 		ps.CatchupCommit = cmn.NewBitArray(numValidators)
	// 	}
	// 	if ps.ProposalPOL == nil {
	// 		ps.ProposalPOL = cmn.NewBitArray(numValidators)
	// 	}
	// } else if ps.Height == height+1 {
	// 	if ps.LastCommit == nil {
	// 		ps.LastCommit = cmn.NewBitArray(numValidators)
	// 	}
	// }
}

// SetHasVote sets the given vote as known by the peer
func (ps *PeerState) SetHasVote(vote *types.Vote) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	ps.setHasVote(vote.Height, vote.Round, vote.Type, vote.ValidatorIndex)
}

func (ps *PeerState) setHasVote(height int64, round int, type_ byte, index int) {
	prs := ps.getRoundStateAtHeight(height)
	logger := ps.logger.With("peerH/R", cmn.Fmt("%d/%d", prs.Height, prs.Round), "H/R", cmn.Fmt("%d/%d", height, round))
	logger.Debug("setHasVote", "type", type_, "index", index)

	// NOTE: some may be nil BitArrays -> no side effects.
	psVotes := ps.getVoteBitArray(height, round, type_)
	if psVotes != nil {
		psVotes.SetIndex(index, true)
	}
}

// ApplyNewRoundStepMessage updates the peer state for the new round.
func (ps *PeerState) ApplyNewRoundStepMessage(msg *NewRoundStepMessage) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	// Ignore duplicates or decreases
	// if CompareHRS(msg.Height, msg.Round, msg.Step, ps.Height, ps.Round, ps.Step) <= 0 {
	// 	return
	// }

	prs := ps.getRoundStateAtHeight(msg.Height)

	// Just remember these values.
	// psHeight := prs.Height
	// psRound := prs.Round
	// psCatchupCommitRound := prs.CatchupCommitRound
	// psCatchupCommit := prs.CatchupCommit

	startTime := time.Now().Add(-1 * time.Duration(msg.SecondsSinceStartTime) * time.Second)
	prs.Step = msg.Step
	prs.StartTime = startTime
	if prs.Round != msg.Round {
		prs.Proposal = false
		prs.ProposalBlockPartsHeader = types.PartSetHeader{}
		prs.ProposalBlockParts = nil
		prs.ProposalCMPCTBlockParts = nil
		prs.ProposalPOLRound = -1
		prs.ProposalPOL = nil
		// We'll update the BitArray capacity later.
		prs.Prevotes = nil
		prs.Precommits = nil
		prs.Round = msg.Round

		// If rollback happened, remove afterward peer states
		for i := msg.Height+1; i < msg.Height+4; i++ {
			delete(ps.roundStates, int64(i))
		}
	}

	// update latest height
	lh := msg.Height - 3
	if lh > ps.latestHeight {
		ps.latestHeight = lh
		ps.logger.Debug("ApplyNewRoundStepMessage", "lastestheight", ps.latestHeight, "prs_height", msg.Height)
	}

	// if psHeight == msg.Height && psRound != msg.Round && msg.Round == psCatchupCommitRound {
	// 	// Peer caught up to CatchupCommitRound.
	// 	// Preserve psCatchupCommit!
	// 	// NOTE: We prefer to use prs.Precommits if
	// 	// pr.Round matches pr.CatchupCommitRound.
	// 	prs.Precommits = psCatchupCommit
	// }
	// if psHeight != msg.Height {
	// 	// Shift Precommits to LastCommit.
	// 	if psHeight+1 == msg.Height && psRound == msg.LastCommitRound {
	// 		prs.LastCommitRound = msg.LastCommitRound
	// 		prs.LastCommit = prs.Precommits
	// 	} else {
	// 		prs.LastCommitRound = msg.LastCommitRound
	// 		prs.LastCommit = nil
	// 	}
	// 	// We'll update the BitArray capacity later.
	// 	prs.CatchupCommitRound = -1
	// 	prs.CatchupCommit = nil
	// }
}

// ApplyCommitStepMessage updates the peer state for the new commit.
func (ps *PeerState) ApplyCommitStepMessage(msg *CommitStepMessage) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	// if ps.Height != msg.Height {
	// 	return
	// }

	prs := ps.getRoundStateAtHeight(msg.Height)
	prs.ProposalBlockPartsHeader = msg.BlockPartsHeader
	prs.ProposalBlockParts = msg.BlockParts
	prs.ProposalCMPCTBlockParts = msg.BlockParts

	ps.logger.Debug("ApplyCommitStepMessage", "height", msg.Height, "header", prs.ProposalBlockPartsHeader.String())
}

// ApplyProposalPOLMessage updates the peer state for the new proposal POL.
func (ps *PeerState) ApplyProposalPOLMessage(msg *ProposalPOLMessage) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	// if ps.Height != msg.Height {
	// 	return
	// }
	prs := ps.getRoundStateAtHeight(msg.Height)
	if prs.ProposalPOLRound != msg.ProposalPOLRound {
		return
	}

	// TODO: Merge onto existing ps.ProposalPOL?
	// We might have sent some prevotes in the meantime.
	prs.ProposalPOL = msg.ProposalPOL
}

// ApplyHasVoteMessage updates the peer state for the new vote.
func (ps *PeerState) ApplyHasVoteMessage(msg *HasVoteMessage) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	// if ps.Height != msg.Height {
	// 	return
	// }

	ps.setHasVote(msg.Height, msg.Round, msg.Type, msg.Index)
}

// ApplyVoteSetBitsMessage updates the peer state for the bit-array of votes
// it claims to have for the corresponding BlockID.
// `ourVotes` is a BitArray of votes we have for msg.BlockID
// NOTE: if ourVotes is nil (e.g. msg.Height < rs.Height),
// we conservatively overwrite ps's votes w/ msg.Votes.
func (ps *PeerState) ApplyVoteSetBitsMessage(msg *VoteSetBitsMessage, ourVotes *cmn.BitArray) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	votes := ps.getVoteBitArray(msg.Height, msg.Round, msg.Type)
	if votes != nil {
		if ourVotes == nil {
			votes.Update(msg.Votes)
		} else {
			otherVotes := votes.Sub(ourVotes)
			hasVotes := otherVotes.Or(msg.Votes)
			votes.Update(hasVotes)
		}
	}
}

// String returns a string representation of the PeerState
func (ps *PeerState) String() string {
	return ps.StringIndented("")
}

// StringIndented returns a string representation of the PeerState
func (ps *PeerState) StringIndented(indent string) string {
	return fmt.Sprintf(`PeerState{
%s  Key %v
%s  PRS %v
%s}`,
		indent, ps.Peer.Key(),
		indent, ps.getRoundStateAtHeight(ps.latestHeight).StringIndented(indent+"  "),
		indent)
}

//-----------------------------------------------------------------------------
// Messages

const (
	msgTypeNewRoundStep    = byte(0x01)
	msgTypeCommitStep      = byte(0x02)
	msgTypeProposal        = byte(0x11)
	msgTypeProposalPOL     = byte(0x12)
	msgTypeBlockPart       = byte(0x13) // both block & POL
	msgTypeVote            = byte(0x14)
	msgTypeHasVote         = byte(0x15)
	msgTypeVoteSetMaj23    = byte(0x16)
	msgTypeVoteSetBits     = byte(0x17)
	msgTypeCMPCTBlockPart  = byte(0x18)

	msgTypeProposalHeartbeat = byte(0x20)
	msgTypeStateTransition   = byte(0x21)
)

// ConsensusMessage is a message that can be sent and received on the ConsensusReactor
type ConsensusMessage interface{}

var _ = wire.RegisterInterface(
	struct{ ConsensusMessage }{},
	wire.ConcreteType{&NewRoundStepMessage{}, msgTypeNewRoundStep},
	wire.ConcreteType{&CommitStepMessage{}, msgTypeCommitStep},
	wire.ConcreteType{&ProposalMessage{}, msgTypeProposal},
	wire.ConcreteType{&ProposalPOLMessage{}, msgTypeProposalPOL},
	wire.ConcreteType{&BlockPartMessage{}, msgTypeBlockPart},
	wire.ConcreteType{&CMPCTBlockPartMessage{}, msgTypeCMPCTBlockPart},
	wire.ConcreteType{&VoteMessage{}, msgTypeVote},
	wire.ConcreteType{&HasVoteMessage{}, msgTypeHasVote},
	wire.ConcreteType{&VoteSetMaj23Message{}, msgTypeVoteSetMaj23},
	wire.ConcreteType{&VoteSetBitsMessage{}, msgTypeVoteSetBits},
	wire.ConcreteType{&ProposalHeartbeatMessage{}, msgTypeProposalHeartbeat},
	wire.ConcreteType{&StateTransitionMessage{}, msgTypeStateTransition},
)

// DecodeMessage decodes the given bytes into a ConsensusMessage.
// TODO: check for unnecessary extra bytes at the end.
func DecodeMessage(bz []byte) (msgType byte, msg ConsensusMessage, err error) {
	msgType = bz[0]
	n := new(int)
	r := bytes.NewReader(bz)
	msgI := wire.ReadBinary(struct{ ConsensusMessage }{}, r, maxConsensusMessageSize, n, &err)
	msg = msgI.(struct{ ConsensusMessage }).ConsensusMessage
	return
}

//-------------------------------------

// NewRoundStepMessage is sent for every step taken in the ConsensusState.
// For every height/round/step transition
type NewRoundStepMessage struct {
	Height                int64
	Round                 int
	Step                  cstypes.RoundStepType
	SecondsSinceStartTime int
	LastCommitRound       int
}

// String returns a string representation.
func (m *NewRoundStepMessage) String() string {
	return fmt.Sprintf("[NewRoundStep H:%v R:%v S:%v LCR:%v]",
		m.Height, m.Round, m.Step, m.LastCommitRound)
}

//-------------------------------------

// CommitStepMessage is sent when a block is committed.
type CommitStepMessage struct {
	Height           int64
	BlockPartsHeader types.PartSetHeader
	BlockParts       *cmn.BitArray
}

// String returns a string representation.
func (m *CommitStepMessage) String() string {
	return fmt.Sprintf("[CommitStep H:%v BP:%v BA:%v]", m.Height, m.BlockPartsHeader, m.BlockParts)
}

//-------------------------------------

// ProposalMessage is sent when a new block is proposed.
type ProposalMessage struct {
	Proposal *types.Proposal
}

// String returns a string representation.
func (m *ProposalMessage) String() string {
	return fmt.Sprintf("[Proposal %v]", m.Proposal)
}

//-------------------------------------

// ProposalPOLMessage is sent when a previous proposal is re-proposed.
type ProposalPOLMessage struct {
	Height           int64
	ProposalPOLRound int
	ProposalPOL      *cmn.BitArray
}

// String returns a string representation.
func (m *ProposalPOLMessage) String() string {
	return fmt.Sprintf("[ProposalPOL H:%v POLR:%v POL:%v]", m.Height, m.ProposalPOLRound, m.ProposalPOL)
}

//-------------------------------------

// BlockPartMessage is sent when gossipping a piece of the proposed block.
type BlockPartMessage struct {
	Height int64
	Round  int
	Part   *types.Part
}

// CMPCTBlockPartMessage is sent when gossipping a piece of the proposed cmpct block.
// cmpct block contains tx hash but not tx data
type CMPCTBlockPartMessage struct {
	Height int64
	Round  int
	Part   *types.Part
}

// String returns a string representation.
func (m *BlockPartMessage) String() string {
	return fmt.Sprintf("[BlockPart H:%v R:%v P:%v]", m.Height, m.Round, m.Part)
}

//-------------------------------------

// VoteMessage is sent when voting for a proposal (or lack thereof).
type VoteMessage struct {
	Vote *types.Vote
}

// String returns a string representation.
func (m *VoteMessage) String() string {
	return fmt.Sprintf("[Vote %v]", m.Vote)
}

//-------------------------------------

// HasVoteMessage is sent to indicate that a particular vote has been received.
type HasVoteMessage struct {
	Height int64
	Round  int
	Type   byte
	Index  int
}

// String returns a string representation.
func (m *HasVoteMessage) String() string {
	return fmt.Sprintf("[HasVote VI:%v V:{%v/%02d/%v}]", m.Index, m.Height, m.Round, m.Type)
}

//-------------------------------------

// VoteSetMaj23Message is sent to indicate that a given BlockID has seen +2/3 votes.
type VoteSetMaj23Message struct {
	Height  int64
	Round   int
	Type    byte
	BlockID types.BlockID
}

// String returns a string representation.
func (m *VoteSetMaj23Message) String() string {
	return fmt.Sprintf("[VSM23 %v/%02d/%v %v]", m.Height, m.Round, m.Type, m.BlockID)
}

//-------------------------------------

// VoteSetBitsMessage is sent to communicate the bit-array of votes seen for the BlockID.
type VoteSetBitsMessage struct {
	Height  int64
	Round   int
	Type    byte
	BlockID types.BlockID
	Votes   *cmn.BitArray
}

// String returns a string representation.
func (m *VoteSetBitsMessage) String() string {
	return fmt.Sprintf("[VSB %v/%02d/%v %v %v]", m.Height, m.Round, m.Type, m.BlockID, m.Votes)
}

//-------------------------------------

// ProposalHeartbeatMessage is sent to signal that a node is alive and waiting for transactions for a proposal.
type ProposalHeartbeatMessage struct {
	Heartbeat *types.Heartbeat
}

// String returns a string representation.
func (m *ProposalHeartbeatMessage) String() string {
	return fmt.Sprintf("[HEARTBEAT %v]", m.Heartbeat)
}

//-------------------------------------

// StateTransitionMessage is sent to signal pipeline to move forward.
type StateTransitionMessage struct {
	Height int64
	Type   cstypes.RoundStepType
}

// String returns a string representation.
func (m *StateTransitionMessage) String() string {
	return fmt.Sprintf("[StateTransition %v/%v]", m.Height, m.Type)
}
