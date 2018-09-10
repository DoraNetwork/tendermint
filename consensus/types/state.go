package types

import (
	"fmt"
	"time"

	"github.com/tendermint/tendermint/types"
)

//-----------------------------------------------------------------------------
// RoundStepType enum type

// RoundStepType enumerates the state of the consensus state machine
type RoundStepType uint8 // These must be numeric, ordered.

const (
	RoundStepNewHeight     = RoundStepType(0x01) // Wait til CommitTime + timeoutCommit
	RoundStepNewRound      = RoundStepType(0x02) // Setup new round and go to RoundStepPropose
	RoundStepWaitToPropose = RoundStepType(0x03) // Wait previous height to enter prevote
	RoundStepPropose       = RoundStepType(0x04) // Did propose, gossip proposal
	RoundStepWaitToPrevote = RoundStepType(0x05) // Wait previous height to enter precommit
	RoundStepPrevote       = RoundStepType(0x06) // Did prevote, gossip prevotes
	RoundStepPrevoteWait   = RoundStepType(0x07) // Did receive any +2/3 prevotes, start timeout
	RoundStepWaitToPrecommit = RoundStepType(0x08) // Wait previous height to enter commit
	RoundStepPrecommit     = RoundStepType(0x09) // Did precommit, gossip precommits
	RoundStepPrecommitWait = RoundStepType(0x0a) // Did receive any +2/3 precommits, start timeout
	RoundStepWaitToCommit  = RoundStepType(0x0b) // Wait previous height to finish commit
	RoundStepCommit        = RoundStepType(0x0c) // Entered commit state machine
	// NOTE: RoundStepNewHeight acts as RoundStepCommitWait.
)

// String returns a string
func (rs RoundStepType) String() string {
	switch rs {
	case RoundStepNewHeight:
		return "RoundStepNewHeight"
	case RoundStepNewRound:
		return "RoundStepNewRound"
	case RoundStepWaitToPropose:
		return "RoundStepWaitToPropose"
	case RoundStepPropose:
		return "RoundStepPropose"
	case RoundStepWaitToPrevote:
		return "RoundStepWaitToPrevote"
	case RoundStepPrevote:
		return "RoundStepPrevote"
	case RoundStepPrevoteWait:
		return "RoundStepPrevoteWait"
	case RoundStepWaitToPrecommit:
		return "RoundStepWaitToPrecommit"
	case RoundStepPrecommit:
		return "RoundStepPrecommit"
	case RoundStepPrecommitWait:
		return "RoundStepPrecommitWait"
	case RoundStepWaitToCommit:
		return "RoundStepWaitToCommit"
	case RoundStepCommit:
		return "RoundStepCommit"
	default:
		return "RoundStepUnknown" // Cannot panic.
	}
}

//-----------------------------------------------------------------------------

// RoundState defines the internal consensus state.
// It is Immutable when returned from ConsensusState.GetRoundState()
// TODO: Actually, only the top pointer is copied,
// so access to field pointers is still racey
// NOTE: Not thread safe. Should only be manipulated by functions downstream
// of the cs.receiveRoutine
type RoundState struct {
	Height                  int64 // Height we are working on
	Round                   int
	Step                    RoundStepType
	StartTime               time.Time
	CommitTime              time.Time // Subjective time when +2/3 precommits for Block at Round were found
	Validators              *types.ValidatorSet
	Proposal                *types.Proposal
	ProposalBlock           *types.Block
	ProposalBlockParts      *types.PartSet
	ProposalCMPCTBlock      *types.Block
	ProposalCMPCTBlockParts *types.PartSet
	LockedRound             int
	LockedBlock             *types.Block
	LockedBlockParts        *types.PartSet
	LockedCMPCTBlock        *types.Block
	LockedCMPCTBlockParts   *types.PartSet
	Votes                   *HeightVoteSet
	CommitRound             int            //
	LastCommit              *types.VoteSet // Last precommits at Height-1
	LastValidators          *types.ValidatorSet
}

// RoundStateEvent returns the H/R/S of the RoundState as an event.
func (rs *RoundState) RoundStateEvent() types.EventDataRoundState {
	// XXX: copy the RoundState
	// if we want to avoid this, we may need synchronous events after all
	rs_ := *rs
	edrs := types.EventDataRoundState{
		Height:     rs.Height,
		Round:      rs.Round,
		Step:       rs.Step.String(),
		RoundState: &rs_,
	}
	return edrs
}

// String returns a string
func (rs *RoundState) String() string {
	return rs.StringIndented("")
}

// StringIndented returns a string
func (rs *RoundState) StringIndented(indent string) string {
	return fmt.Sprintf(`RoundState{
%s  H:%v R:%v S:%v
%s  StartTime:     %v
%s  CommitTime:    %v
%s  Validators:    %v
%s  Proposal:      %v
%s  ProposalBlock: %v %v
%s  LockedRound:   %v
%s  LockedBlock:   %v %v
%s  Votes:         %v
%s  LastCommit:    %v
%s  LastValidators:%v
%s}`,
		indent, rs.Height, rs.Round, rs.Step,
		indent, rs.StartTime,
		indent, rs.CommitTime,
		indent, rs.Validators.StringIndented(indent+"    "),
		indent, rs.Proposal,
		indent, rs.ProposalBlockParts.StringShort(), rs.ProposalBlock.StringShort(),
		indent, rs.LockedRound,
		indent, rs.LockedBlockParts.StringShort(), rs.LockedBlock.StringShort(),
		indent, rs.Votes.StringIndented(indent+"    "),
		indent, rs.LastCommit.StringShort(),
		indent, rs.LastValidators.StringIndented(indent+"    "),
		indent)
}

// StringShort returns a string
func (rs *RoundState) StringShort() string {
	return fmt.Sprintf(`RoundState{H:%v R:%v S:%v ST:%v}`,
		rs.Height, rs.Round, rs.Step, rs.StartTime)
}

func (rs *RoundState) IsProposalComplete() bool {
	if rs.Proposal == nil || rs.ProposalBlock == nil {
		return false
	}
	// we have the proposal. if there's a POLRound,
	// make sure we have the prevotes from it too
	if rs.Proposal.POLRound < 0 {
		return true
	} else {
		// if this is false the proposer is lying or we haven't received the POL yet
		return rs.Votes.Prevotes(rs.Proposal.POLRound).HasTwoThirdsMajority()
	}
}