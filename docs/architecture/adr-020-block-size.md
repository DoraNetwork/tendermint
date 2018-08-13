# ADR 020: Limiting txs size inside a block

## Changelog

13-08-2018: Initial Draft

## Context

We currently use MaxTxs to reap txs from the mempool when proposing a block,
but enforce MaxBytes when unmarshalling a block, so we could easily propose a
block thats too large to be valid.

We should just remove MaxTxs all together and stick with MaxBytes, and have a
`mempool.ReapMaxBytes`.

But we can't just reap BlockSize.MaxBytes, since MaxBytes is for the entire block,
not for the txs inside the block. There's extra amino overhead + the actual
headers on top of the actual transactions + evidence + last commit.

## Proposed solution

Therefore, we should

1) Get rid of MaxTxs.
2) Use MaxTxsBytes (rename to MaxDataBytes?) to limit the overall transactions size (Data).
3) Introduce Max\* variables for header, evidence and last commit:
  * MaxHeaderBytes
  * MaxEvidenceBytes
  * MaxLastCommitBytes
4) Use these variables to limit the appropriate parts (header, evidence, ...)
https://github.com/tendermint/tendermint/blob/8a1a79257e8e8b309cd35bb1fe40bf9b3330fd7d/types/block.go#L195

Proposed default values are:

MaxHeaderBytes - 512 bytes (~200 bytes hashes + 200 bytes - 50 UTF-8 encoded symbols of chain ID)
MaxEvidenceBytes - 4096 bytes (10 DuplicateVoteEvidence ~350 bytes each)
MaxLastCommitBytes - 163840 (160 KB) (1000 votes ~150 bytes each)

When we unmarshall a block, we calculate the sum of the four above variables
and use the result as the total allowed limit for a block:

```go
_, err = cdc.UnmarshalBinaryReader(
    cs.ProposalBlockParts.GetReader(),
    &cs.ProposalBlock,
    int64(MaxTxsBytes+MaxHeaderBytes+MaxEvidenceBytes+MaxLastCommitBytes),
)
```

## Status

Proposed.

## Consequences

### Positive

* one way to limit the size of a block
* more granular control over the size of block parts (header, evidence, ...)

### Negative

* Neccessity to configure 3 additional variables

### Neutral
