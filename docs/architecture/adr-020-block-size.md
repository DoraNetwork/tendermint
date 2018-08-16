# ADR 020: Limiting txs size inside a block

## Changelog

13-08-2018: Initial Draft
15-08-2018: Second version after Dev's comments

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
2) Rename MaxTxsBytes to MaxBytes.
3) Add MaxNumEvidences cs parameter to limit the number of evidences per block.

Proposed default values are:

- MaxNumEvidences - 10

When we need to ReapMaxBytes from the mempool, we calculate the upper bound as follows:

```
MaxLastCommitBytes = {number of validators currently enabled} * {MaxVoteBytes}
MaxEvidenceBytes = {MaxNumEvidences} * {MaxEvidenceBytes}

mempool.ReapMaxBytes(MaxBytes - AminoOverheadForBlock - MaxLastCommitBytes - MaxEvidenceBytes - MaxHeaderBytes)
```

where MaxVoteBytes, MaxEvidenceBytes, MaxHeaderBytes and AminoOverheadForBlock
are constants defined inside the `types` package:

- MaxVoteBytes - 150 bytes
- MaxEvidenceBytes - 350 bytes
- MaxHeaderBytes - 512 bytes (~200 bytes hashes + 200 bytes - 50 UTF-8 encoded
  symbols of chain ID 4 bytes each in the worst case + amino overhead)
- AminoOverheadForBlock - 16 bytes (assuming MaxHeaderBytes includes amino
  overhead for encoding header, MaxVoteBytes - for encoding vote, etc.)

## Status

Proposed.

## Consequences

### Positive

* one way to limit the size of a block
* more granular control over the size of block parts (header, evidence, ...)

### Negative

* Neccessity to configure additional variables

### Neutral
