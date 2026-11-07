# Increment 18 plan: fresh configuration reads without journal growth

Status: the core protocol and its targeted tests are implemented. GET `/config` still
uses increment 17's committed journal barriers. Runtime/API integration and the repeated-
read process check are the next slice; no HTTP read-cost reduction is claimed yet.

```powershell
go test ./internal/raft -run Read -v
```

The new core API is `BeginRead`, `ReadReady`, and `ForgetRead`. Callers must serialize
these with every other core operation and wait for local application before responding.
The core bounds contexts to eight, retransmits unconfirmed IDs on heartbeat ticks, checks
the current-term committed prefix, and fences higher-term or pre-restart confirmations.

## Problem

Every configuration read currently appends a command and syncs a full log image. Polling
eventually exhausts the 1024-entry bound despite no configuration changes. Fix this read
cost before wiring the gateway to sustained replicated-controller polling.

## Safety contract

1. A leader must first commit an entry from its current term. Otherwise it cannot start
   a read confirmation or claim it knows the committed prefix after election/restart.
2. Each read captures the current committed index and allocates a new context ID after
   request arrival. Contexts are scoped to the leader's term and never reused within it.
3. The leader sends that context to peers and needs a majority of distinct members,
   including itself, to confirm its term. Duplicate, unknown, stale-term, and wrong-context
   replies cannot satisfy the read. Higher-term traffic invalidates its leadership.
4. Only after confirmation, and after local application reaches at least the captured
   index, may it capture the configuration response. There is no leadership lease.
5. A later write may be included in that response if it has already applied. The response
   must still represent a point during the read's invocation/response interval.
6. Cancelled/timed-out requests release their contexts. No context survives leadership
   change or restart. Bound both live core contexts and runtime waiters to eight.
7. Read confirmations do not append entries or update the follower commit index. Normal
   log replication continues to carry log matching and commit propagation.

## Implementation sequence

- Implemented: explicit read-probe/read-response messages and a small bounded context map
  to the single-owner Raft core. Retransmit unconfirmed contexts on heartbeat ticks.
- Targeted tests pass for current-term gating, distinct majority, delayed/duplicate replies, term
  changes, restart fencing, cancellation/context bounds, and persistence failure before
  a higher-term acknowledgement.
- Next: change GET `/config` to wait on that protocol and local applied index. Preserve replay
  support for old read-barrier entries and keep PUT behavior unchanged.
- Then extend the existing three-process relay scenario: repeated fresh reads must keep log
  length and journal file sizes unchanged; the isolated former leader must still fail
  fresh reads while the majority can serve them.
- Core checks run with the existing Go suite. API integration must also pass both process
  scenarios and record a clean source commit plus repeated-read raw output before claiming
  that HTTP reads avoid appends.

## Boundaries and acceptance

The planned HTTP integration will remove reads as a source of log growth. Writes, retries, conflicts, and leader
no-ops still need retention/compaction. No general storage-capacity or throughput claim
follows. The transport, fixed membership, trusted-loopback assumptions, and lack of
gateway leader discovery remain unchanged.

Integration acceptance: the targeted core counterexamples fail unsafe shortcuts, the existing
configuration/recovery behavior remains valid, sustained reads in the declared small
same-host check do not append/sync journal records, and a minority cannot claim freshness.

After API integration: gateway controller-peer selection/failover with bounded view age,
then checkpoint/log retention with a complete successful-request-ID recovery contract.
