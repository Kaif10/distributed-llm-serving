# Increment 18: fresh configuration reads without journal growth

GET `/config` now uses the core's fresh majority confirmation instead of appending
another read-barrier command. The owner captures a committed index after request arrival,
waits for a distinct current-term majority, applies through that index, then snapshots
the machine. PUT behavior and replay of old read-barrier entries are preserved.

```powershell
go test ./internal/raft -run Read -v
go test ./internal/raftnet -v
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/replicated_config_check.py bin/raftpeer.exe --output artifacts/replicated-config/read-quorum.json
```

On Linux omit `.exe`. The runner uses three actual peer processes, separate journals,
and controlled loopback HTTP relays. These are control-plane checks without LLM traffic.
The evidence ledger records source commits and raw results.

## Problem

The previous read path appended a command and synced a full log image for each poll.
That could exhaust the 1024-entry bound despite no configuration changes. A read now
requires network confirmation and local application, without adding a command.

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

## Runtime and resource lifecycle

The HTTP handler submits a read event to the same single-owner queue as proposals and
protocol messages. The owner first applies any newly committed entries, then calls
`BeginRead`. Followers, fenced nodes, and leaders whose current-term no-op has not yet
committed cannot start a read; the HTTP response is 503. Retry after leadership settles.
There is no automatic forwarding or leader discovery yet.

The core's `ReadReady` checks are necessary but insufficient on their own: the owner also
requires `applied_index >= token.Index`. It captures a cloned configuration snapshot only
when both conditions hold. Diagnostic `/status` remains local and potentially stale.
The existing leader no-op establishes commitment after restart before this path can serve.

There are at most eight live read contexts and eight runtime read waiters, within the
existing eight-handler client listener admission limit. Cancellation removes a waiter and
calls `ForgetRead` on the next owner completion/tick. Success, leadership loss, and shutdown
also release contexts. An isolated peer with a previously committed current-term entry
can start a read but cannot finish its quorum check; it returns HTTP 504 at the deadline.

No disk write is needed to confirm an unchanged current term. Observing a newer term still
requires normal durable term/vote handling before replying. Read probes do not replace log
matching, replication acknowledgements, or commit propagation. Existing bounded sender queues
may drop probes; the core retries live unconfirmed contexts on heartbeat ticks.

## Verification

- Core counterexamples cover current-term commit gating, distinct quorum counting,
  delayed/wrong-context replies, terms/restart, context bounds, and persistence failure.
- HTTP tests complete 1,100 reads through a single-member configuration runtime while
  checking unchanged journal length and a one-entry log. This exceeds the entry limit
  that would have stopped the former read implementation.
- A targeted application test holds the machine behind its captured read index and
  verifies that even an already-confirmed read cannot respond. After ordered application,
  it sees the update and successfully skips an older persisted read-barrier command.
- Ten network cancellations exceed the eight-context bound and verify that both runtime
  waiters and core tokens are released. A higher-term vote request promptly fails a live
  read rather than allowing it to finish under old leadership.
- The three-process relay scenario serves 128 sequential reads without configuration
  updates, checking all journal byte counts, log entries, terms, and pending read counts
  before/after. It then isolates the former leader and verifies that a newer majority's
  configuration is readable while the old leader cannot claim freshness. Existing CAS,
  successful retry replay, and full-restart checks remain part of the same run.

These are sampled local checks, not a formal linearizability proof or a throughput
benchmark. Unchanged journal lengths/logs are the measured read-growth evidence; no
filesystem syscall tracing or physical power-loss test is included.

## Boundaries and acceptance

Reads no longer consume log entries. Writes, retries, conflicts, and leader no-ops
still need retention/compaction. No general storage-capacity or throughput claim
follows. The transport, fixed membership, trusted-loopback assumptions, and lack of
gateway leader discovery remain unchanged.

Next: gateway controller-peer selection/failover with bounded view age,
then checkpoint/log retention with a complete successful-request-ID recovery contract.
