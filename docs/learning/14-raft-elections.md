# Increment 14: elect a controller leader without claiming replicated writes

The configuration machine now needs a durable shared command order. The first protocol
increment implements fixed-membership elections, based on Figure 2 and section 5.2 of
[Ongaro and Ousterhout's extended Raft paper](https://raft.github.io/raft.pdf).

This core is isolated from the serving gateway and controller HTTP API. It implements no
log replication, quorum commit, distributed reads, snapshots, or dynamic membership yet.
The current controller remains a single local journal owner.

## Run the scenario

```powershell
go test ./internal/raft -v
go run ./cmd/electiondemo
```

Use the local Go path if needed. The demo uses explicit ticks and queued in-process message
delivery with three real term/vote journals under artifacts/elections/. Its transcript is
artifacts/elections/result.json. It is a deterministic transport model, not a three-process
network deployment or a real controller failover demonstration. New runs use separate journal
directories, preserving old records. No LLM requests are needed to test this protocol boundary.

The scenario elects A in term 1, isolates A, and lets B/C elect C in term 2. A still reports
leader in term 1 while isolated. After healing, the higher-term heartbeat makes A persist
term 2 and become follower. Reopening all journals preserves terms/votes while all nodes
start as followers; roles and known leaders are volatile.

## Core boundary

`Node.Tick` advances one logical tick. `Node.Step` handles one delivered message. Both return
outbound messages only after required term/vote persistence succeeds. Callers must serialize
access and deliver returned messages; there are no timers, sockets, or goroutines in the core.

Each node draws an election timeout in [base, 2*base) from a seeded PRNG. The seed is an explicit
input for reproducibility. A deployed adapter will need appropriate independent timer/seed
behavior; replaying a seed is not a guarantee of liveness under arbitrary schedules.

On timeout, persist an incremented term and self-vote, become candidate, and request votes.
A candidate needs a strict majority of distinct configured members. Duplicate, old-term,
outsider, and misdirected votes cannot create a majority. A split vote leads to another
timeout/election. Receiving a higher term persists that term, clears the old vote, and steps
down. A current-term election heartbeat also makes a candidate a follower without clearing
its already persisted vote.

Vote grants require a free or matching vote and an up-to-date candidate log tail: compare
last terms first, then last indices. Tail metadata is supplied to this election slice;
actual log entries and their persistence are not implemented. Rejected or stale messages
do not reset election timers. Granted votes and current-term heartbeats do.

`election_heartbeat` is deliberately named for this limited slice. It has no AppendEntries
prefix validation or entries. It cannot authorize commits, controller updates, or fresh
reads. An isolated old leader may retain its role until it learns a newer term. Election
safety means at most one elected leader per term, not one leader belief across all terms.

## Durable term/vote state

`FileStorage` uses an exclusive journal, bounded checksummed frames, and a format-kind marker.
It syncs before returning Save, rejects term regression/conflicting same-term votes, and
syncs recovered state before exposing it. Incomplete final frames are repaired; complete
checksum corruption rejects startup. Storage failures fence both the store and node: no
vote or election messages are returned from a failed persistence operation.

Controller and election journals share only the OS-lock utility, not state or record formats.
The existing controller crash check still passes after that extraction. Locking is supported
on Windows/Linux. Local file reopen checks do not prove survival of disk loss, power failure,
or file-creation metadata loss. Journals have no compaction or migration scheme yet.

## Evidence and limitations

Targeted tests cover majority counting, duplicates/stale votes, split retries, one-node
majorities, heartbeat terms, membership filtering, log-tail ordering, vote survival across
file reopen, and persistence failure before campaign, term advancement, or vote grant.
File tests cover ownership exclusion, incomplete headers/payloads, checksum corruption,
and recovery of a complete vote after an uncertain sync result.

Fifty seeded five-node runs model partitions, loss, duplication, reordering, and node restart
using retained term/vote state. They check historical leader uniqueness per term and
convergence after the modeled network heals. This samples schedules; it is not a formal
proof, an exhaustive test, or evidence of multi-host behavior. Seeds are test names for replay.

Next: persist and reconcile replicated log entries, then enforce quorum/current-term commit
rules before applying any controller command. Election-only leader status is insufficient.
