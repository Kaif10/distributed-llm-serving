# Increment 15: distinguish a durable proposal from a committed command

`NewReplica` enables durable log replication on the isolated Raft core. The older `New`
constructor remains election-only and refuses a nonempty durable log. Neither constructor
is connected to serving-controller HTTP requests yet.

```powershell
go test ./internal/raft -v
go run ./cmd/replicationdemo
```

Use the local Go executable path if needed. The demo uses deterministic queued message
delivery and three actual local journals, applying committed command bytes to independent
configuration machines. It is not a network deployment, process-crash experiment, or
controller failover test. Journals/transcript are saved under artifacts/replication/.

## Follow the scenario

1. A wins term 1, commits a no-op and then a drain command. Every configuration machine
   reaches revision 1 at Raft commit index 2.
2. A is isolated. It durably appends another command locally, but cannot commit it.
3. B/C elect C in term 2, commit its no-op and a resume command, reaching revision 2 at
   log index 4. A still has only commit index 2 and an extra uncommitted suffix.
4. Healing lets C replace A's conflicting suffix. All nodes reach the same committed log
   and revision 2; the isolated proposal never reaches application.
5. Reopening the journals preserves entries, terms, and votes, while volatile commit and
   delivery cursors reset. A term-3 election/no-op re-establishes commitment through index 5.
   Fresh configuration machines rebuild revision 2 by replaying committed commands.

## Replication boundary

Propose copies command bytes, persists the new entry, and returns messages for delivery.
Success from Propose means local acceptance, **not quorum commitment or client success**.
The caller must wait for committed delivery before acknowledging an application operation.

The replication path sends bounded AppendEntries batches with a previous index/term.
Followers reject missing/mismatched prefixes and persist changes before success responses.
Only a term conflict removes an uncommitted suffix. A same-index/same-term different command
is rejected, and the core refuses to replace a known committed entry.

Each peer has one pending probe. Retransmission keeps its ID and payload until a response;
otherwise delayed replies could become perpetually stale. Once progress changes, old probe
replies are ignored. Success must confirm exactly the sent range. Negative replies back off
the next index without undoing already confirmed match progress. This is linear backoff,
not an optimized conflict-term protocol.

The leader advances commitment by counting a majority only for a current-term entry. Earlier
entries then become committed as part of that prefix. Each newly elected replica leader
persists a current-term no-op to establish such a boundary. Followers advance only through
the prefix confirmed by the received append, not an unmatched local trailing suffix.
These rules follow [the Raft paper, sections 5.3 and 5.4](https://raft.github.io/raft.pdf).

## Persistence and replay limits

For inspectability, each raft-state-v2 journal record stores hard state and a full bounded
log image. Writes and recovery have sync barriers. The reader accepts older election-v1
records before v2, but does not allow downgrade afterward. Election term/vote safety and
exclusive local ownership still apply. A failed log persistence operation returns no
acknowledgement and fences the node. This is separate from the configuration journal format.

Limits: 1024 entries, four MiB of command data, 64 KiB per command, and batches of 1-64
entries (default 16). Records are limited to eight MiB. There is no compaction; a full log
can prevent further progress, including the leader no-op. Full-image writes cost O(log size)
per update and are not a production throughput design. No performance claim is made.

Commit index, next/match tracking, pending probes, and DrainCommitted's cursor are volatile.
After reopening, commitment must be re-established before delivery. DrainCommitted returns
copies and advances a volatile cursor; it does not implement durable exactly-once application.
A future application adapter needs an applied-index/rebuild/checkpoint contract and must
skip no-ops. Configuration revision is independent of Raft log index.

## Verification

Targeted tests use real journals for commit/delivery and reopen. Other tests control messages
to cover prior-term majority without commitment, short empty appends with extra local suffixes,
conflict repair, committed-prefix protection, duplicate/stale probes, and persistence failure.
A legacy-vote fixture upgrades to v2 and preserves its log after incomplete-tail recovery.

Twenty-five seeded five-node runs model loss, duplication, reordering, partitions, restarts,
and proposals. They retain historical committed entries and check that no node commits a
different term/command at an already observed committed index. They also check elected-leader
uniqueness per term and progress after healing. Modeled storage retains state across restart;
these are sampled schedules, not exhaustive proofs or physical disk/network faults.

The serving gateway/controller retain their existing single-controller contract. No real
transport, replicated client API, fresh distributed reads, durable applied checkpoint,
membership change, or model-serving failover claim is added by this increment.

Next: put this core behind a bounded real-process transport and verify election/log recovery
with actual peer processes. Only then connect application acknowledgements and controller
reads to the replicated commit/apply contract.
