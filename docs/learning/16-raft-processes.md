# Increment 16: move Raft across process boundaries

The previous replication demo controlled every message in one process. This increment
runs each peer as a separate executable with its own exclusively owned journal, HTTP
listeners, clock, and outbound senders. It tests whether a surviving majority can make
progress after an actual peer process dies.

```powershell
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/raft_process_check.py bin/raftpeer.exe --output artifacts/raft-process/run.json
```

On Linux use `bin/raftpeer` for both commands. The runner allocates six loopback ports,
starts three peers, and preserves statuses, acknowledgements, process logs, source SHA,
working-tree state, and binary hash in the output JSON. Temporary journals are removed
after the check. No inference requests are part of this protocol experiment.

## One command through the system

```text
client POST /propose -> leader owner loop -> synced local journal
                                          |
                           bounded HTTP AppendEntries
                                          |
                          follower owner loop -> synced follower journal
                                          |
                              separate AppendResponse message
                                          |
                 leader observes majority -> commit index -> client HTTP 200
```

An HTTP 204 from the peer endpoint means that the message was processed. It is not a
Raft replication acknowledgement; that travels as an independent protocol message.
The client waits for its exact log index and term to commit. A leader that can only
write to its own disk cannot return a committed HTTP 200 in a three-member group.

Client timeout or a lost connection leaves the outcome unknown. A timed-out proposal
may commit after the majority returns. This raw command API has no request IDs or
deduplication; blindly retrying can append another command. Application-level retry
semantics remain the responsibility of the forthcoming controller adapter.

## Failure trace

1. Elect a leader and commit four uniquely identified command payloads.
2. Start another client request and kill the leader while it is in flight. The runner
   records whether HTTP succeeded or failed; it does not assume the command was lost.
3. Let the two survivors elect a leader and commit a new command.
4. Restart the killed peer using its existing journal. Wait for all logs and committed
   prefixes to agree; check every acknowledged index, term, and command byte.
5. Kill two peers. Submit to the remaining leader and require its commit wait to time out.
6. Restart the majority, converge, then kill/restart all peers. Re-establish commitment
   through the new leader's no-op and verify every acknowledged command again.

These are abrupt process kills on one host. This exercise does not simulate power loss,
physical disk loss, multi-host networking, or all possible partitions/interleavings.
The deterministic core tests continue to exercise message loss/reordering schedules.

## Bounds and ownership

One owner goroutine alone calls the Raft core. HTTP handlers submit events to a queue
of 32; eight concurrent handlers are admitted per listener. There are at most eight
pending proposal waiters, each with a five-second commit deadline. Each remote peer
has one sender and a two-message outbound queue. Full queues drop messages and the
core retransmits on later ticks. Send failures and drops appear in diagnostic status.

Ticks are 50 ms, heartbeat spacing is two ticks, and election timeout is randomized
from a ten-tick base. These are local experiment settings, not measured operational
defaults. Peer HTTP requests time out after 300 ms; proposal bodies are limited to
64 KiB and peer bodies to two MiB. The CLI uses the core's default 16-entry batches.
The inherited 1024-entry/four-MiB log bound and full-image journal write cost remain.
There is no compaction, dynamic membership, or unbounded recovery history.

The peer protocol and proposal/diagnostic API use separate listeners. Both require
explicit loopback IP addresses. Membership IDs are checked but not authenticated;
there is no TLS. This is a trusted local experiment, not a remotely exposed service.
HTTP connection handling and OS socket backlogs are not globally memory bounded by
the application handler limits.

`GET /status` exposes local diagnostic log/commit state. It is **not a linearizable
configuration read**, and payload bytes are visible there. Do not submit secrets.
Successful raw commands are committed, not applied controller changes. The existing
serving controller and real-model gateway retain their previous contracts.

## Inspect it yourself

To start peers manually, use the same fixed membership string in three terminals:

```powershell
$peers = 'a=http://127.0.0.1:19201,b=http://127.0.0.1:19202,c=http://127.0.0.1:19203'
.\bin\raftpeer.exe -id a -peers $peers -client-listen 127.0.0.1:19301 -journal artifacts/peers/a.wal
```

For B/C change the ID, client port to 19302/19303, and journal path. Query each client's
`/status`, find the leader, and POST opaque bytes to that leader's `/propose` endpoint.
Stop two peers and predict the response before trying another write. After restoring
them, check whether that timed-out command appears in the committed log. Explain why
a timeout cannot be treated as proof that a command was rejected.

Next: connect committed commands to configuration-machine replay, request-ID retry
results, and a read contract that prevents stale local state from claiming freshness.
