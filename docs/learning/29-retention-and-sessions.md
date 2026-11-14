# 29: Keep healthy replicas progressing and bound retry state

The review exposed a real liveness defect: only the leader could trim its log.
Two always-healthy followers filled their 1,024-entry images, rejected appends,
and prevented a three-member cluster from committing. The earlier 1,103-index
single-member experiment did not cover this failure. It remains historical evidence,
not evidence that the old multi-peer implementation worked beyond that boundary.

## Automatic retention on every replica

In retained-storage mode, each node snapshots its fully applied committed prefix
after 128 entries or one MiB of suffix pressure. A follower need not become leader.
Capture validates the committed position and preserves any uncommitted suffix.
Publication is durable before the prefix is removed. A behind follower can instead
install a leader's validated image. `/status` reports capture counts, installed
indexes, and capture failures. `POST /retain` remains available for manual capture.

A full follower now returns a correlated capacity rejection, rather than leaving
the leader to retry an unanswered RPC. It can advance commit through its already
matched prefix, deliver that prefix, retain it, and accept the next append. The
leader retries capacity failures on a later heartbeat, avoiding a hot retry loop.

The core tests exercise entry and byte pressure and 25 seeded schedules combining
retention, snapshots, reordered/duplicated/dropped messages, elections, and storage
reopen. The native three-peer check first runs 2,200 logged retries with all peers
healthy, verifies their bases exceed 1,024, then exercises offline catch-up and restarts.

## A new, explicit bounded retry contract

The old `PUT /config` contract remembers every successful request ID forever. It
still does: silently expiring those IDs could re-execute an old administrative write.
Its retained-mode limit remains 1,024 legacy successful IDs and an eight-MiB image.

New clients can use one of **64 durable session slots** instead. Each slot retains
a generation fence, registration retry information, and only its latest successful
sequence/result. It supports an unbounded number of ordinary updates within those
fixed resources, subject to uint64 counters and the eight-MiB encoded-image limit.
Slots are operator-assigned; clients sharing a slot must coordinate.

Open an unused slot against the current leader:

```http
POST /sessions
Content-Type: application/json

{"slot":0,"expected_generation":0,"request_id":"operator-registration-1"}
```

The result is `{"slot":0,"generation":1}`. Repeating this registration returns
the same handle until that generation is explicitly replaced.

```http
PUT /sessions/config
Content-Type: application/json

{"slot":0,"generation":1,"sequence":1,"expected_revision":0,"workers":[]}
```

The update uses the same global revision CAS as `/config`. Its successful result
is revision 1. Retry the **identical** latest sequence after an uncertain response:
it returns the original result, including after another session advances the state
or the cluster restarts. Reusing that sequence with a different body returns 409.
A revision conflict does not consume a sequence; refresh `/config` and retry with
the expected revision corrected. Do not issue the next sequence while the previous
outcome is unknown. Once a higher sequence succeeds, retries of older sequences
return **410**, never re-execute, and no longer promise the old result.

Reopening slot 0 with `expected_generation:1` and a **new** registration ID produces
generation 2. This deliberately retires generation 1. Old-generation updates and
registrations return 410. The durable generation counter survives restart; there
is no wall-clock expiry or unbounded tombstone table. All session results and
fences are replicated commands, not HTTP-process memory.

The native regression performs 2,200 distinct successful session writes across
automatic three-peer retention, full-cluster restart, generation retirement, and
leader failure. Legacy original retry results must still be preserved.

## Compatibility and storage

Session routes require retained configuration mode. Their version-3 commands and
version-2 application images require **all members to run the upgraded binary**
before use. Existing version-1 images and version-1/2 commands remain readable.
Deploy the upgraded release consistently to all members; mixed-version cluster
operation is unsupported. There is no downgrade guarantee after session commands.
Existing legacy request history is not evicted or automatically migrated.

Persistence still publishes bounded whole images synchronously in the Raft owner.
This keeps persistence-before-send ordering simple, at the cost of fsync latency
blocking peer processing. Automatic retention bounds the suffix; it does not make
this a high-throughput log. A segmented WAL or asynchronous durable-ready pipeline
would require a separate storage/recovery design and measurement.

On Linux, creation now syncs the file and containing directory chain before a store
is usable, so its first durable vote does not rely on an unsynced directory entry.
Other platforms retain their documented process-crash guarantee. Process-kill tests
and directory-sync tests do not constitute physical power-loss validation.

## Client-visible fault histories

`scripts/check_config_history.py` runs three native peers behind actual HTTP relays.
It records concurrent CAS/read/retry calls, isolates a leader, heals the network,
kills a process, and restarts the cluster after automatic retention. Its finite
checker searches for a legal sequential history respecting recorded real-time order.
An unknown write can be absent or commit after the error response; it is not assumed
to have failed. Search-budget exhaustion is inconclusive, never success.

This checker models the legacy config API, not the session API or all possible
executions. Session behavior has separate state-machine and native-process checks.
The harness uses one host, and timestamps have that host's monotonic-clock resolution.

```sh
go test ./internal/raft ./internal/control ./internal/raftnet
go build -o bin/raftpeer ./cmd/raftpeer
python scripts/check_replicated_retention.py bin/raftpeer --output artifacts/review/retention.json
python -m unittest discover -s scripts -p test_config_history.py -v
python scripts/check_config_history.py bin/raftpeer --output artifacts/review/history.json
```

On Windows use `bin/raftpeer.exe`. Checks use owned temporary processes and storage;
they do not modify a running deployment.

## Sharing an engine's admission bound

Frontend counters remain local. To give two frontend gateways one capacity bound
without introducing a distributed counter, put one existing gateway process directly
in front of each engine and point both frontends only at that worker-side gate.
`python scripts/check_shared_worker.py --output artifacts/shared-worker/local.json`
checks this with real llama.cpp: frontend A holds a stream, frontend B is locally
idle but receives 503 from the shared gate, cancellation releases engine capacity,
and B can then generate. Build `bin/gateway.exe` and install the pinned assets first.

This adds a network hop and a per-engine process dependency. Every path must use
the same gate; another gate or direct engine access defeats the shared bound. It
does not coordinate distributed counters, make active streams recoverable, or
establish multi-host high availability. Keep the engine listener private.
