# Increment 17: commit, apply, and read fleet configuration

Raw committed bytes do not yet change the worker configuration. This increment adds
`raftpeer -configuration`: a replicated configuration API backed by the existing
deterministic `control.Machine`. The original `cmd/controller` remains the separate
single-writer service. Gateway discovery/failover across controller peers is still pending.

```powershell
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/replicated_config_check.py bin/raftpeer.exe --output artifacts/replicated-config/run.json
```

On Linux omit `.exe`. This runner starts three real peer processes and controlled HTTP
relays on one host. The relays drop protocol messages to/from a selected process while
leaving its client listener alive. This is deliberate network-fault injection over actual
HTTP connections; the relays do not replace the Raft nodes or their journals. No inference
traffic is included in this control-plane check.

## Update contract

`PUT /config` accepts the same JSON update as the single-writer API:

```json
{
  "request_id": "drain-worker-1",
  "expected_revision": 0,
  "workers": [{"id": "worker-1", "origin": "http://127.0.0.1:8081", "draining": true}]
}
```

The leader validates shape and appends a versioned update command. After quorum commitment,
each peer applies the committed prefix in order. Only then does the client receive its
configuration result. `expected_revision` is evaluated **during application**, so two
concurrent commands expecting the same revision cannot both advance it. The loser gets
HTTP 409. Configuration revision is distinct from log index: no-ops, reads, conflicts,
and successful retries consume entries without advancing the configuration revision.

Successful request IDs remember the original update and result. An identical retry returns
that original result, even after newer revisions or a full restart. Reusing a successful ID
with altered update fields returns 409. Failed CAS attempts are not retained as terminal
request-ID results; a later retry is evaluated at its new position in the log. Every retry
still passes through quorum commitment; local cached results cannot bypass an unavailable
majority. A historical PUT retry result must not be treated as the latest configuration.

A deadline, leadership change, or connection failure leaves the update outcome unknown.
Retry with the same request ID and payload. Successful deduplication concerns configuration
transitions only; it does not make external side effects or model execution exactly-once.

## Fresh reads without a leadership lease

`GET /config` appends a read-barrier command in the current term. The peer waits for its
exact entry to commit and returns the snapshot captured when that entry is applied.
All earlier committed updates have been applied before the snapshot is taken. Later updates
in the same delivery batch cannot alter that captured result.

This gives the read a position in the same committed order as updates. An isolated former
leader can still think it is leader and have an old local snapshot, but cannot commit a
new barrier with only its own vote. It times out instead of labelling that snapshot fresh.
Followers return 503; there is no automatic forwarding or client leader discovery yet.
`GET /status` remains explicitly local diagnostics and can expose stale configuration.

The simple read design writes a synced journal record and consumes a log entry for every
read. It inherits the 1024-entry/four-MiB log bound and full-image journal write cost.
Reads, retries, and conflicts can exhaust the log and stop progress. This is a bounded
protocol implementation; continuous gateway polling needs a read-path/log-retention design
before it can run indefinitely. No latency or throughput improvement is claimed.

## Restart and application state

The machine and applied index are volatile. At startup, validate the versioned log records
but do not apply an unconfirmed suffix. The new leader's current-term no-op re-establishes
commitment; peers then rebuild the configuration and successful request-ID results by
replaying the committed prefix into a fresh machine. No-op entries advance applied index
without changing revision. Application errors such as a CAS conflict do not stop subsequent
commands. Invalid committed command encoding or application-order violations stop the host.

There is no durable application checkpoint or compaction. A public configuration snapshot
omits deduplication state and cannot serve as a complete recovery checkpoint.

Use **fresh configuration-only journals** for this mode. Do not reuse a raw-proposal or
single-writer-controller journal, mix modes within a group, change the fixed member IDs,
or move a journal to a different member. Configuration mode rejects opaque commands at
startup and on peer receipt, and disables `/propose`. Cluster identity/application mode
are not authenticated or durably bound to membership. All listeners remain trusted loopback.

## Failure trace to inspect

1. Commit a drain at revision 1 and wait for all machines to apply it.
2. Isolate the leader's protocol messages while leaving its client API accessible.
3. The other two peers elect a leader and commit a resume at revision 2.
4. After that update returns, read both APIs. The majority returns revision 2. The old
   leader times out; its diagnostic snapshot still shows revision 1. An isolated update
   also times out and must not change that local application state before commitment.
5. Heal the partition and retry the initial drain request. The original revision-1 result
   returns without reverting the current state from revision 2.
6. Race two commands expecting revision 2: exactly one succeeds and one conflicts.
7. Kill/restart all peers. Recover revision 3, retry the original ID, and verify its original
   result is restored. Reject the same ID with changed fields and refuse follower reads.

The raw JSON includes logs, revisions, applied/committed indices, response outcomes, source
commit, working-tree state, binary hash, and process output. This sampled local scenario is
not an exhaustive linearizability proof or a multi-host/power-loss test.

For manual operation, use the three-terminal setup in increment 16 and add `-configuration`
to **every** peer, with new journal paths. Send GET/PUT requests to the elected leader's
client port. `-peer-listen` is an optional loopback backend bind used by the test relays;
without it each peer binds its advertised membership origin directly.

Next: make gateway configuration consumption survive controller leader changes, and address
read/log growth before sustained polling. Draining still needs gateway application and
active-request accounting; committing desired drain state is not a completed fleet drain.
