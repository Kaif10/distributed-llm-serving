# Increment 11: durable, versioned configuration

Gateway drain flags currently live in memory. A separate controller now stores desired
fleet configuration and ordered revisions on local disk. This walkthrough describes storage
independently; [the next increment](12-applied-configuration.md) now connects optional
controller-managed gateways for fixed-pool drain application.

```powershell
go run ./cmd/controller -listen 127.0.0.1:19090 -journal artifacts/controller/fleet.wal
curl.exe http://127.0.0.1:19090/config
curl.exe -X PUT -H "Content-Type: application/json" --data-binary "@examples/config-drain.json" http://127.0.0.1:19090/config
```

Use the local Go path from earlier walkthroughs if needed. A new journal starts at revision
0. The example fully replaces desired configuration at revision 1; it is not a patch.
New updates need the current expected revision and a new request ID. The unauthenticated
listener is restricted to loopback, with Windows and Linux file locking supported.

## Concurrency and retries

`expected_revision` is compare-and-swap. A new update with a stale revision receives 409.
One mutex serializes duplicate checking, persistence, state installation, and reads.
Identical retries of `request_id` return the original snapshot and revision, even after
newer updates. They never reapply the old configuration. Reusing an ID for different fields
receives 409. Worker order and expected revision participate in update identity.

A timeout can hide a successful update. A complete append may survive even without an
acknowledgement. Recovery preserves valid complete records. Retry the same ID to learn its
result; GET /config returns the latest state, which can differ from an older retry result.

## Persistence contract

```text
validate -> check ID/revision -> append frame -> file sync -> install -> reply
restart  -> lock journal -> replay complete frames -> repair incomplete tail
```

Records have a bounded length, payload CRC32, header CRC32, and JSON. Success follows
File.Sync. Append/sync errors fence both reads and writes until reopening, returning 503
because the last outcome may be uncertain. Complete records with bad checksums reject
startup. Only incomplete final headers/payloads are truncated to the last complete record
and synced before accepting more writes. Checksums detect corruption; they are not authentication.

An OS lock excludes another controller opening the same journal. It releases automatically
on process/handle exit; no stale ownership file requires manual deletion. This does not
protect against arbitrary external file modification.

There is no compaction, deduplication expiry, or on-disk format migration yet. Disk/history
use grows. Limits are 128 workers, 128-byte IDs, and one-MiB requests/records. This is a
single local store, not replicated state. Process-crash recovery is checked; power loss,
disk loss, initial file-creation metadata durability, remote filesystems, and multi-machine
durability are not established.

## Reproduce the evidence

```powershell
go test ./internal/control -v
go build -o bin/controller.exe ./cmd/controller
python scripts/check_controller.py
```

The executable check sends real HTTP updates, rejects a competing controller, forcibly
kills/restarts after acknowledgements, checks retries, and races eight CAS writers
(one success/seven conflicts). It appends an incomplete tail while stopped, checks repair,
then acknowledges another update and kills/restarts again. Journals are isolated temporary
test files; raw results go to artifacts/controller-check/.

Storage tests cover every incomplete byte-prefix of a trailing record, both checksum
failures, concurrency, old retries, ownership, and an injected sync error that fences the
store and recovers an uncertain complete entry. Injection is not real disk-failure evidence.
No model requests are needed for this storage-only check; inference routing remains separate.

Next: gateways consume snapshots, expose applied revisions, and expire stale views. Controller
acknowledgement, gateway application, and completed draining remain different events. Raft
will replicate this state machine after the single-controller contract is understood.
