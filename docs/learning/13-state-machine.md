# Increment 13: make configuration decisions independent of storage

`internal/control/machine.go` now owns validation, expected-revision checks, successful
request-ID history, configuration updates, and result copying. `store.go` owns serialization,
exclusive file access, checksummed journal framing, synchronization, and recovery. HTTP and
gateway snapshot consumers keep their existing API contracts.

```text
HTTP update -> serialized Store -> read-only prepare -> append + sync -> Machine.Apply
Journal     -> validate frames  -> Machine.Apply     -> recovery sync -> publish Store
```

The machine accepts ordered typed commands, returns a snapshot or an error, and uses no
clock, random generator, network, filesystem, or goroutine. It requires callers to serialize
access. Identical valid command histories must yield identical results and state. Input and
output worker slices are copied, so a caller cannot change state or retry history outside
an ordered command.

## Understand the command history

| Command | Result | Current configuration revision |
| --- | --- | --- |
| Drain, expected 0, ID A | Success at revision 1 | 1 |
| Identical retry A | Original result at revision 1 | 1 |
| Resume, expected 0, new ID B | Conflict | 1 |
| Resume, expected 1, new ID C | Success at revision 2 | 2 |
| Identical retry A | Original draining snapshot at revision 1 | 2 |

Only successful new updates advance the configuration revision and reserve a request ID.
Failed commands do not create deduplication entries. A retry of an old success returns its
original result; it does not rewind current state. Invalid UTF-8 in direct typed callers is
rejected so strings can round-trip through the JSON journal representation. Ordinary decoded
HTTP JSON follows the same validated command path.

## Why this is preparation for Raft

The current single-writer store can prepare while holding its mutex, then sync and apply
without another command intervening. That is not sufficient for a replicated leader:
multiple proposals can be in flight, so expected-revision decisions must be made in committed
apply order. Leader-side preflight is not the authoritative CAS result.

A future Raft log index and configuration revision are different counters. A committed retry
or failed CAS can occupy a log index without creating another configuration revision. Raft
must deliver the same committed order to each machine; deterministic execution alone does
not establish consensus, durable replication, election safety, or linearizable distributed reads.

`Machine.Snapshot()` returns the public current fleet view, not a complete Raft checkpoint.
A future checkpoint also needs successful request-ID history and applied-log position to
preserve retries across compaction. Current recovery rebuilds history from the entire journal.

## Recovery barrier

An append may survive a failed or unacknowledged sync. The store now syncs the recovered
complete journal before publishing reads or cached retry results. Otherwise a retry could
receive success for a recovered record that had never passed a successful synchronization
barrier. An incomplete tail is already synced as part of repair. Sync failure during recovery
rejects Open. Disk/power-loss and initial directory metadata limitations remain unchanged.

## Verify

```powershell
go test ./internal/control -v
go build -o bin/controller.exe ./cmd/controller
go build -o bin/gateway.exe ./cmd/gateway
python scripts/check_controller.py
python scripts/check_config_follow.py
```

Use the local Go path from earlier walkthroughs if needed. Tests check explicit command
outcomes, identical replay into independent machines, input/output isolation, a seeded
300-command comparison with the journal-backed store and repeated reopening, and a literal
legacy journal record. Existing crash, incomplete-tail, corruption, uncertain-write, and
concurrent-CAS tests continue to run. A regression test requires recovery to propagate an
injected sync error. Injected failures are not evidence of physical disk failure.

Executable checks verify actual controller crashes/retries and real-model drain/expiry
behavior after the separation. They do not turn independent in-memory machines into a
replicated cluster. The journal format is unchanged by this refactor.

Next: build a small Raft election core with explicit term/vote state and a deterministic
transport/clock harness. Integrate replicated writes only after election, persistence,
replication, and commit-rule checks are established.
