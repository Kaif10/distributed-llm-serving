# Increment 21: publish and select checkpoints across process crashes

This records the publication-store introduction. [Increment 22](22-controller-checkpoint-recovery.md)
connects it to opt-in controller capture/recovery; the journal contracts below still apply.

The checkpoint codec validates one application image. Recovery also needs to know which
image was published, whether a newer append finished, and whether the file belongs to the
expected group/member. `internal/checkpoint` now provides that publication store. It is a
separate library; serving Raft recovery does not consume it or trim log entries yet.

```powershell
go test ./internal/checkpoint -v
python scripts/check_checkpoint_publication.py --output artifacts/checkpoint-publication/run.json
```

If Go is not on PATH, pass `--go` with its executable path. The recorder captures source,
working-tree state, Go/platform, commands, native test events, and crash outcomes. Tests
kill actual child processes at controlled I/O boundaries using real files. Fault controls
are private to the package and configured only by tests. There is no inference traffic.

## One publication record

Each append contains the entire canonical application checkpoint plus:

- Kind `fleet-publication-v1` and an increasing generation.
- Operator-supplied group/member labels, checked on every recovered record.
- Applied index/term matching the embedded application checkpoint.
- A SHA-256 checkpoint digest inside an outer checksummed frame.

The journal has one exclusive OS lock. Publication appends the complete frame, calls file
Sync, then updates the selectable in-memory generation and returns success. Caller-owned
buffers are copied. Selection and publication are serialized by a mutex. An identical retry
of the latest image returns its generation without another append. An older image cannot
replace a newer selection.

Positions advance in index and cannot regress in term. A newer image must preserve every
earlier retained successful request's original update/result. Reverting configuration,
dropping IDs, or rewriting payloads is rejected before I/O. Application retry history is
separate from publication-generation idempotency.

The journal is self-contained. It does not scan standalone checkpoint files or select a
file by a high index in its name. An unreferenced file beside it cannot publish that image.
Full-image appends avoid a multi-file selection transaction in this increment, at the cost
of retained journal space and recovery work.

## Recovery and uncertain outcomes

Open validates every complete record's framing, canonical encoding, identity, generation,
digest, position, application image, and successful-history continuity. An incomplete final
header/payload is trimmed to the last complete record. Complete corruption or invalid
metadata stops Open; it does not silently select an older generation. Recovery syncs the
file before exposing any selected image, including after tail repair.

A write or sync failure fences reads and writes until close/reopen. Returning an old cached
image after an uncertain append could hide a newly persisted generation. Capacity and input
validation errors leave known selected state available because no I/O was attempted.

A missing acknowledgement is not proof that a new image was lost. The recorded process
kill outcomes are:

| Kill boundary | Recovered selection | Retry of candidate |
| --- | --- | --- |
| During a partial frame write | Prior generation 1 | Publishes generation 2 |
| Complete append, before Sync | Generation 2 after recovery Sync | Returns generation 2 |
| After Sync, before Publish returns | Generation 2 | Returns generation 2 |
| After Publish returns successfully | Generation 2 | Returns generation 2 |

These observations concern process termination with kernel/filesystem state retained.
Before-Sync recovery does not establish power-loss survival. Other tests inject short
writes, write/sync failures, and recovery-barrier failures; they do not reproduce a
physical device fault.

## Bounds and trust

The publication journal allows 16 generations, each at most 12 MiB including JSON/base64
framing. The application image retains its eight-MiB/1024-success limits. An identical
latest-image retry remains allowed at capacity; new images get `ErrFull`. No old image or
successful request ID is silently deleted. Safe rollover/retention is still needed before
continuous checkpoint publication.

Group/member labels are durable scope checks, not authentication or a consensus certificate.
The caller must capture state and applied position together under the owner after applying
a committed prefix, and bind a selected image to trusted Raft group/log state before
installation. The boundary term must never replace current election term/vote. Empty
journals have no selected image or established record identity until the first publication.

File Sync and same-host process kills are the exercised storage contract. Directory-entry
durability, power loss, disk loss, and malicious filesystem changes are outside that
evidence. The store does not delete or rewrite the existing Raft journal.

## Checks and next integration

Tests cover identity mismatch, exclusive ownership across processes, exact-image retries,
independent caller buffers, position/state/history regression, incomplete headers/payloads,
complete corrupt/invalid records, fencing, recovery barriers, unreferenced files, capacity,
and concurrent selection/publication. The recorder disables test caching and checks that
all four kill outcomes and the second-process lock check ran.

Next: wire committed-prefix capture and selected-checkpoint validation into controller
recovery while retaining the full Raft log. Then add offset-aware log storage, lagging-peer
snapshot installation, and publication-journal rollover before enabling continuous
checkpointing and networked log compaction. The current serving recovery path and log
capacity limit are unchanged.
