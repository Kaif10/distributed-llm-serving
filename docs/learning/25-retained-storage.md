# Increment 25: recover a checkpoint and log suffix together

The previous increment separated absolute Raft indices from retained slice positions.
This increment adds an alternative local storage adapter, `raft.RetainedStorage`.
It implements `SnapshotSource`, but is not wired into `raftpeer` or the serving host.
The existing runtime gates still reject nonzero retained boundaries.

The problem is concrete: replacing a prefix must never select a new application image
with an old, incompatible suffix after a crash. The existing append-only full-log WAL
also grows with lifetime writes even though the live log has a fixed bound.

## Storage layout and write ordering

A dedicated directory contains three files:

| File | Purpose |
| --- | --- |
| `owner.wal` | Stable exclusive lock and immutable format/group/member identity |
| `image-0.wal` | One complete generation |
| `image-1.wal` | The other generation, or an interrupted replacement |

Each image contains the same generation's hard state (term/vote), checkpoint boundary
and bytes, and retained suffix. A length/checksum frame surrounds canonical JSON.
Generation numbers select the newest complete image; they are local storage versions,
not Raft terms or application revisions.

To publish, the adapter clears the older slot and syncs that clearance, writes one new
complete image into it, then syncs the file. Only then does it return success and change
the in-memory selection. The current slot stays untouched. The next publication switches
slots again. There is no rename, growing lifetime journal, or separately updated pointer.

Reopen requires a valid identity file and both slot files. It checks complete images and
their transition, selects the newest complete generation, clears interrupted bytes, and
syncs the recovered files before exposing state. Two complete generations must be
consecutive and have compatible term/vote/boundary transitions. A complete corrupt frame
fails closed rather than silently rolling back a durable vote. An incomplete frame can
fall back to the previous complete image. Uncertain write/sync errors fence all use until
close/reopen; validation errors before I/O do not fence the store.

Each slot has a 24-MiB encoded-payload limit plus its 12-byte header. The identity frame
is at most 1036 bytes. Thus these files have a fixed logical-size bound below 49 MiB,
independent of the number of writes. This is a bound on file lengths, not a measurement
of physical filesystem allocation or peak process memory. Full-image encoding and two
syncs per publication remain expensive; no write-throughput improvement is claimed.

## One recovery trace

The application test commits a `drain` update at index 2, captures its real application
checkpoint, then commits `resume` at index 3. After the old node is no longer used, the
test owner retains the known committed prefix through index 2. The new generation holds
that checkpoint and entry 3 together.

On reopen, a new Raft node starts at boundary 2. The application restores the checkpoint.
A new-term leader no-op at index 4 confirms the suffix; the owner applies entry 3 once.
The resulting configuration matches full application replay. Retrying `drain` still
returns its original revision-1 draining result while the current revision stays 2.

`Retain(snapshot, applied)` requires the serialized owner to certify the local committed,
applied prefix and the exact application bytes. The adapter checks advancement, range,
term, and preservation of the suffix. It treats application bytes as opaque: it cannot
prove commitment, validate successful-request history, or accept an arbitrary remote
snapshot. Those checks belong to the application owner and future installation protocol.
Never retain behind a live node; reconstruct the node and restore the application before
using the new boundary. This increment exposes no live compaction API.

## Reproduce the checks

```powershell
go test ./internal/raft -run RetainedStorage -count=1 -v
go test ./...
go vet ./...
go build ./...
```

The process tests launch the actual Go test executable and kill only their own child
processes at staged file-publication barriers. They exercise real files and locks;
checkpoint bytes in the process tests are explicitly opaque fixtures. The separate
application test uses the real checkpoint codec and original retry results.

| Killed process stage | Selected state on reopen |
| --- | --- |
| Older slot cleared | Previous complete generation, full old suffix |
| Partial header or body written | Previous complete generation, full old suffix |
| Complete frame written before sync | New complete generation, synced during reopen |
| Synced before returning | New complete generation |
| Publication acknowledged | New complete generation |

The last acknowledged suffix and durable vote survive every staged case. Additional
checks cover a second process failing to acquire ownership, conflicting votes, uncertain
vote publication, short writes, failed recovery sync, identity mismatch, missing files,
complete corruption, generation gaps, byte ownership, and repeated slot reuse without
append-only growth.

A local core test also reproduces leader-no-op failure with 1024 no-op entries, replaces
that known committed prefix offline, reopens, and commits index 1025. This is a storage/core
fixture, not 1024 acknowledged application writes or a repaired live controller benchmark.

## Limits and the next integration gate

The fault model here is process termination with the operating system/filesystem still
running. File sync is required, but directory-entry durability, machine power loss,
device failure, arbitrary truncation of acknowledged files, and independent-host failures
are not established. Interrupted first-time bootstrap or missing/corrupt ownership
metadata fails closed; it never automatically erases existing files to start over.

The original full-log WAL is unchanged. No conversion of existing journals, network
snapshot transfer, live prefix removal, or publication-journal rollover is implemented.
The application still retains all successful IDs and has its existing codec bounds;
bounded Raft files do not establish unlimited application lifetime.

Next connect owner-validated application restoration and local retention without losing
original retry results. Add lagging-peer snapshot installation before enabling prefix
removal in a multi-peer service. Then rerun the controller capacity and real-model failure
checks against that integrated path before recording the final demo.

Exercise: why must reopen preserve a complete but unacknowledged vote? Why would choosing
the older image after a checksum failure risk voting twice in one term?
