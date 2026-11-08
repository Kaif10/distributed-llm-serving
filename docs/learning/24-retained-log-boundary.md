# Increment 24: keep absolute Raft indices after a checkpoint

The capacity experiment exposed a concrete failure: a full log prevents a restarted
leader from appending its required no-op. A checkpoint currently preserves application
state but does not free log space. Before removing any entries, the core must understand
that an entry's absolute index is different from its position in the retained slice.

This increment changes that bookkeeping in the Raft core. It does not remove files or
enable compaction in the running controller.

## One recovery trace

Suppose a coherent durable image represents the committed prefix through index 1500,
term 2. Its retained log is empty. The node starts with commit/applied index 1500 and
knows the term at that boundary. A successful election appends its no-op at 1501;
the next command is 1502. Neither entry is renumbered to 1 or 2.

After reopening, only the boundary is initially known committed. The retained suffix
must be confirmed again before delivery. The application must restore the boundary
image before applying that suffix. `DrainCommitted` delivers suffix entries only.

`SnapshotSource` describes the future storage adapter's contract: hard state, snapshot,
and retained suffix must come from one coherent durable selection, read with exclusive
ownership. A nonzero boundary certifies a committed prefix. An independently published
application checkpoint cannot certify that by itself. The core treats image bytes as
opaque; application validation and atomic file publication remain adapter/runtime work.

## When a follower is behind

A follower with a later retained boundary can reject an earlier append with its boundary
index and term. The leader verifies that pair against its own log before advancing the
next probe. This hint never counts as a replication acknowledgement or advances commit.

In the other direction, a leader cannot send entries it has removed. It identifies that
peer through `PendingSnapshotPeers` and suppresses the unsupported append. It neither
fabricates missing entries nor counts the peer toward new commits. Snapshot transfer is
required to make progress with that peer; it is not implemented here.

## Reproduce the checks

```powershell
go test ./internal/raft -run Retained -v
go test ./internal/raftnet -run Retained -v
go test ./...
go vet ./...
go build ./...
```

The retained-boundary cases use deterministic in-memory storage and messages. They cover
absolute-index recovery, suffix delivery, fresh-read gating, conflict repair, committed
entry protection, non-acknowledging hints, lagging peers, independent image bytes, invalid
metadata, and index-overflow rejection. They do not establish disk-crash recovery or
network snapshot installation. The existing process checks still exercise the original
full-log path.

Both configuration-host construction and the raw host run loop reject nonzero retained
boundaries. These gates prevent the service from skipping application restoration while
the integration is incomplete. Existing native storage uses a zero boundary and its
format is unchanged. The entry/payload limits and measured full-log restart failure
remain unchanged.

Next: implement coherent durable snapshot-plus-suffix storage, exercise interrupted
publication/reopen, then connect application restoration and lagging-peer installation
before enabling prefix removal. Preserve original successful retry results throughout.

Exercise: why is a matching boundary hint insufficient to acknowledge the leader's
new term entry? Which state must survive together if a process dies during publication?
