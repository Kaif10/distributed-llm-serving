# Increment 27: recover a lagging peer after prefix retention

Three-member configuration groups can now use `-retained-dir` and the same
`-retained-group` on every peer, with a separate directory and member ID per process.
The existing `POST /retain` remains a manual leader operation on a fully applied
committed prefix. Default `-journal` mode stays available for the original full-log path.

When a follower's next index is below the leader's retained boundary, the leader sends
an `install_snapshot` message with its committed application image, group, term, and
correlated probe. The receiver validates the real checkpoint, its boundary, and every
successful request/result it has already applied. It rejects a group mismatch before
processing a term change. Group labels are not authentication; transport remains trusted
loopback HTTP with fixed membership.

The core ignores an already committed image. For a newer boundary, it preserves a local
suffix only if the boundary index/term matches; otherwise the uncommitted suffix is
discarded. The storage adapter publishes the image and compatible suffix as one synced
generation. An explicit installation marker distinguishes this valid remote transition
from local prefix trimming during recovery. Normal term/log publications clear that marker.

The owner installs the validated application before emitting the snapshot acknowledgement.
Only the exact current probe and boundary acknowledgement advance the leader's replication
estimate. The leader then sends ordinary suffix entries. Lost replies can safely cause
retransmission: a committed duplicate does not republish, reapply, or roll back state.
An uncertain sync fences the recipient and produces no successful acknowledgement.
Reopen selects/syncs the complete generation before another vote or read is possible.

The protocol sends one bounded complete image rather than chunks. The peer HTTP body
limit is 12 MiB to carry the codec's eight-MiB payload plus base64/metadata. This is a
bounded educational protocol, not a large-model-weight transfer mechanism. Tensor data,
tokens, and KV caches are never replicated by this controller.

```powershell
go test ./...
go vet ./...
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/check_replicated_retention.py bin/raftpeer.exe --output artifacts/replicated-retention/run.json
python scripts/check_controller_failover.py --retention --output artifacts/controller-failover/retained.json
```

The native three-peer check stops a follower, commits changes with the remaining majority,
retains the leader prefix, and restarts that follower. It requires an installed image and
new suffix to reach revision 6. It then kills the leader, checks historical revision-1
retries, restarts the entire group, and confirms the same configuration/history. A wrong
group probe is rejected without changing the recipient's term.

The real CPU-model check kills the controller leader, applies drain/resume, transfers the
retained image to the old leader, and restarts that installed image after quorum loss.
The gateway expires its view and rejects new work while an existing real generation
finishes; confirmed revision 3 and new generation recover with quorum. The original
revision-1 update still returns its original result.

Core tests cover duplicate/lost acknowledgements, false or stale probes, byte ownership,
suffix delivery, installation sync failure, reopen, and normal term persistence after an
installed generation. Owner tests reject incoming loss of successful request history.
Linux CI runs race tests and the three-process scenario; real-model checks require the
pinned local assets and run separately.

The two-slot mode owns both checkpoint and suffix generations, so it does not use the
older 16-generation independent publication journal or require a separate rollover loop.
That journal remains an explicitly bounded comparison mode. Manual retention, the
1024-successful-request/eight-MiB codec bounds, no dynamic membership, and single-host
validation remain limitations. Retain before suffix exhaustion. There is no migration
tool for existing full journals, automatic ID eviction, or claim of production readiness.

Exercise: why does a snapshot acknowledgement prove only its retained boundary rather
than the leader's next command? What happens if the receiver syncs its new image and
dies before replying?
