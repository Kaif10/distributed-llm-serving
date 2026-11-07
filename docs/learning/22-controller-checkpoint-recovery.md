# Increment 22: recover a controller from its published application checkpoint

The codec and publication journal now connect to the running configuration controller.
Checkpointing is opt-in and manual. The full Raft log is retained; no prefix is removed.
This closes the capture/recovery integration before changing retention behavior.

```powershell
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/replicated_config_check.py bin/raftpeer.exe --checkpoints --output artifacts/replicated-config/checkpoint-run.json

go build -o bin/gateway.exe ./cmd/gateway
python scripts/check_controller_failover.py --checkpoints --output artifacts/controller-failover/checkpoint-run.json
```

The first runner uses three actual Raft peer processes with controlled loopback relay
partitions and compares restored application images with full replay. On Linux use the
binary without `.exe`. The second uses the installed pinned Windows CPU model and checks
gateway freshness/real stream behavior through a checkpoint-backed peer restart.

## Enable and capture

Add these to each configuration peer, keeping its member ID, group label, and paths stable:

```powershell
-configuration -checkpoint-group fleet-local -checkpoint-journal artifacts/peers/a.checkpoint.wal
```

Use a distinct publication path per member, separate from its Raft journal. Both checkpoint
flags are required together and are rejected outside configuration mode. Omitting them
preserves full-replay behavior. The group label scopes the publication file; it does not
authenticate or durably bind the Raft membership configuration.

POST an **empty body** to that peer's client `/checkpoint` endpoint. The request goes through
the same serialized owner as Tick, Step, proposals, and application. It first applies newly
committed entries, requires a nonzero fully applied committed prefix, captures the machine
plus exact boundary index/term, and publishes the resulting bytes through the synced journal.
Success returns publication metadata. An identical latest-image capture is idempotent.

A follower may capture a previously confirmed local prefix. Capture success is neither a
fresh configuration read nor proof of current leadership. Use quorum-confirmed GET `/config`
for fresh state. The checkpoint API accepts no caller-supplied state, position, or file path.
It shares the existing bounded loopback client listener and event queue.

## Restart selection and application

```text
open full Raft journal + scoped checkpoint publication journal
    -> select/validate image
    -> compare complete image with retained-prefix replay
    -> hold image pending; application state remains uninstalled
    -> Raft re-establishes commitment through selected index
    -> recheck boundary and complete-prefix match
    -> install selected machine and applied index
    -> replay committed entries after that index
    -> serve reads only after fresh majority confirmation and application
```

The comparison includes all successful request-ID results, not only the current worker
snapshot. A valid self-contained image with a matching index/term but different history
is rejected. Missing boundaries, wrong entry terms, member mismatches, invalid publication
records, and changed prefixes fail closed. A pending image is checked again when commitment
returns; computing a verification machine does not expose it as applied configuration.

The owner skips entries already represented by the installed image while advancing the
core's delivery cursor. New committed suffix commands retain normal CAS/retry outcomes.
Checkpoint restoration never changes Raft election term/vote. Fresh-read and gateway
maximum-view-age rules remain unchanged.

GET `/status` now includes optional `checkpoint` diagnostics:

- `published`: generation, owner labels, selected boundary, and application-image SHA-256.
- `recovery_pending`: a selected image awaits confirmed commitment.
- `recovery_verified`: startup's complete retained-prefix comparison passed; it does not
  mean application was installed yet.
- `recovery_index`: the checkpoint index actually installed in this process (zero for a
  full-replay startup). It remains the startup recovery index after later captures.
- `last_error`: a nonfatal publication-capacity error, if present.

Diagnostics are local. Metadata snapshots are copied before delivery to HTTP handlers.

## Evidence gates

The three-peer runner checkpoints B, kills/restarts every peer, and waits for all machines
to converge. B reports its checkpoint recovery index; A uses full replay from its empty
publication store. Capturing both at the same committed position must produce identical
application-image hashes, including the entire retry table. Old successful IDs still return
their original results; altered payloads conflict. Existing partition, quorum-read, CAS,
and unchanged-journal-during-reads checks remain in the scenario.

The real-model runner checkpoints the follower that is later killed during quorum loss.
An active real generation finishes after the gateway's view expires. Restarting that peer
must install its checkpoint after commitment, restore quorum, let the gateway reconfirm the
same revision, and allow another real generation. These are same-host process checks.

Targeted Go tests hold an image pending at commit index zero, restore it after confirmation,
apply its suffix, reject same-state/different-retry-history images, revalidate a changed
prefix, check owner/member binding, and exercise HTTP capture/idempotency/metadata ownership.

## Costs and limits

Manual capture serializes encoding and publication I/O on the controller owner. Slow disk
work can delay protocol progress. This is a simple bounded correctness path, not a latency
or throughput optimization. Publisher I/O/invariant failures stop the owner, leaving Raft's
durable log for restart; publication capacity exhaustion returns 503 for capture while the
configuration service can continue. There is no periodic capture loop.

Startup still replays the full selected prefix for verification. No recovery-speed or disk
space improvement is claimed. The current 1024-entry Raft bound, eight-MiB application image,
and 16-generation publication bound remain. Controller installation is now connected, but
lagging-peer snapshot transfer, offset-aware logs, publication-journal rollover, security,
and multi-host/power-loss validation are separate work before compaction can be enabled.
