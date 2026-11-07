# Evidence ledger

The serving gateway routes real-model streams across a fixed worker pool, with admission,
health, draining, and controller-snapshot freshness checks. Real CPU-model experiments and
their limitations are recorded below. The serving controller has a durable single-writer
configuration journal. Raft elections, replication, peer-process transport, and a replicated
configuration API with fresh quorum-confirmed reads are implemented. Reads no longer append
log entries. Gateway controller-peer failover is implemented and checked with real CPU
inference. A complete application checkpoint codec is validated in an offline recovery
scenario. A scoped publication journal now survives tested process-kill boundaries and
selects complete images after recovery sync. Configuration peers can opt into owner capture
and checkpoint-plus-suffix recovery verified against full replay. Write retention, snapshot
transfer, scheduling, and conditional sharding remain outstanding. Historical entries describe their original increments rather than current status.

## Controller-owned checkpoint capture and confirmed-prefix recovery

Source: clean commit `6590ecd06e69471a53aed268c1e527ab4a858d19`. Reproduce:

```powershell
go build -o bin/raftpeer.exe ./cmd/raftpeer
go build -o bin/gateway.exe ./cmd/gateway
python scripts/replicated_config_check.py bin/raftpeer.exe --checkpoints --output artifacts/replicated-config/checkpoint-6590ecd.json
python scripts/check_controller_failover.py --checkpoints --output artifacts/controller-failover/checkpoint-6590ecd.json
```

Both raw runs record the same clean source. The
[three-peer run](results/controller-checkpoint-6590ecd.json) uses actual peer processes,
retained journals, and controlled loopback relay faults without inference traffic. The
[real CPU-model run](results/controller-checkpoint-real-6590ecd.json) uses pinned llama.cpp
b12110 and checksum-verified SmolLM2-135M Q4_K_M, actual gateway/peer processes, two engine
threads, Windows amd64, Go 1.27.3, and Python 3.12.7. Commands, executable hashes, model pins,
responses, selected checkpoint metadata, and recovery status are recorded.

The peer run captured B's application at index 8/term 2, revision 3, publication generation 1.
After killing/restarting every peer, B reported confirmed recovery index 8 with pending=false
and verification=true. A's empty publication store used full replay. Capturing A and B at the
same committed index 9/term 3 produced identical complete application-image SHA-256 values,
`06ac1906ec6331f0177012c5e799ae88c19ee87c67f8f73e4444f839d03213ac`.
A published generation 1 and B generation 2. The comparison includes historical request-ID
results, not just worker state. Subsequent original-ID retries and altered-ID conflicts
remained correct. Existing partition, quorum-read, CAS, and read-journal-size checks passed.

In the real-model run, C captured generation 1 at index 5/term 2 after revision 3 applied.
C was then killed, removing the majority while another real generation was active. The
gateway view expired and readiness/new work were rejected; the admitted 1536-token-budget
stream finished with finish_reason=length and [DONE]. Restarted C verified/installed its
checkpoint at recovery index 5 after commitment returned. The gateway reconfirmed revision 3
and a further real generation completed. These remain same-host process observations.

Implementation: optional `-checkpoint-journal` and explicit `-checkpoint-group` flags in
configuration mode. POST `/checkpoint` accepts an empty body and captures through the
serialized owner after applying the committed prefix. It stores the exact applied index/
entry term and complete machine image through the synced publication journal. The selected
image stays uninstalled until Raft confirms commitment through its position. Startup and
installation compare the entire image against retained-prefix replay; a matching index/term
alone is insufficient. Only committed entries after the installed boundary reach application.
No-op, CAS, retry, fresh-read, and Raft term/vote behavior remain separate and unchanged.

Go tests check pending-image isolation at commit index zero, suffix application, owner/member
binding, missing/wrong boundaries, identical current state with different retry history,
revalidation after an unconfirmed prefix changes, HTTP capture/idempotency, response metadata
ownership, and unavailable-publisher errors. Local `go test ./...`, `go vet ./...`, and
`go build ./...` passed. [Linux CI](https://github.com/Kaif10/inference-fleet/actions/runs/42425470768)
passed formatting, vet, race tests, builds, the offline checkpoint demo, gateway smoke,
raw peer recovery, and configuration runs both with and without controller checkpoints.
Local Windows race detection was not run. The real-model run is separate from Linux CI.

Costs/limits: full retained-prefix replay is still performed for verification, and the full
Raft log remains mandatory. No startup speedup, log-space reduction, compaction, or lagging-peer
snapshot-transfer claim is established. Manual capture performs bounded encoding/publication
I/O on the controller owner; slow disk work can delay progress. Uncertain publisher I/O stops
the owner; publication capacity rejects capture while allowing configuration service to
continue. Current application/publication/log bounds, trusted-loopback assumptions, and
single-host failure domain remain. See the [walkthrough](learning/22-controller-checkpoint-recovery.md).
The final working-demo video is a planned deliverable, not yet recorded.

## Checkpoint publication journal and actual process-kill selection

Source: clean commit `40bfb2c9ed102c83784c49fcd43c445d028ec40a`. Reproduce:

```powershell
go test ./internal/checkpoint -v
python scripts/check_checkpoint_publication.py --output artifacts/checkpoint-publication/run.json
```

Use `--go <executable>` if Go is not on PATH. The
[recorded process run](results/checkpoint-publication-40bfb2c.json) contains the actual command,
clean source commit, Go 1.27.3 Windows amd64, Python/platform, uncached Go JSON events,
all four kill outcomes, and the second-process exclusive-owner check. Fault boundaries
are controlled by private test hooks, but writes, Sync calls, OS locks, process termination,
and recovery use actual files/processes. No inference traffic or live Raft checkpoint
installation is part of this check.

A child process opened a journal already containing generation 1 and tried to publish a
complete newer application image as generation 2. Killing it during a partial frame write
recovered generation 1; retrying the candidate published generation 2. Killing after a
complete append but before Sync recovered generation 2 after the recovery sync barrier.
Kills after Sync but before Publish returned, and after Publish returned, also recovered
generation 2. Every candidate retry then returned generation 2. These are process-crash
observations with kernel/filesystem state retained; before-Sync recovery is not proof of
power-loss durability. A complete unacknowledged append has an unknown outcome until reopen.

The new separate publication store checks canonical framing, checksums, group/member labels,
increasing generation and applied position, checkpoint digest, and application-image validity.
A later checkpoint must retain every prior successful request's original result. Exact
latest-image retries append nothing. Recovery repairs incomplete final headers/payloads,
fails closed on complete corruption/invalid metadata, and syncs before exposing selection.
Write, short-write, or sync errors fence reads/writes until reopen; simulated failure tests
exercise those branches. An unreferenced checkpoint file cannot change journal selection.

Other Go tests cover wrong group/member, position/config/history regression, idempotency,
caller buffer independence, recovery-sync failure, invalid complete metadata, capacity,
and concurrent readers/publication. Local `go test ./...`, `go vet ./...`, and `go build ./...`
passed. [Linux CI](https://github.com/Kaif10/inference-fleet/actions/runs/42423071759) passed
formatting, vet, race tests (including real publication-process kills and concurrent
selection), builds, the offline checkpoint demo, gateway smoke checks, and both protocol
process scenarios. Local Windows race detection was not run.

Limits: 16 full-image generations, at most 12 MiB per outer record, and the existing
application-codec limits. New images return `ErrFull` at capacity without eviction; retrying
the latest image still succeeds. Publication-journal rollover is outstanding. Labels do not
authenticate a Raft group or prove capture at a committed boundary. Controller-owned capture,
selected-image binding/installation, offset-aware log recovery, and lagging-peer snapshot
transfer remain separate prerequisites for compaction. The live controller/gateway path and
Raft log bound are unchanged. No directory-entry/power-loss, disk-loss, multi-host, or model
performance claim is added. See the [publication walkthrough](learning/21-checkpoint-publication.md).

## Complete application checkpoint and offline suffix recovery

Source: clean commit `3187117cb4c69cfc491dcc458f6c1f8c33aa3bac`. Reproduce:

```powershell
go test ./internal/control -run Checkpoint -v
go run ./cmd/checkpointdemo -output artifacts/checkpoint/3187117.json
```

The [raw transcript](results/application-checkpoint-3187117.json) records a clean source
commit, Go 1.27.3, Windows amd64, checkpoint file size/hash, actual journal entries, and
restored states/results. The scenario uses one actual Raft core with explicit ticks and
real local journal/checkpoint files. It is an offline application-recovery check, not
networked snapshot installation or a new inference/model experiment.

After a leader no-op, drain, and resume, the machine reached revision 2 at applied log
position `(index=3, term=1)`. The synced, closed checkpoint was 492 bytes, SHA-256
`b3f2cb988f9d917a76ba58a868a5407e7c2d5012f3219a61043386bcb45f1cd8`.
It retained both successful IDs, including the original revision-1 drain result. The
subsequent suffix contained a successful retry, a stale CAS conflict, and one new update,
reaching revision 3 without changing the checkpoint file.

After closing/reopening the Raft journal, the demo refused to treat the checkpoint boundary
as confirmed while volatile commit knowledge was zero. A term-2 leader no-op re-established
commitment through index 7. The checkpoint boundary matched the trusted entry's term;
a deliberately wrong boundary term was rejected. Restoring the checkpoint and replaying
only the committed suffix after index 3 produced a canonical complete application image
identical to full replay through index 7, including the successful-request table. Both
machines reached revision 3. Retrying the original drain returned revision 1 without
rewinding current state; altered fields under that ID conflicted. Corruption was rejected.

The codec stores a versioned current snapshot, successful history, and owner-supplied
applied position in one canonical checksummed frame. It validates complete framing,
checksum, kind, revision/CAS order, duplicate IDs/keys, worker records, state/history
agreement, and canonical encoding, returning no machine on failure. It bounds wire payload
to eight MiB and retained successes to 1024 without silently evicting IDs. Checkpoint size
still grows with distinct successful requests. The codec does not certify commitment,
authenticate group identity, or prove the owner supplied the correct capture boundary.

Tests cover historical retry restoration, independent buffers, deterministic output,
every truncation point of a fixture, framing/semantic corruption with valid checksums,
unknown versions/fields, ambiguous encoding, invalid positions, and count/size bounds.
Twenty seeded histories compare complete execution with repeated checkpoint restore plus
tail execution. Local `go test ./...`, `go vet ./...`, `go build ./...`, and the file-backed
demo passed. [Linux CI](https://github.com/Kaif10/inference-fleet/actions/runs/42414242867)
also passed formatting, vet, race tests, builds, the file-backed checkpoint demo, gateway
smoke checks, and both protocol process scenarios. Local Windows race detection was not run.

The [walkthrough](learning/20-application-checkpoints.md) defines capture/publication and
compaction follow-ups. Live serving recovery remains unchanged and no log entries are
removed. The new offline file is not an atomically installed snapshot or a directory/
power-loss durability contract. Current Raft term/vote stay independent of the checkpoint's
boundary term. Checkpoint publication/recovery selection, offset-aware storage, snapshot
transfer to lagging peers, and successful-ID retention policy remain prerequisites for
live compaction. No change to earlier real-model performance or unresolved timeout evidence
is established by this control-plane preparation.

## Gateway controller failover with real CPU inference

Source: clean commit `e39b0f91f1517ee3e5d7e193dbd358217777bae3`. Reproduce with installed
pinned Windows assets:

```powershell
go build -o bin/gateway.exe ./cmd/gateway
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/check_controller_failover.py --output artifacts/controller-failover/run.json
```

The [raw run](results/controller-failover-e39b0f9.json) records the clean tree, source SHA,
actual commands, executable hashes, model pins/checksum, processor/platform, streamed
outputs, controller origins, applied revisions, expiry, and recovery observations. This
used Windows amd64, Go 1.27.3, Python 3.12.7, 12 logical CPUs, and two llama.cpp engine
threads. One real CPU SmolLM2-135M Q4_K_M worker ran under pinned llama.cpp b12110 alongside
three actual Raft peers and the gateway. No worker replacement or mock inference response
was used for this experiment; downloads and paid infrastructure were not involved.

A initially supplied confirmed revision 1. After A was killed, B/C elected C, and the
gateway switched its recorded source to C while reconfirming revision 1. The runner observed
610 ms from initiating the kill through seeing the changed gateway source. This is one
same-host trial, not a failover SLO or a latency benchmark. A drain committed through C
became applied revision 2 and rejected new inference with HTTP 503. Resume revision 3
restored generation. Real short streams completed before the failure, after failover,
and after quorum recovery with finish reasons and `[DONE]` recorded.

A real long generation (1536-token budget, `ignore_eos=true`) began before B was killed,
leaving C without a majority. The gateway view expired at revision 3; its existing stream
still held one reservation at expiry. Readiness returned 503. The stream subsequently
finished normally with `finish_reason=length` and `[DONE]`; no engine error event was
accepted by the checker. With the engine then idle and configuration still expired,
new inference also returned 503. Restarting B from its journal restored quorum, let the
gateway reconfirm revision 3, and allowed another complete real generation.

Implementation: `-controllers` accepts 1-9 configured peer origins, exclusive with the
legacy `-controller` option. Polls prefer the last successful source, fall back across
configured origins on request/status/validation failure, and never follow redirects.
One timeout budget is divided across the remaining candidates, avoiding a slow old leader
monopolizing a cycle. Only an accepted snapshot refreshes the view, with expiry measured
from that attempt's start. Diagnostic `controller_origin` updates with the applied snapshot.
A failed cycle preserves the last valid view/revision/source and exposes its error.

Go network tests cover a hung endpoint, follower status, successful-source preference,
source changes, rollback rejection without freshness renewal, expiry, same-revision
recovery, input bounds, and redirect refusal. Existing active-stream tests continue to
pass. Local `go test ./...`, `go vet ./...`, `go build ./...`, and gateway executable smoke
checks passed. [Linux CI](https://github.com/Kaif10/inference-fleet/actions/runs/42410422086)
passed formatting, vet, race tests, builds, gateway smoke checks, and both Raft/configuration
process scenarios. CI's protocol/network checks are separate from this Windows real-model
experiment; local race detection was not run.

The [walkthrough](learning/19-controller-failover.md) gives the setup and failure trace.
The list must refer to one replicated group with fresh-read semantics; cluster identity
is not authenticated. The list and worker membership are fixed at gateway startup.
Administrative writers still select a leader directly. There is one gateway/worker and
one shared host/disk in this check, with no multi-host or power-loss claim. The write log
still fills without retention/compaction; public snapshots omit successful request-ID
recovery data. Earlier unresolved real-model timeout evidence remains unchanged.

## HTTP read quorums without per-read journal growth

Source: clean commit `a39e8d537ae6acb03d272dd11681b96ad5a90b43`. Reproduce:

```powershell
go test ./internal/raftnet -v
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/replicated_config_check.py bin/raftpeer.exe --output artifacts/replicated-config/read-quorum.json
```

Windows 11 amd64, Go 1.27.3, Python 3.12.7. The
[raw transcript](results/read-quorum-a39e8d5.json) records the clean source commit, binary
hash, invocation, per-peer log/application state, and response outcomes. This uses three
actual peer processes and separate journals with controlled same-host HTTP relay faults.
No inference traffic or model performance measurement is part of this increment.

After B committed revision 1 and all peers applied it, 128 sequential fresh GET `/config`
requests returned that revision without changing terms or adding entries. Journal byte
counts remained A=620, B=550, C=620; each log stayed at two entries. All pending read counts
were zero afterward. These are journal-length/log observations, not syscall traces or
an inference/router throughput benchmark.

B was then isolated while its client listener stayed alive. A/C elected A, committed
revision 2, and served that newer revision. B still called itself leader but its read
returned HTTP 504 instead of stale configuration; its isolated update also timed out
without application. Healing, apply-time CAS, successful request-ID retries, and a full
kill/restart remained correct in this sampled run. All peers ended at revision 3, with
commit/applied indices both 11 and no pending reads. Followers refused fresh reads.

GET now allocates a fresh term-scoped core read context after request arrival. The owner
waits for distinct majority confirmation plus application through the captured committed
index, then clones its machine snapshot. Current-term commit gating prevents a new leader
from reading before re-establishing commitment. Old read-barrier commands remain replayable;
PUT still commits/applies in log order and successful retry semantics are unchanged.

Targeted tests cover 1,100 single-member HTTP reads with unchanged journal length/log,
a confirmed read held behind local application, legacy barrier replay, ten network client
cancellations exceeding the eight-context capacity, and live-read failure on higher-term
leadership loss. Cancelled contexts are reclaimed by the owner; success and shutdown also
release them. Runtime read waiters and core contexts remain bounded to eight each.

Local `go test ./...`, `go vet ./...`, and `go build ./...` passed.
[Linux CI](https://github.com/Kaif10/inference-fleet/actions/runs/42409310084) passed formatting,
vet, race tests, builds, gateway smoke checks, raw peer recovery, and the extended
configuration read/partition/replay scenario. Local Windows race detection was not run.

Limits: this removes read-driven growth, not writes/retries/conflicts/leader no-ops or the
1024-entry/four-MiB log bound. No compaction, durable application checkpoint, gateway leader
discovery, authentication/TLS, or multi-host/power-loss guarantee is added. A newly elected
leader can temporarily return 503 before its current-term entry commits; a live isolated
former leader's read can time out. The implementation uses fresh confirmations, not a
leadership lease. The [walkthrough](learning/18-read-quorum.md) describes the failure trace
and the next gateway-integration step. Earlier unresolved real-model timeouts are unchanged.

## Historical read-confirmation core preparation

The [next-increment plan](learning/18-read-quorum.md) defines a fresh majority-confirmed
read protocol to replace per-read appends. The core now exposes `BeginRead`, `ReadReady`,
and `ForgetRead`, with current-term commit gating, term-scoped context IDs, distinct voter
counting, heartbeat retransmission, and an eight-context bound. Higher-term leadership
changes and pre-restart replies cannot validate an old read. Core readiness still requires
the caller to wait for the captured committed prefix to be locally applied.

`go test ./internal/raft -run Read -v` checks prior-term commit rejection, duplicate and
wrong-context replies, stale/unknown senders, fresh IDs for later reads, retransmission,
restart/term fencing, explicit context release, context bounds, and no read acknowledgement
after a failed higher-term persistence barrier. Core read operations leave the inspected
log unchanged. The full local `go test ./...`, `go vet ./...`, and `go build ./...` passed.
These were deterministic core checks using retained model storage, not a new process/network
measurement. At that preparation step GET `/config` still used journal barriers. HTTP/runtime integration,
applied-index gating, cancellation cleanup, and repeated-read journal-size evidence remain
pending at that stage; the later integration and process evidence are recorded above.

## Replicated configuration application and read barriers

Source: clean commit `e27279297cfca4e1bdac5e039f408b987aedcbcc`. Reproduce:

```powershell
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/replicated_config_check.py bin/raftpeer.exe --output artifacts/replicated-config/run.json
```

Windows 11 amd64, Go 1.27.3, Python 3.12.7. The
[raw transcript](results/replicated-config-e272792.json) includes a clean working tree,
binary hash, invocation, peer logs, protocol entries, applied/committed indices, and API
outcomes. Three real peer processes use separate journals and HTTP fault-injection relays
on the same host. Relays deliberately drop messages across the selected partition; they
are not replacement Raft implementations. No model requests or inference measurements
are part of this increment. See the [walkthrough](learning/17-replicated-configuration.md).

B initially committed/applied a drain at revision 1. Its protocol traffic was isolated
while its client API stayed alive. A/C elected A and committed a resume at revision 2.
After that update returned, A's read returned revision 2; B still called itself leader
but its read returned HTTP 504. An isolated update also returned 504 and B's diagnostic
application state remained at revision 1, confirming no application of that uncommitted
update in this run. After healing, the isolated update's retry conflicted at revision 2.

Retrying the original successful drain ID returned its historical revision-1 result without
reverting current state. Two concurrent commands expecting revision 2 produced one HTTP 200
(revision 3, empty workers) and one HTTP 409. After killing/restarting all peers, their fresh
machines replayed revision 3. A fresh read returned revision 3; the original successful ID
still returned its original result. Altered fields under that ID returned 409. A follower
refused a configuration read with HTTP 503. Final commit/applied indices were both 13 on
all three peers, with identical logs and configuration.

Local `go test ./...`, `go vet ./...`, and `go build ./...` passed. Network tests additionally
exercise single-member committed reads, concurrent apply-time CAS, successful retry results,
malformed updates, raw-endpoint exclusion in configuration mode, and rejection of raw journals.
[Linux CI](https://github.com/Kaif10/inference-fleet/actions/runs/42399829760) passed formatting,
vet, race tests, builds, gateway smoke checks, raw peer recovery, and the replicated
configuration partition/replay scenario. Local Windows race detection was not run.

The optional `raftpeer -configuration` mode serves GET/PUT `/config`; the old single-writer
`cmd/controller` is unchanged. The machine applies only committed entries in order, skips
no-ops, and rebuilds successful request-ID results after recovery. Reads are actual replicated
commands, not local leadership checks or leases. Every read/retry/conflict consumes bounded
log space and synced full-image journal writes. Continuous gateway polling still needs a
read/log-retention design; no compaction, checkpoint, gateway leader discovery, remote
security, dynamic membership, or performance improvement is established. Fixed membership
and consistent application mode/journal ownership are operator requirements. Same-host
process kills and selected relay faults are not power/disk-loss tests, exhaustive histories,
or evidence of production readiness. Earlier unresolved real-model timeouts remain unchanged.

## Real-process Raft transport and recovery

Source: clean commit `ec03507df0df1cace731e35936845a402544617d`. Reproduce:

```powershell
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/raft_process_check.py bin/raftpeer.exe --output artifacts/raft-process/run.json
```

Windows 11 amd64, Go 1.27.3, Python 3.12.7. The
[raw process transcript](results/raft-process-ec03507.json) records a clean working tree,
binary SHA-256, exact invocation, process logs, acknowledged command bytes, and final peer
states. This used three actual OS processes and separate journal files on one machine,
with loopback HTTP. It did not use an in-process message simulator or send LLM traffic.
The [walkthrough](learning/16-raft-processes.md) explains the transport and client contract.

A became leader and acknowledged four commands. A client request in flight during A's
abrupt termination lost its HTTP connection. The B/C majority elected C in term 2 and
acknowledged another command at index 7. Restarted A caught up through committed index 7.
With A/B stopped, C's next proposal returned HTTP 504 (`deadline; outcome unknown`),
without falsely reporting commitment. Once the majority returned, this timed-out command
committed at index 8. After killing/restarting all peers, each re-established commitment
through index 9; all five explicitly acknowledged commands retained their exact index,
term, and bytes. The request interrupted by the leader kill did not appear in the final
log in this run. In general, failed HTTP delivery is not proof of rejection; the minority
timeout above demonstrates a failed response whose command later committed.

The runtime serializes core ownership, bounds application event/sender queues and handlers,
separates protocol and client listeners, and rejects malformed/oversized messages and
unknown claimed members. Network tests cover single-member quorum acknowledgement,
listener separation, peer validation, oversized proposals, and no-majority behavior.
Local `go test ./...`, `go vet ./...`, and `go build ./...` passed.
[Linux CI](https://github.com/Kaif10/inference-fleet/actions/runs/42273504664) also passed:
formatting, vet, race tests, builds, gateway process smoke checks, and the three-peer
process scenario. Local Windows race detection was not run.

Limits: trusted loopback only, no authentication/TLS, no configuration-machine application
or linearizable application reads at the peer API, no deduplicated raw proposals, no dynamic
membership or compaction. Fixed IDs and journal ownership must be preserved across restarts;
journals do not bind an authenticated cluster identity. Queue/handler limits do not bound
all HTTP connections or OS socket resources. These checks establish observed process-crash
recovery on one host, not arbitrary network schedules, power/disk loss, production readiness,
or LLM serving-controller failover. The unresolved model timeout in earlier evidence is
unaffected by this protocol-only increment.

## Isolated durable Raft replication and commitment

Source: clean commit `9667f93`. Reproduce with `go test ./internal/raft -v` and
`go run ./cmd/replicationdemo`. The [recorded transcript](results/replication-9667f93.json)
uses explicit ticks/queued message delivery with three real local journals and independent
configuration machines. It is not a peer-process network deployment or serving-controller
failover test. No LLM performance experiment is part of this protocol-only increment.

Initially A led term 1 and all nodes committed through index 2, reaching configuration
revision 1. Isolated A appended an additional proposal, but its commit index remained 2.
B/C elected C in term 2 and committed a no-op plus resume command through index 4, reaching
revision 2. After healing, A's conflicting uncommitted suffix was repaired; every node had
commit index 4 and revision 2. The isolated proposal was never applied. Reopening all journals
preserved entries and hard state while resetting volatile commitment; election of C in
term 3 committed its no-op at index 5 and fresh configuration machines replayed revision 2.

Targeted tests verify file-backed commit/delivery, command-buffer ownership, volatile delivery
cursor behavior, prior-term majority blocked until a current-term entry commits, unmatched
follower suffixes not being committed by shorter appends, conflict truncation, preservation
of committed prefixes, retransmitted probe identity, stale rejection handling, and no follower
acknowledgement after failed log persistence. A literal election-v1 fixture upgrades to v2,
preserves its log after incomplete append recovery, and cannot be opened by the election-only
constructor once log entries exist.

Twenty-five seeded five-node schedules exercise proposals, loss, duplication, reordering,
partitions, and restarts using retained model storage. They check historical uniqueness of
committed term/command by index, leader uniqueness per term, and progress after healing.
The prior fifty election schedules continue to pass. Sampled schedules are not formal proofs,
exhaustive interleaving coverage, or evidence of physical network/storage fault tolerance.

`go test ./...`, `go vet ./...`, the demo, and Linux CI passed, including race detection.
The new storage writes synced full hard-state/log images, with conservative bounds rather
than an optimized incremental WAL. Per-command limit is 64 KiB; total log is bounded by
1024 entries/four MiB payload. No compaction is implemented, so reaching the bound can stop
progress. Commit/application cursors are volatile; exactly-once external side effects and
durable application checkpoints are not provided. The serving controller is not yet connected
to this replicated core. Power/disk loss and dynamic membership remain outside tested scope.


## Isolated Raft election core

Source: clean commit `0c79b3c8641e5c633c6295d4c4509e7b50edb4ff`. Reproduce with
`go test ./internal/raft -v` and `go run ./cmd/electiondemo`. The
[recorded transcript](results/election-0c79b3c.json) contains the actual core statuses from
an explicit-tick, queued-message scenario using three real local vote journals.
This is not a network deployment, multi-process Raft experiment, or controller failover.

A became leader in term 1. While A was isolated, B/C elected C in term 2; A continued
believing it was leader in term 1. After message delivery healed, A persisted term 2 and
stepped down. Reopening all journals preserved terms/votes, while every node restarted as
follower with no remembered leader. No configuration command was replicated or applied.

Fifty seeded five-node transport-model runs check at most one elected leader per term
across loss, reordering, duplication, partitions, and restarts, then require convergence
after healing. Seed numbers are subtest names. These sample schedules with retained
in-memory hard state; they are not formal proofs or actual process/network fault injection.

Targeted tests cover strict majority and duplicate/stale response counting, split retries,
single-node election, current/stale heartbeat behavior, outsider filtering, log-tail ordering,
and persistence errors that return no outbound messages and fence the core. File-backed
tests verify a durable granted vote across reopen, denial of another candidate in that term,
new-term voting, conflicting-write rejection, writer exclusion, incomplete trailing frames,
header/payload corruption, and recovery of an uncertain complete vote. An additional local
case exercises failure while granting a vote in an already-persisted term.

Election storage uses separate format-tagged journals and syncs before Save returns and
before recovered state is published. OS-lock code was moved to a shared utility; controller
and election data/formats are not combined. The rebuilt controller's existing executable
kill/restart/CAS check passed after the move:
[regression results](results/election-lock-regression-0c79b3c.json). It reports pending
transcript artifacts in the working tree; runtime code was unchanged during that check.

`go test ./...`, `go vet ./...`, the demo, and Linux CI for 0c79b3c passed, including race
detection. This validates the current election slice only. Local file reopen/sync checks do
not establish durability under disk or power loss, and the isolated old leader has no
quorum-serving authority. The election heartbeat has no AppendEntries prefix validation.
Replicated logs, commit/application rules, real transport, distributed reads, checkpoints,
and controller integration are the remaining steps. The inference path stays on its current
single-controller configuration contract.


## Deterministic machine separated from persistence

Runtime separation: `beef146`. The public update/snapshot fields and journal envelope remain
compatible. Machine owns ordered validation, CAS, successful retry history, and state/result
copying; Store owns serialization, locking, append/sync, and recovery. No election, transport,
quorum, or replication protocol is implemented. Linux CI passed for beef146 and
0c01c21, including race detection.

`go test ./...` and `go vet ./...` passed. Tests cover explicit outcomes for new commands,
retries, conflicts, and empty fleets; replay into three independent machines; caller mutation
isolation; and a seed-7, 300-command comparison between two in-memory machines and Store,
with repeated reopening. A literal legacy journal frame still recovers its state and retry
result. Competing commands can both pass read-only preflight, but only the first succeeds
when applied in order; failed CAS does not advance the configuration revision.

At clean commit beef146, both executable checks passed: actual controller kill/restart,
CAS/idempotency/ownership, and real SmolLM2 generation through managed drain and stale-view
expiry. Raw results: [controller](results/machine-controller-beef146.json) and
[real-model integration](results/machine-inference-beef146.json).

Follow-up 0c01c21 rejects invalid UTF-8 typed fields that would otherwise normalize during
JSON persistence, making in-memory and replayed state differ. Tests require rejection without
changing state. Decoded HTTP JSON commands retain their ordinary semantics.

Follow-up 050b7f6 adds a recovery sync barrier: a complete record may survive an uncertain
write, so the recovered journal is synced before Open exposes state or cached retry results.
A regression test requires recovery to propagate a sync error. Existing incomplete-tail,
checksum, concurrent CAS, and uncertain-write tests pass. The
[post-barrier executable check](results/recovery-barrier-050b7f6.json) passed. That run reports
a dirty tree because prior result artifacts were pending commit, not because runtime code
changed during the run. Physical power/disk failure remains untested.

Configuration revision is not a future Raft log index: committed errors/retries can occupy
log positions without creating new configurations. Public Machine.Snapshot is not a full
replication checkpoint; retry history and applied-log position must accompany any future
compaction snapshot. Independent replay is not evidence of consensus or fault-tolerant replication.


## Gateway application and stale-view expiration

Source: clean commit `efce208`. Reproduce with builds of controller and gateway, then
`python scripts/check_config_follow.py`. The [raw results](results/config-follow-efce208.json)
include desired/applied revisions, freshness timestamps, worker state, engine pins, launch
commands, and the successful generation after controller recovery.

A real llama.cpp b12110 CPU process serves SmolLM2-135M Q4_K_M with two threads, one slot,
and a 2048-token context. A separate real controller uses a temporary local journal and
one gateway polls its snapshots. The experiment uses a 100 ms configuration poll interval,
200 ms fetch timeout, and two-second maximum age; defaults are 500 ms, one second, and
five seconds respectively.

Revision 1 enabled the configured worker and became applied/fresh. A real 256-token stream
started, then controller revision 2 committed its drain. The gateway reported revision 2
and draining with one inflight request, rejected new work, and allowed the stream to
complete. Revision 3 resumed the worker. Another real stream requested 1024 tokens, and
the controller was forcibly killed after initial content arrived.

The gateway retained applied revision 3 but reported fresh=false after expiry. Readiness
returned 503 while the active stream remained admitted; the stream completed with DONE.
After its reservation returned to zero, generation still returned 503 despite idle capacity,
confirming the stale-view gate rather than simple capacity rejection. Restarting the
controller recovered revision 3 from disk. A valid same-revision confirmation restored
freshness and a new real generation completed. No Raft or multi-host failure is involved.

`go test ./...`, `go vet ./...`, and both builds passed. Controlled tests cover startup
without an applied snapshot, atomic drain overlay, stable logical IDs, local-admin exclusion,
expiry without stream truncation, recovery, rollback, conflicting same-revision data, and
unsupported origins without extending freshness. A test assertion initially required two
successive clock reads to be strictly increasing; its starting timestamp was made explicit
so it no longer depends on timer resolution. Runtime freshness logic did not require that
strict ordering. Linux CI passed, including race detection.

Scope: the gateway retains its configured worker membership and applies desired drain flags
by exact origin match. Unsupported membership changes reject the view without refreshing it.
Controller acknowledgement, gateway application, and completion of draining remain separate.
The freshness gate and reservation share a mutex; already reserved uploads and streams can
continue after expiry. A disconnected gateway can admit from an old confirmed view before
it expires. No fleet-wide drain completion or gateway-durable local view is claimed.


## Durable single-controller configuration

Source: clean commit `4ae5798c7a613a50cbcf75a3c201a5071381eaec`. Reproduce with
`go build -o bin/controller.exe ./cmd/controller` and `python scripts/check_controller.py`.
[Raw executable results](results/controller-4ae5798.json) retain the acknowledged state,
retry result, concurrent-writer status codes, and final recovered configuration.

The real controller process writes an actual local journal and serves GET/PUT /config.
This increment is independent of inference and does not change a gateway's serving state.
No model-performance result is claimed for a storage experiment.

The executable check acknowledged revision 1, rejected a second controller opening the same
journal, then forcibly killed and restarted the owner. Revision 1 and its drain flag survived.
An identical retry returned its original result; a later resume committed revision 2, and
retrying the older drain returned revision 1 without changing current state. Stale revisions
and conflicting reuse of a request ID received 409.

Eight concurrent HTTP writers using expected revision 2 produced one HTTP 200 and seven
409 responses, advancing current state to revision 3. After another forced process kill,
the stopped journal received a three-byte incomplete next header. Recovery retained revision
3, repaired that tail, and acknowledged a subsequent update at revision 4. Another forced
kill/restart preserved revision 4 and its resume flag. These checks use isolated temporary
journal files; there is no destructive modification of application or inference data.

`go test ./...`, `go vet ./...`, and the Windows controller build passed. Storage tests check
32 concurrent CAS attempts, original retry results after later changes and restart, every
incomplete byte-prefix of a trailing record, header/payload checksum corruption, exclusive
journal ownership, and an injected sync error that fences the store. The complete uncertain
record is recovered idempotently after reopening. The Windows test initially attempted to
read a locked journal through another handle; it was corrected to close/reopen around fixture
capture and check those I/O errors. This did not require changing the locking implementation.
Linux CI for this source completed successfully, including race detection.

Scope: one local writer, no Raft, no gateway snapshot consumer yet, no history compaction or
request-ID expiry. A successful update follows file sync, but durability under power loss,
file-creation metadata loss, disk failure, or remote filesystems is not established. An
incomplete final append can be repaired; a complete corrupt record fails startup. Unknown
append/sync outcomes return 503 and fence the store until reopening. A complete unacknowledged
record may survive; clients resolve ambiguity by retrying its original request ID.


## Real worker restart during sustained arrivals

Source: clean commit `2ad3185`. Reproduce with `python scripts/check_restart_load.py --output-dir artifacts/restart-load-aligned`
after building the gateway and setting up the pinned real model. The
[raw results](results/restart-load-2ad3185.json) retain commands, process IDs, observed
event times, all traffic outcomes, interrupted control streams, and final capacity state.

Two actual llama.cpp b12110 CPU processes serve SmolLM2-135M Q4_K_M on the same Windows
machine, each with one slot, two threads, and a 2048-token context. One gateway uses
per-worker limit one, a 100 ms probe interval, and 500 ms probe timeout. Each trial starts
fresh processes. Policy order is fixed least-inflight then affinity, so the run is a
correctness check, not a performance ranking.

A separate control request starts a 1024-token generation with EOS stopping disabled. After
real content arrives, traffic offers 32 requests at two per second independently of responses.
At two seconds, the control worker must still be generating; its process is terminated.
The test observes it becoming unhealthy, requires the control stream to fail without a
completion marker, waits two seconds, starts a new process at the same origin, and requires
health-based recovery. Traffic continues throughout.

| Policy | Trials | Offered traffic | Completed | Rejected | Failed traffic | Intentionally interrupted controls |
| --- | --- | --- | --- | --- | --- | --- |
| Least-inflight | 3 | 96 | 96 | 0 | 0 | 3 |
| Prefix affinity | 3 | 96 | 96 | 0 | 0 | 3 |

All 192 offered traffic requests completed. All six separate control generations failed
explicitly when their workers were killed; these intentional failures are not hidden inside
or excluded silently from the traffic counts. Every run observed four requests scheduled
during the detected outage complete on the survivor. After observed recovery, later
requests completed on the new process (11 per least-inflight run, 22 per affinity run).
Different counts reflect placement and timing, not a capacity or performance ranking.

All original and restarted process IDs differed. Every final gateway snapshot showed both
workers healthy with zero inflight requests, and both real engines reported idle slots.
This verifies eventual cleanup and progress in these runs; it does not promise instantaneous
cleanup or lossless traffic in every failure race. In this schedule no traffic request hit
the detection race. No interrupted generation was retried or resumed by the gateway.

The first harness attempt at `bd7e89e` used different prefix labels for the targeted stream
and background traffic. Affinity legitimately preferred the survivor after recovery, so an
assertion requiring traffic on the recovered worker failed. The
[initial result](results/restart-initial-bd7e89e.json) is retained. The correction aligns
labels and preserves per-run diagnostics on failure; it changes no gateway behavior.

Validation: the harness compiled and completed six real-model runs. It checks active engine
state before kill, incomplete control streams, survivor/restarted-worker progress, count
reconciliation, and zero gateway/engine occupancy after completion. No Go runtime code
changed in this increment. The previous unexplained generation timeout remains open.
Multi-host partitions, disk failure, and gateway restart are not exercised here.

## Mixed request costs and unequal CPU allocation

Source: clean commit `8ec295e21535482f1f9702363c3af25181777c11`. Reproduce with
`python scripts/bench_load.py --profile mixed --worker-threads 1 4 --calibrate --requests 16 --trials 3 --rates 1 4 --output-dir artifacts/mixed-workers`.
[Raw results](results/mixed-8ec295e.json) contain every calibration and offered request,
worker identifiers, token usage, class outcomes, settings, and exact launch commands.

Same Windows i5-1235U host, pinned llama.cpp b12110 CPU engine and SmolLM2-135M Q4_K_M.
Worker 0 uses one CPU thread and worker 1 four; both have one slot and a 2048-token context.
Extra RAM prompt-cache storage is disabled. Both share the host; neither has pinned cores.
There are three trials per policy/rate pair, fresh processes per run, shuffled with seed 11.
Each run offers 12 short and 4 long requests using the same document/arrival sequence.
Completed short requests used 125-126 prompt tokens and requested 16 output tokens; completed
long requests used 520-521 prompt tokens and requested 128 output tokens. The exact prompt
count depends on the document and question. Actual usage and output lengths are checked.

Before offered traffic, each engine receives one direct short and one direct long calibration
with prompt reuse disabled, followed by unrelated warmup. All calibration cached-token counts
were zero. Across 12 runs, per-worker direct completion times were:

| Worker allocation | Class | Samples | Median completion | Observed range |
| --- | --- | --- | --- | --- |
| One CPU thread | Short | 12 | 629.940 ms | 587.986-917.226 ms |
| One CPU thread | Long | 12 | 6917.942 ms | 3837.484-9461.097 ms |
| Four CPU threads | Short | 12 | 435.726 ms | 414.285-556.944 ms |
| Four CPU threads | Long | 12 | 2600.533 ms | 2500.049-3303.562 ms |

Calibration is isolated, in fixed short/long order, and includes first-use shape effects.
It demonstrates different observed service costs, not a model of concurrent service time
or an isolated causal estimate of CPU scaling. Both input and output length change together.

Objectives remain TTFT <= 2000 ms and completion <= 3000 ms, measured from scheduled arrival,
for both classes. They were not retuned after observing calibration. These are experiment
thresholds, not validated application requirements.

| Offered rate | Policy | Offered | Completed | Rejected | Failed | Met both objectives |
| --- | --- | --- | --- | --- | --- | --- |
| 1/s | Least-inflight | 48 | 35 | 13 | 0 | 28 |
| 1/s | Prefix affinity | 48 | 33 | 15 | 0 | 30 |
| 4/s | Least-inflight | 48 | 15 | 33 | 0 | 12 |
| 4/s | Prefix affinity | 48 | 13 | 35 | 0 | 10 |

All 192 offered requests reconcile: 96 completed, 96 rejected, no non-rejection errors.
All completed requests had exactly their requested output count. All admitted responses
had one worker identifier, either 0 or 1; all rejections lacked one in this run. Maximum
generator dispatch lag was below 3.1 ms. Offered/admitted class counts and successful-only
latency percentiles remain in the raw file.

At 1/s, each policy admitted all 12 offered long requests, but least-inflight rejected
13/36 short requests and affinity rejected 15/36. At 4/s, each rejected 6/12 long requests;
short rejections were 27/36 and 29/36 respectively. The long requests always arrive first
in each four-request cycle; this phase and the limited key-to-worker mapping are confounds
for any fairness or general policy claim. The previous uniform experiment has different
request counts and resource settings, so the two runs are not a controlled single-factor comparison.

These results demonstrate that binary inflight occupancy cannot express the measured service
cost difference. They do not demonstrate that a particular cost-aware scheduler would improve
goodput. No placement policy was changed in this increment. Heterogeneous GPUs, jittered
arrival phases, varied independent input/output lengths, and an established-router comparison
remain unmeasured. The earlier unexplained timeout remains open.

`go test ./...`, `go vet ./...`, the build, and Linux CI passed, including race detection.
A network test verifies that upstream values cannot replace or duplicate the gateway's
reserved-worker header while preserving the upstream status and retry guidance. The short
real-model smoke run exercised mixed generation budgets, calibration, and rejection; the
full run's count, output-length, worker-attribution, and zero-cache calibration checks passed.

## Fixed-arrival load and objective-compliant goodput

Source: clean commit `c7e6a88253a9ad7ad5cf9ff38d466f6ab5a88f66`. Reproduce with
`python scripts/bench_load.py` after the pinned model setup and gateway build.
[Raw results](results/load-c7e6a88.json) retain settings, exact arrival document sequence,
commands, every request outcome, token usage, dispatch lag, and per-trial summaries.

Two fresh real llama.cpp b12110 CPU processes per run, SmolLM2-135M Q4_K_M, one slot and
two threads each, 2048-token context, extra RAM prompt-cache tier disabled. One unrelated
warmup per engine. Same Windows i5-1235U laptop with about 8 GB RAM. The gateway permits
one admitted request per worker. Each run offers 24 requests, with 16 output tokens each.

Three trials for each policy/rate pair, shuffled with seed 11. Rates are 1 and 4 requests
per second, independent of outstanding responses. The seed-7 schedule changes from hot A
to hot C halfway through and includes B as background; the exact selected sequence is in
the raw settings. Inputs are constructed documents, not an application-user trace.

The objectives were specified before the measured run: first content within 2000 ms and
complete response within 3000 ms of the scheduled arrival. Both must hold. Rejected and
failed work contributes zero compliant requests. These are experimental objectives, not
an application owner's validated requirements.

| Offered rate | Policy | Offered | Completed | Rejected | Failed | Met both objectives |
| --- | --- | --- | --- | --- | --- | --- |
| 1/s | Least-inflight | 72 | 69 | 3 | 0 | 63 |
| 1/s | Prefix affinity | 72 | 71 | 1 | 0 | 70 |
| 4/s | Least-inflight | 72 | 17 | 55 | 0 | 5 |
| 4/s | Prefix affinity | 72 | 17 | 55 | 0 | 5 |

All 288 offered requests reconcile: 174 completed, 114 rejected, zero transport/stream/HTTP
errors other than rejection. Observed HTTP-200 admissions matched completions in this run.
Admission observations are not a general replacement for authoritative server counters.

Per-trial goodput (compliant requests divided by observed duration, including response tails):
least-inflight at 1/s ranged from 0.749 to 1.000 req/s; affinity ranged from 0.958 to 1.000.
At 4/s the ranges were 0.167-0.329 and 0.000-0.500 respectively. This variation and only three
trials do not establish a general best policy. At the higher rate, affinity's sequential
cache benefit did not yield a consistent goodput advantage. Both policies encountered the
same admission limit and rejected 55 of 72 requests.

Maximum measured generator dispatch lag was below 1.4 ms across all runs. Latencies include
that lag. Successful-only nearest-rank p95 values are recorded per trial alongside failures,
rejections, and all-offered objective compliance; they are not whole-population latency
percentiles. Cache fractions likewise describe completed work only. The observed interval
includes the full scheduled arrival window and any remaining response tail.

The executable owns and cleans up all engine/gateway processes. The harness compiled,
its short real-model smoke run exercised completion and rejection accounting, and all
per-run outcome-reconciliation assertions passed. No Go runtime code changed. No timeout
was observed here; the earlier isolated timeout remains unresolved. GPU performance,
heterogeneous workers, varied lengths, and existing-router comparisons remain unmeasured.

## Capacity-bounded prefix affinity

Gateway source: `90c7678bbd6ae2ca7d14beb4bbb3e692aa7ce5f6`. Recorded runs report a dirty
working tree because README and walkthrough edits were pending; the measured runtime and
experiment source matched this commit. No runtime code changed during measurement.
Raw comparison: [affinity-90c7678.json](results/affinity-90c7678.json).
Reproduce with `python scripts/bench_cache.py --trials 3 --conditions least-inflight prefix-affinity --output-dir artifacts/affinity-comparison`.

Same Windows CPU/model setup and constructed documents as the previous cache baseline:
two fresh real llama.cpp processes per condition/trial, one slot and two threads each,
2048-token contexts, RAM cache tier disabled. Each engine receives an unrelated warmup.
Both policies receive identical X-Prefix-Key labels, messages, and generation settings.
Three shuffled trials, eight sequential measured requests per policy/trial, 48 completed
requests and no observed failures in this comparison.

| Policy | Requests | Cached prompt tokens | Median TTFT |
| --- | --- | --- | --- |
| Least-inflight | 24 | 0.577% | 1514.194 ms |
| Prefix affinity with capacity escape | 24 | 48.654% | 752.857 ms |

These are whole-workload fractions, not a binary cache-hit rate. The sequential comparison
establishes locality on this document schedule. It does not establish concurrent throughput,
tail latency, GPU performance, or superiority on other workloads. The policy estimates
placement from an explicit label; it does not know actual engine cache contents.

Real capacity check: `python scripts/check_real_pool.py` now includes prefix affinity and
a common hot-prefix key on its long generation requests. Two real engines become busy,
the third request receives 503, cancellation releases both slots, and another generation
completes. [Raw capacity results](results/affinity-capacity-90c7678.json) retain all three
policies. This is a bounded burst assertion, not an open-loop load experiment.

`go test ./...`, `go vet ./...`, and the gateway build passed. Controlled tests verify stable
key placement, spillover when preferred capacity is occupied, and exclusion of unhealthy
and draining preferred workers. The cache harness now captures failed-request parameters,
elapsed time, and engine slot observations; forwarding logs record worker, duration, and
context error. No earlier timeout was reproduced in these runs, and its cause remains unresolved.

## Real prefix-cache controls and routing baselines

Source: clean commit `5ff2ed59de61f823de45c626e2f5649137911d79`. Run
`python scripts/bench_cache.py --trials 3 --output-dir artifacts/cache-repeat` after building
the gateway and setting up the pinned model. Raw measurements and commands:
[cache-baseline-5ff2ed5.json](results/cache-baseline-5ff2ed5.json).

Hardware: the same Windows i5-1235U laptop with about 8 GB RAM, CPU inference only.
Engine/model: llama.cpp b12110 and SmolLM2-135M Q4_K_M. Each engine has two CPU threads,
one slot, a 2048-token context, and the extra RAM cache tier disabled. Processes are fresh
for every condition and trial. One unrelated warmup per engine precedes the measured
workload; model download/loading are excluded from client timing. No concurrent arrivals.

Three trials per condition, eight requests each, 96 completed measured requests and zero
failures in this fresh run. The constructed document schedule and questions are committed
in `examples/cache-workload.json`, with its hash in the results. Every response generates
16 tokens. Conditions are shuffled per trial with seed 7; worker task-ID changes identify
where each request actually ran.

| Condition | Requests | Cached prompt-token fraction | Median TTFT |
| --- | --- | --- | --- |
| Direct, cache disabled | 24 | 0% | 1711.519 ms |
| Direct, cache enabled | 24 | 48.654% | 907.615 ms |
| Round-robin, two workers | 24 | 0.577% | 1769.993 ms |
| Least-inflight, two workers | 24 | 0.577% | 1707.033 ms |

Each direct cached sequence reused `[3, 503, 3, 503, 3, 503, 3, 503]` prompt tokens.
Three shared template tokens can survive even for unrelated documents; each adjacent
question sharing the same document reused 503. The disabled-cache control reported zero
for every request. Both pool policies selected workers `[0,1,0,1,0,1,0,1]` in every trial,
placing adjacent repeated documents on different workers. This explains their low reuse
for this particular schedule. It does not establish which policy is better under load.

TTFT includes transport, scheduling, and engine computation. It is not pure prefill time.
Per-trial median TTFT for the disabled control ranged from 1620.929 to 2892.795 ms, showing
substantial timing variation. Token counters provide the evidence of computation reuse;
these laptop timings should not be generalized to GPUs or interpreted as a capacity result.
The workload is constructed, not an application-user trace. No answer cache or affinity
policy was added, and no product adoption or answer-quality result is claimed.

### Interrupted run retained

An earlier three-trial attempt at `c108584` stopped after 51 completed measured requests
when the next least-inflight request did not complete within the configured 60-second
deadline. The gateway log reported a context deadline during response copying; the client
reported an incomplete stream. The engine log shows that request starting but no completed
generation before cleanup. Root cause is not established.

Evidence: [completed conditions](results/cache-interrupted-c108584-results.json),
[interrupted condition](results/cache-interrupted-c108584-failed-1-least-inflight.json), and
[engine/gateway logs](results/cache-interrupted-c108584-logs.json). The failed request is
not included among the fresh run's 96 successes. It remains a separate observed failure.

An isolated replay of least-inflight completed eight requests without reproducing the timeout:
`python scripts/bench_cache.py --trials 1 --conditions least-inflight --output-dir artifacts/cache-reproduction`.
Its [raw results](results/cache-reproduction-5ff2ed5.json) are also retained. The later fresh
three-trial run succeeded with the same deadline. That is not a root-cause fix. Further
timeout diagnostics and a load experiment are needed before making reliability claims.

Validation: the disabled-cache, repeated-prefix, unrelated-prefix, token-count, complete-stream,
and worker-attribution checks passed in the completed run. The Python harness compiled and
executed. No Go runtime code changed in this increment; the preceding health/drain commit's
Linux CI completed successfully, including race detection.

## Real worker health and draining

Source: clean commit `e1bd5e5`. Raw results, model/engine pins, exact launch commands, request
parameters, state observations, and generated output are in
[real-health-e1bd5e5.json](results/real-health-e1bd5e5.json). Reproduce with
`go build -o bin/gateway.exe ./cmd/gateway` then `python scripts/check_real_health.py`.

Two real llama.cpp b12110 CPU processes run SmolLM2-135M Q4_K_M on the same Windows machine.
Each has one slot, two threads, and a 2048-token context. The experiment uses 100 ms probe
intervals and timeouts. This is faster than the executable defaults and is recorded in the
raw commands. No synthetic generations are used in this experiment.

- A 256-token real generation stayed active when drain was acknowledged and completed
  with `[DONE]`. New generation completed on the other worker. Drain eventually reported
  zero locally admitted requests, then an explicit resume restored eligibility.
- Killing worker 0 during a subsequent generation produced a stream failure without a
  completion marker. Its health changed to false and two new generations completed on
  the survivor, without replaying the interrupted request.
- Killing the survivor made all workers unhealthy. Both readiness and new generation
  returned 503. Restarting worker 0 restored health and a new generation completed.
- The script stopped all of its processes after the checks. Logs remain under ignored
  `artifacts/real-health/`; raw observations are committed in the linked JSON.

`go test ./...` and `go vet ./...` passed. Controlled tests cover unhealthy-to-healthy
transitions, a stalled probe and probe shutdown, drain acknowledgement during a stream,
stream completion under drain, resume, and health probes preserving the drain flag.
The gateway process smoke check passed. The earlier real-model routing/admission script
also passed under both policies after fixing its startup check to accept an empty HTTP 200
readiness body; it had incorrectly required JSON and timed out despite successful readiness.

These results establish process-failure and local-drain behavior. They do not establish
durable drain state, coordination across gateways, multi-host partitions, or instantaneous
failure detection. Local race detection was not run; Linux CI requests it.

## Real worker-pool admission

Source: clean commit `9b6d8e0`. Reproduce with `go build -o bin/gateway.exe ./cmd/gateway`
and `python scripts/check_real_pool.py` after the pinned model setup. Raw requests,
launch commands, engine-slot observations, and outcomes are in
[real-pool-9b6d8e0.json](results/real-pool-9b6d8e0.json).

The experiment starts two separate llama.cpp b12110 CPU processes with real SmolLM2-135M
Q4_K_M weights on the same Windows i5-1235U machine. Each has two threads, one inference
slot, a 2048-token context, and prompt caching disabled. The gateway admits one request
per worker. All children are stopped at the end of the experiment.

Both round-robin and least-inflight passed the following sequence:

1. Two requests produce real generated text; both engine slots report active generation.
2. A third request receives 503 with `Retry-After: 1`, while those slots remain occupied.
3. Closing both clients causes both engines to report idle slots.
4. A new generation is admitted and completes with its finish reason and `[DONE]` marker.

This verifies routing and admission behavior, not comparative throughput or a preferred
policy. With one slot per worker, both policies select the available worker. A separate
controlled test distinguishes least-inflight from round-robin under unequal load.

`go test ./...`, `go vet ./...`, and the gateway executable smoke check passed. Controlled
tests also cover 64 concurrent acquisition attempts against capacity four, cancellation
releasing a selected worker, invalid input and transport failures releasing reservations,
and rejection of invalid pool configuration. Local race detection was not run; Linux CI
requests it. Limits apply only to this gateway, not direct engine traffic or other gateways.

## Real CPU inference baseline

Source: clean commit `10734009255f441d1494e801230ae8b0c3580f2f`. Gateway behavior is unchanged
from `cfd2014`; this increment adds the engine setup and experiment tooling. Raw samples,
requests, generated text, usage, and slot observations are preserved in
[real-cpu-1073400.json](results/real-cpu-1073400.json).

Environment: Windows 11 amd64; Intel Core i5-1235U; 8,245,350,400 bytes physical RAM;
CPU execution only. llama.cpp b12110 reports version 0.9.0-dev, commit cd959bae9. Model:
SmolLM2-135M-Instruct Q4_K_M, revision and checksums in `config/llama-cpu.json`. Engine
configuration: four threads, one slot, 2048-token context, GPU offload disabled, prompt
caching disabled. Both engine and Go gateway run on this machine over loopback.

Commands: `python scripts/setup_llama.py`; launch the engine with `scripts/run_llama.ps1`
and the gateway on port 18080 as in the walkthrough; run `python scripts/check_real_inference.py`.
The engine and model downloads passed SHA-256 verification. Python syntax checks,
`go test ./...`, and `go vet ./...` passed. No third-party Python package was installed.

One warmup per route, then three sequential measured requests per route in seed-7 shuffled
order. Each request returned 41 prompt tokens, 32 generated tokens, zero cached prompt tokens,
a length finish reason, and `[DONE]`. All six generated texts matched. No measured failures.

| Route | Samples | Median first-content latency | Median completion latency |
| --- | --- | --- | --- |
| Direct to engine | 3 | 124.325 ms | 344.485 ms |
| Through gateway | 3 | 128.978 ms | 344.364 ms |

These measurements include the HTTP client and loaded-model computation, excluding downloads
and model loading. Three samples per route cannot establish a stable overhead estimate, tail
latency, or a performance improvement. This is a sequential CPU integration baseline, not GPU
or fleet capacity evidence. Cache reuse across requests and concurrent batching were not exercised.

Cancellation: request 1024 tokens with EOS stopping disabled; confirm the slot is busy after
initial content, close the client connection to the gateway, then query the engine directly.
An idle slot was observed 45.224 ms after disconnect. The engine log recorded cancellation
of task 541 followed by slot release. Raw busy/idle observations are in the linked JSON.
This observes capacity release, not instantaneous interruption of every CPU instruction.

## Single-worker gateway validation

Environment: Windows amd64, Go 1.27.3; Python 3.12 for executable smoke checks.

- `go test ./...` and `go vet ./...`: passed.
- `go test ./internal/gateway -v` reproduces the committed network checks: first-chunk
  delivery before worker completion, disconnect propagation through two HTTP connections,
  direct-versus-forwarded complete content, preserved worker rejection without application
  retry, no-backend 503, unreachable-worker 502, and pre-stream timeout 504.
- The same suite checks a worker aborting mid-stream: the client receives a read error and
  no fabricated completion marker. Invalid JSON and oversized requests never reach a worker.
- A non-reading-client test floods the stream until socket buffers fill, then verifies that
  the gateway handler and worker return within bounded waits. This is a cleanup assertion,
  not an application-level memory or fleet admission benchmark.
- `go build -o bin/gateway.exe ./cmd/gateway` and
  `python scripts/smoke.py bin/gateway.exe`: passed. The updated smoke check expects 503
  for generation without a configured worker; liveness and occupied-port checks still pass.
- A manual Python HTTP client against separate gateway and worker executables received a
  full stream, then disconnected a second stream after its first chunk. The worker logged
  `outcome=cancelled chunks=1`. Stopping the worker made a subsequent gateway request return
  502. The committed tests above reproduce these behavior checks using separate HTTP servers.

Local race detection was not run; Linux CI requests it. Active-stream gateway shutdown,
real model cancellation, and performance under load have not been established here.

## Direct streaming worker validation

Environment: Windows amd64, Go 1.27.3, Python 3.12. No external Go dependencies.

- `go test ./...`: passed. Worker tests use real HTTP connections to check incremental
  delivery, prompt cancellation during a long inter-chunk wait, complete SSE framing, and
  rejection of malformed, oversized, unsupported, and out-of-range requests.
- `go vet ./...`: passed.
- `go build -o bin/testworker.exe ./cmd/testworker`: passed.
- A local Python HTTP client against the compiled executable received a complete three-chunk
  response, then disconnected a second request after its first chunk. The worker log recorded
  `outcome=cancelled chunks=1`. This was a manual executable smoke check, not a committed test runner.

The committed network tests reproduce the streaming and cancellation assertions with
`go test ./internal/testworker -v`. Local race detection was not run; Linux CI requests it.
No slow-reader saturation, gateway propagation, inference-engine cancellation, or performance
claim is established by these checks.

## Foundation validation

Environment: Windows amd64, Go 1.27.3, Python 3.12; Ubuntu under WSL for the Linux process check.
The Go archive was verified against its official download checksum. No external Go dependencies.

| Check | Result |
| --- | --- |
| `go vet ./...` | Passed |
| `go test ./...` | Compiled successfully; no Go test files yet |
| `go build -o bin/gateway.exe ./cmd/gateway` | Passed on Windows |
| `python scripts/smoke.py bin/gateway.exe` | Health JSON, generation 404, method 405, occupied-port failure passed |
| Cross-compile with `GOOS=linux GOARCH=amd64 go build -o bin/gateway-linux ./cmd/gateway` | Passed |
| `wsl -d Ubuntu -- python3 /mnt/d/inference-fleet/scripts/smoke.py /mnt/d/inference-fleet/bin/gateway-linux` | HTTP checks and SIGTERM shutdown passed |

SIGTERM was checked with no active requests. Graceful draining of active inference streams
is not yet tested because inference forwarding is not implemented. Windows smoke testing
skips graceful shutdown; terminating a Windows subprocess is not the same as delivering SIGTERM.
Local race detection was not run. The Linux CI workflow requests race detection, but with
no Go tests this does not establish exercised race coverage.

At the foundation increment there were no inference measurements. The CPU integration
baseline above is now available; routing, cache-policy, and consensus benchmarks remain future work.

Future experiment entries must include the commit, exact command, environment, raw artifacts,
outcome, limitations, and whether inference was synthetic, CPU, or GPU.
