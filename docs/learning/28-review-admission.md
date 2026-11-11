# Review follow-up: separate upload admission and protect checkpoint capacity

The review identified a concrete serving weakness: the gateway reserved a worker before
reading and validating the request body. An incomplete upload could occupy the only
inference slot while the engine was idle. A raw-socket regression reproduced this: a
second complete request received HTTP 503. The regression failed before the fix.

## Separate upload and generation budgets

`-max-uploads` bounds concurrent body ingestion per gateway (default 8, maximum 1024).
`-upload-timeout` bounds the socket read for the body (default 10 seconds). Programmatic
zero values select those defaults. Request bodies retain the 64-KiB bound.

The handler takes a nonblocking upload token, reads and validates the complete JSON body,
then releases that token. Only then does it select/reserve an inference worker. Upload
saturation returns HTTP 503 with `Retry-After: 1`; an upload read timeout returns 408;
malformed JSON returns 400 and oversized bodies return 413. None reserves worker capacity.
The serving reservation lasts until forwarding completes, fails, or is cancelled.

Routing rechecks configuration freshness, health, drain state, and capacity after the body
arrives. A worker drained during an upload cannot receive that pending request. Read
deadlines are cleared after a complete upload; failed/expired reads retain their bound
for HTTP cleanup. Accepted streams retain their separate forwarding/write deadlines.

These limits bound handler body ingestion and per-worker active generation. They are not
a global connection limiter, tenant fairness policy, or a fleet-wide counter. Bodies remain
buffered while their generation is active; allocations depend on both configured budgets.

Network tests exercise incomplete uploads, recovery after deadline, concurrent inference
with one upload token, and a drain committed while a body is still arriving. A separate
two-gateway test targets one worker with its own one-request bound: the second gateway's
local reservation does not override the worker's 503. Disconnecting the first client frees
the worker for the second gateway. This is an explicit HTTP test fixture, not a claim of
multi-gateway GPU scaling or a new shared admission service.

## Refuse uncheckpointable new successes

The retained application codec keeps every successful request's original update/result
and has limits of 1024 successful IDs and eight MiB. Previously, further unique successes
could still be acknowledged even though a later retention operation could no longer
encode the application image.

New configuration submissions in `-retained-dir` mode use version-2 update commands. Ordered
application checks the candidate's complete checkpoint before admitting a fresh success.
It reserves the maximum uint64 index/term encoding lengths, so later elections and retries
cannot break the byte budget merely by increasing those numbers. A capacity rejection
returns 503, leaves revision/history unchanged, and never evicts acknowledged IDs. Original
successful retries still return their original results at capacity.

The leader performs the same read-only check before proposing a known-full update, so
repeated known-full submissions do not fill the Raft log. The apply-time check remains
authoritative for intervening commits. Conflicts and retries retain their existing semantics.
This serial admission check encodes the candidate history and costs work proportional to
the retained history; it is an administrative-path tradeoff, not a throughput optimization.

Version-1 commands keep their existing replay semantics. Default full-journal submissions
remain version 1. Existing acknowledged history is not rewritten or silently reinterpreted.
Version-2 logs require the upgraded binary: there is no downgrade/mixed-version protocol
guarantee. An already oversized legacy history is not repaired by this policy.

This makes capacity exhaustion fail safely; it does not remove the lifetime history limit.
Retention is still manual and must occur before suffix exhaustion. A sustainable retention
or history-expiration protocol needs an explicit administrative contract, not larger constants
or silent deduplication eviction.

## Reproduce

```powershell
go test ./internal/gateway -run 'TestSlowUpload|TestUpload|TestDrainDuringUpload|TestTwoGateways' -count=1 -v
go test ./internal/control -run CheckpointAdmission -count=1 -v
go test ./internal/raftnet -run 'TestRetainedAdmission|TestOrderedBounded' -count=1 -v
go test ./...
go vet ./...
```

The native-file controller test seeds an actual ordered version-1 prefix with 1023 successes,
admits the last version-2 success over HTTP, rejects new submissions without advancing the
log, retains the image, reopens storage, and verifies current state plus an original retry.
The byte-bound test reaches the payload limit before the count limit with valid large
configurations. Linux CI additionally runs the suite with race detection and native peer
failure/recovery scenarios.

Clean-source evidence on `3780f04`: [real two-engine pool](../results/review-admission-3780f04-pool.json),
[real-model controller failover](../results/review-admission-3780f04-failover.json), and
[Linux race/native-process CI](https://github.com/Kaif10/distributed-llm-serving/actions/runs/43048907488).

Request-cost weighting and prefix-policy tuning remain experiments requiring measured
goodput improvements. This change introduces neither a claimed routing win nor fleet-wide
admission. Independent-host validation and the previously recorded reset/timeout remain
unresolved; no fix for those observations is claimed here.
