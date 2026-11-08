# Increment 23: measure the controller's storage and restart limits

Checkpoint recovery is connected, but it retains the full Raft journal. This experiment
measures the current limits before changing storage. It uses real single-member controller
processes, synced files, and sequential closed-loop administrative requests. It does not
measure inference, quorum-network throughput, or multi-host capacity.

```powershell
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/check_controller_capacity.py bin/raftpeer.exe --output artifacts/controller-capacity/full.json
```

Use `--go <executable>` if Go is not on PATH. `--profile quick` runs a small recovery check
for CI. The full profile adds three paired recovery trials and both capacity-limit checks.
Successful runs clean up their own temporary files; failed runs preserve them for inspection.

## Workload and observations

Recovery starts from an identical journal containing 128 successful updates. Full-replay
and checkpoint-backed variants receive copies of those same bytes; variant order alternates
between trials. Each must reach the same configuration, preserve an original successful
retry, and emit the same complete application-image hash at the same committed position.

Two exhaustion workloads use valid desired-worker configurations. One worker gives small
commands and reaches the 1024-entry limit. A 128-worker configuration with long IDs gives
larger commands and reaches the four-MiB command-data limit first. Those workers are desired
configuration records only; no inference workers are started in this control-plane test.
Each request waits for the preceding response. Raw accepted/rejected latencies are retained.

The runner records acknowledged revisions, log entries and command bytes, journal bytes,
growth samples, exact rejection messages, historical retry behavior at capacity, and
restart outcomes. A read-only CRC-framed journal inspector checks that acknowledged entries
are still present after restart, including when a process cannot become ready.

## What the current implementation demonstrates

- At the entry-count limit, the running leader can still serve fresh reads, but cannot append
  another update or even a duplicate successful update. A restart cannot append its required
  current-term leader no-op, so the process stops before serving fresh configuration.
- At the payload-byte limit, writes and logged retries also fail. A restart can append a
  zero-payload no-op because there is still entry-count space, and fresh reads can recover.
- A checkpoint can be captured at either limit, but does not remove the need to append the
  new leader no-op or retain the full log. It therefore does not fix entry-full restart.
- Repeated full-log images in the WAL consume much more disk than the live command payload.
  A bounded in-memory log is not a bounded lifetime journal.

The evidence ledger records the actual counts, byte sizes, timings, and source commit.
Expected rejections and entry-full startup failure count as successfully reproduced limits,
not successful service availability. Acknowledged data retention and ability to serve after
restart are reported separately.

## Reading the timing results

Listener readiness is the first observed successful diagnostic request; fresh-read readiness
is the first successful quorum-backed configuration read. Both include process startup,
polling/network effects and scheduling; fresh reads also include randomized election timing.
Three trials and uncontrolled OS file caching cannot isolate application restore CPU cost or
establish a speedup. Checkpoint mode still replays the retained prefix to verify it. No
recovery-latency improvement or production SLO is claimed.

## Storage decision

Raising the entry bound would defer the same failure and increase full-image I/O. Reserving
one no-op slot would only defer it through a limited number of elections. The measured issue
justifies a real retained-prefix boundary rather than another ad hoc limit adjustment.

Next: offset-aware Raft storage tied to a durable checkpoint, then lagging-peer installation
and bounded checkpoint-catalog rollover before enabling live compaction. Every acknowledged
update and retained retry result must survive. The finite application retry-table capacity
also needs an explicit contract; compaction must not silently forget successful request IDs.
