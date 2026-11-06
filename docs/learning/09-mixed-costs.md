# Increment 9: one active request is not a fixed amount of work

We now vary request shape and worker CPU allocation while keeping the existing policies.
This diagnoses their assumptions before introducing another routing heuristic.

```powershell
go build -o bin/gateway.exe ./cmd/gateway
python scripts/bench_load.py --profile mixed --worker-threads 1 4 --calibrate --requests 16 --trials 3 --rates 1 4 --output-dir artifacts/mixed-workers
```

Use the local Go executable path from earlier walkthroughs if needed. The pinned real
llama.cpp model setup remains unchanged. This runs two actual CPU inference processes,
one with one thread and one with four. Thread count is a resource setting, not a guarantee
that the four-thread worker is faster. Both share the same host and memory subsystem.

## Request classes

Every fourth request uses the full 16-record document and requests 128 output tokens.
The other requests use the header and first three records, requesting 16 output tokens.
Both are real model generations, with EOS stopping disabled to fix the output budget.
Each 16-request run offers four long and twelve short requests. The document schedule,
questions, arrival times, and shape assignments are identical across policies and trials.
Prefix hints include the document and request class so different document versions/shapes
are explicitly distinguished. Runtime cache matching remains the engine's responsibility.
Both input and output lengths change together; this run cannot attribute a timing difference
to prefill versus decoding separately. Long requests occupy positions 0, 4, 8, and 12, so
their arrival phase can affect class rejection rates. No fairness conclusion follows from
this one fixed phase.

Default `--profile uniform` and `--worker-threads 2 2` preserve the preceding experiment's
settings. Mixed-profile output counts are validated against each request's own budget;
they are not assumed to be 16. Individual results record request class, requested output
tokens, input character count, actual engine-reported token usage, and selected worker.

## Calibration and controls

With `--calibrate`, each fresh engine receives one direct short and one direct long request
with prompt reuse disabled. Timings and usage are recorded before an unrelated warmup
resets its slot content for the offered workload. These are isolated direct measurements;
they do not predict speed under simultaneous CPU contention. Per-run repetitions expose
variation instead of treating thread count as a measured speed ratio.
Calibration order is short then long, with one sample of each per engine/run. First-use
shape effects and host load can influence those samples; they are not fitted service-time
predictions or an isolated causal study of CPU scaling.

The gateway now reports `X-Inference-Worker` after reserving capacity. Its value is the
zero-based backend-list index. Upstream values for that header are removed, so a worker
cannot replace the gateway's identifier or introduce a duplicate. A network test verifies
this while preserving upstream status and retry guidance. It is a diagnostic identifier,
not a durable fleet-wide worker ID.

The harness records admission when this header is present, including errors after a
reservation. It also preserves HTTP-200 admission as a compatibility fallback. A 503 is
still classified as rejection; an admitted upstream 503 can therefore overlap that count.
In the isolated experiment, gateway-side capacity rejection is expected to have no worker
header. Raw status, header, outcome, and class let reviewers inspect that assumption.

## Read the results by class

The recorded run offered 192 requests: 96 completed and 96 were rejected, with no other
errors. At 1/s, least-inflight met objectives for 28/48 and affinity for 30/48; at 4/s the
counts were 12/48 and 10/48. Direct calibration medians ranged from 436 ms for a short
request on four threads to 6918 ms for a long request on one thread. See the
[evidence ledger](../EVIDENCE.md) for all class counts, ranges, and raw measurements.

Counts reconcile per run and per request class. A policy can appear good overall while
short requests lose capacity to long generations. Conversely, many rejected long requests
could make a successful-only latency percentile look favorable. Compare offered counts,
objective-compliant fractions, and worker assignments before interpreting averages.

The 2000 ms first-content and 3000 ms completion objectives are unchanged and apply to both
classes. They are experimental thresholds, not evidence that real users need identical
deadlines for short and long answers. We do not retune them after observing the results.

Each worker still has a single engine slot and gateway admission limit one. Inflight load
is consequently zero or one, not a queue-length estimate. The experiment can demonstrate
unequal service costs, but cannot by itself establish that a cost-aware scheduler would
improve goodput. No new placement policy is introduced in this increment.

Only same-host CPU allocation differs. This does not simulate heterogeneous GPUs, network
latency, worker CPU affinity, or distributed storage. The earlier unexplained timeout remains
open even if these requests complete. Broader comparisons should include an established router
before treating this gateway as an operational improvement.
