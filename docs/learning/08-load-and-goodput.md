# Increment 8: compare useful completions under fixed arrivals

Sequential requests can show cache reuse but hide contention. This experiment schedules
arrivals independently of earlier responses and changes the hot document halfway through.
All generated responses come from two real pinned llama.cpp CPU processes.

```powershell
go build -o bin/gateway.exe ./cmd/gateway
python scripts/bench_load.py
```

Use the local Go executable path from earlier walkthroughs if needed. The script owns the
two engines and gateway and cleans them up afterward. Ports 18080, 18081, and 18082 must
be available. Logs, individual request outcomes, settings, and launch commands are saved
under `artifacts/load-baseline/`. Model setup is the same as the preceding experiments.

## Workload and controls

Default settings are 24 offered requests per run, rates 1 and 4 requests per second,
least-inflight and prefix-affinity, three trials each: 12 runs and 288 offered requests.
Conditions are shuffled with seed 11. The document schedule uses seed 7: A is hot in the
first half, C in the second, with an 80% selection probability and B as background traffic.
This changes the hot prefix; it does not guarantee that A and C hash to different workers.
The exact selected sequence is recorded and identical across all conditions and trials.

Every request uses the same constructed document corpus, changing questions, cache-enabled
generation, and 16 output tokens. Each engine has one slot, two CPU threads, a 2048-token
context, and its additional RAM cache tier disabled. The gateway permits one active request
per worker. Processes restart per run; each engine gets one unrelated warmup before arrivals.

These are constructed requests with actual model computation, not an application-user trace.
The objectives below are experimental thresholds, not a user-validated production SLO.

## Arrival and latency accounting

The generator schedules request i at `start + i / rate`, regardless of outstanding work.
It uses a bounded thread pool sized to the total request count (maximum 64), with no smaller
executor pool silently queuing offered work. Actual thread dispatch can still lag. Each
request records that lag; latencies start at its scheduled due time, so lag is included.

The declared objectives are TTFT <= 2000 ms and completion <= 3000 ms. A request must
produce content, complete the SSE stream with a finish reason and `[DONE]`, report usage
for 16 output tokens, and meet both thresholds. Otherwise it contributes zero goodput.
Rejections and errors are never retried by the generator.

The script reports:

- Offered, observed admitted (HTTP 200), completed, rejected (HTTP 503), and failed counts.
- Requests meeting both objectives, and their fraction of **all offered requests**.
- Completed throughput and objective-compliant goodput over the observed run duration.
- Successful-request p95 TTFT/completion, cache reuse among completions, and dispatch lag.

The observed duration includes the entire scheduled arrival window plus any remaining
response tail. Success-only p95 values exclude failures and rejections and must be read with
their counts and the all-offered objective fraction. With small samples, p95 is coarse;
the script uses nearest rank, not an extrapolated tail model. No p99 claim is made.

HTTP-200 admission is an observation, not authoritative server accounting. A proxy error
before headers can have unknown admission status. In this isolated setup a 503 is treated
as rejection; another engine configuration might itself return 503 after accepting work.
Incomplete streams after HTTP 200 count as admitted and failed. Outcomes reconcile to the
offered count. This script does not issue cancellations deliberately; transport timeouts
are failures and close their connections.

## Interpretation

The recorded run offered 288 requests. At 1/s, least-inflight met both objectives for
63/72 requests and prefix affinity for 70/72. At 4/s, both completed 17, rejected 55,
and met both objectives for only 5/72 requests. No non-rejection errors occurred.
See the [evidence ledger](../EVIDENCE.md) for per-trial ranges and raw samples.
This identifies a capacity/objective problem at the higher rate, not a general policy winner.

A high cached-token fraction can coexist with high rejection or missed latency objectives.
Compare the per-trial outcomes, not just a pooled percentile or one favorable timing.
Both policies can spread a hot prefix across two workers because of the capacity escape rule.
At high arrival rates, both can reject most requests. An empty or evicted preferred cache
still requires real prefill computation.

Each failure retains its error type and elapsed time; raw engine and gateway logs remain
available locally. The earlier unexplained timeout is not considered fixed by successful
load runs. These same-host CPU measurements do not establish GPU performance, multi-host
behavior, demand for the product, or a general best policy.

The next step depends on the measured outcomes: identify a concrete latency or rejection
problem before adjusting placement. The broader plan still includes varied request lengths,
heterogeneous workers, restarts, and comparison against an existing router.
