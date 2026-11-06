# Evidence ledger

The gateway routes streaming requests across a static worker pool with local per-worker
admission limits. Milestones 1 and the local integration portion of 2 are implemented;
milestone 3's routing, admission, health, and local draining increments are verified with
two real model processes. Milestone 4 now has measured real-model cache controls and
existing-policy baselines; affinity routing and load experiments remain unfinished.
Milestones 5 through 9 remain planned.
Historical entries below describe the checks at their original increments.

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
