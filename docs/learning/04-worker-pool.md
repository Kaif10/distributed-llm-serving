# Increment 4: route to available capacity

One worker limits how much work can proceed at once. We now configure several replicas of
the same model and reserve capacity before dispatching each request.

```text
                        +-> model server A (one admitted request)
Client -> Go gateway ---+
                        +-> model server B (one admitted request)
```

Round-robin chooses the next worker with capacity. Least-inflight chooses the worker with
the fewest admitted requests, rotating through ties. Both skip workers at their configured
limit. If every worker is full, the gateway immediately returns 503 and `Retry-After: 1`.
That header is a retry suggestion, not a reservation or a guarantee of available capacity.

## Run the real model experiment

Set up the pinned model with `python scripts/setup_llama.py` if needed, then build the gateway:

```powershell
$fleetGo = "$env:LOCALAPPDATA\inference-fleet\toolchains\go1.27.3\go\bin\go.exe"
& $fleetGo build -o bin/gateway.exe ./cmd/gateway
python scripts/check_real_pool.py
```

The script owns two real llama.cpp processes, each loading SmolLM2-135M Q4_K_M with one
slot, two CPU threads, and a 2048-token context. It starts a gateway for each policy,
observes both engine slots actively generating, verifies rejection of a third request,
disconnects the streams, observes idle engine slots, and verifies another generation completes.
It stops its processes on exit. Ports 18080, 18081, and 18082 must be available.
Raw engine logs and results go to `artifacts/real-pool/`. This Windows CPU experiment uses
real weights for every generation; it measures correctness rather than serving capacity.

For manual operation, launch two engines on different ports, then configure the gateway:

```powershell
& $fleetGo run ./cmd/gateway -listen 127.0.0.1:18080 -backends http://127.0.0.1:18081,http://127.0.0.1:18082 -policy least-inflight -max-inflight 1 -request-timeout 60s
```

The older `-backend` flag still configures a single worker. Use one of `-backend` or
`-backends`; empty pools serve health but reject generation. Default per-worker admission
is now one request. All configured workers must serve the same model and configuration.

## Why reservation must be atomic

Suppose two concurrent requests both observe a worker with zero active requests. If selection
and incrementing the counter happen separately, both can claim its last slot. The pool holds
one short mutex while selecting a worker and incrementing its counter. Network operations
run outside that lock.

Capacity is held from admission through request reading and forwarding. A deferred release
runs on completion, invalid input, timeout, disconnect, or a proxy panic after a broken stream.
The count represents requests admitted by this gateway; a slow upload also occupies a slot.
There is no waiting queue. Request reading and response writes retain their existing deadlines.

## What the limit means

The limit is local to this gateway process. Two gateways each configured with limit one can
jointly send two requests to a worker. Direct traffic to the engine also bypasses this limit.
Engine-side admission will remain necessary when we introduce multiple gateways. This limit
does not cap all open HTTP connections or all server memory.

Inference may continue briefly after transport cancellation before the engine notices. Thus
releasing a gateway reservation does not prove the GPU or CPU is already idle; our separate
real-engine observations check eventual slot release.

Active health probes and worker draining are the next increment. Currently an unreachable
worker remains configured: a request selected for it fails with 502, releases its reservation,
and is not replayed on another worker. Existing partial streams cannot be resumed transparently.

## Verification and exercises

`go test ./internal/gateway -v` includes concurrent admission, policy selection under unequal
load, rejection while two workers are busy, and reuse after cancellation and transport failure.
These controlled test fixtures exercise interleavings and errors; the separate real-model
script establishes the same admission behavior with actual inference engines.

- Why does rotating ties matter when every worker reports the same load?
- Can request count alone distinguish a short generation from a long one?
- Why does a per-gateway counter fail to impose a fleet-wide limit?
- What should happen to an active stream when an administrator drains its worker?
