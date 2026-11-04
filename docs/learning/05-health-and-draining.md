# Increment 5: remove a worker from new assignments

Worker availability changes after startup. The gateway now probes each worker and offers a
local drain operation for planned maintenance. Existing streams keep their original worker.

## Run against real model servers

After the pinned engine/model setup, build the gateway and run the experiment:

```powershell
$fleetGo = "$env:LOCALAPPDATA\inference-fleet\toolchains\go1.27.3\go\bin\go.exe"
& $fleetGo build -o bin/gateway.exe ./cmd/gateway
python scripts/check_real_health.py
```

The script starts two real llama.cpp CPU processes and one gateway, and owns their cleanup.
Ports 18080, 18081, 18082, and 18090 must be available. It records raw results and logs under
`artifacts/real-health/`. It checks:

1. Drain worker 0 while it generates 256 tokens; the stream still reaches `[DONE]`.
2. Send new work through worker 1 while worker 0 is draining.
3. Resume worker 0, start another generation, and kill its process. The active stream fails.
4. Observe worker 0 become unhealthy and complete requests on the survivor.
5. Stop the survivor; new generation and readiness return 503.
6. Restart worker 0; successful probes restore eligibility and generation works again.

This is a process-failure experiment on one machine, not a multi-host partition test.

## Health is an observation

The executable starts workers as ineligible until their first successful probe. Each worker
has a sequential probe loop, independent of other workers. The default probe is `GET /health`,
with a 500 ms timeout and a one-second delay after each probe. Only HTTP 200 is success;
redirects are not followed. Configure these using `-health-path`, `-health-timeout`, and
`-health-interval`. The synthetic test fixture also implements `/health`.

A failed or timed-out probe excludes the worker from new assignments. A subsequent successful
probe restores health. One sample changes the health state; hysteresis is not implemented.
Probe results never clear administrative drain state. They also do not cancel active streams.

Failure can occur between a successful probe and dispatch. That request may still fail with
502 before streaming, or an interrupted stream afterward. There is no replay on another worker.
Under ordinary scheduling, detection takes up to roughly the interval plus probe timeout;
this is not a hard real-time guarantee. An overloaded health endpoint can also produce a
false failure observation. The timestamp exposes when the latest probe completed.

`/healthz` reports gateway process liveness. `/readyz` reports whether at least one worker is
healthy and not draining. Readiness deliberately ignores temporary capacity exhaustion;
generation can return 503 while readiness is 200. Empty, all-unhealthy, and all-draining
pools have readiness 503.

## Drain through a separate listener

Example gateway command, with engines already running on ports 18081 and 18082:

```powershell
& $fleetGo run ./cmd/gateway -listen 127.0.0.1:18080 -backends http://127.0.0.1:18081,http://127.0.0.1:18082 -admin-listen 127.0.0.1:18090 -request-timeout 60s
```

```powershell
curl.exe http://127.0.0.1:18090/workers
curl.exe -X POST http://127.0.0.1:18090/workers/0/drain
curl.exe http://127.0.0.1:18090/workers
curl.exe -X POST http://127.0.0.1:18090/workers/0/resume
```

IDs are zero-based positions in the configured backend list. The admin listener is optional,
must bind to a loopback IP, and exposes no operations on the generation listener. It has no
authentication and is intended for trusted local operation only.

The drain flag and request reservations share the same mutex. After drain is applied, new
reservations skip that worker. Previously admitted requests, including requests still uploading,
may finish. The acknowledgement can report `draining=true` with `inflight>0`; it does not wait
for completion. Poll `/workers` until `drained=true` before stopping a worker for maintenance.
This means the worker is draining and this gateway has no remaining admitted requests.

These states are local and in memory. Restart clears drains. Other gateways and direct engine
clients are outside this accounting. `resume` permits selection only if health is also good.
The later durable control-plane milestone will introduce persisted, versioned desired state;
we do not claim those guarantees for this local control API.

## Understand the tradeoffs

- Why can a request fail even when the most recent probe succeeded?
- Why must health recovery leave a drain flag intact?
- Why can an acknowledgement say draining without saying drained?
- What extra coordination would two gateways need before reporting a fleet-wide drain complete?

Next milestone: measure cache locality versus load under a concrete repeated-prefix workload.
First establish routing baselines and verify the real engine's cache observations before
implementing an affinity policy.
