# Inference Fleet

A project to build and understand reliable routing across a pool of LLM inference servers.

The intended user operates several replicas of the same model. Requests differ in length,
popular prompts concentrate traffic, and workers sometimes slow down or disappear. The
goal is to keep useful work flowing while bounding queues and handling failures explicitly.

**Current status:** a Go gateway routing streams across statically configured replicas with
round-robin, least-inflight, or hinted prefix-affinity selection and per-worker admission limits, validated with
both the synthetic worker and real SmolLM2-135M generation through llama.cpp on CPU. Network
tests cover cancellation, deadlines, and worker failures; a separate local-engine check observes
slot release on disconnect and records a small sequential latency baseline. A two-engine CPU
experiment checks routing and overload against real models. Active health probes exclude
unavailable workers, and a separate local admin API drains assignments while streams finish.
Real-model experiments cover drain, worker process loss, and recovery. Serving-controller replication remains
planned. A repeated-document experiment measures real prefix-cache reuse against disabled
caching and the existing routing policies; its evidence includes an unresolved timeout from
an interrupted run. No production usage is claimed. The operational problem still
needs validation with a real application workload.

A durable controller persists desired configuration with CAS and idempotent retries.
Gateways can consume its drain snapshots for a fixed worker pool, report applied revisions,
and reject new work when their confirmed view expires. Replication and dynamic membership
remain future work.

## Start here

- [Milestones](docs/PLAN.md): what to build, why, and what evidence completes each step.
- [Architecture](docs/ARCHITECTURE.md): request flow, control-plane responsibilities, and failure contracts.
- [First walkthrough](docs/learning/00-service.md): run the service and understand its lifecycle.
- [Streaming worker walkthrough](docs/learning/01-streaming-worker.md): watch chunks arrive and disconnect the client.
- [Gateway forwarding walkthrough](docs/learning/02-gateway-streaming.md): stream across two connections and trace failures.
- [Real inference walkthrough](docs/learning/03-real-inference.md): run pinned CPU inference and observe engine cancellation.
- [Worker pool walkthrough](docs/learning/04-worker-pool.md): run two real engines and reject excess work.
- [Health and draining walkthrough](docs/learning/05-health-and-draining.md): remove workers safely from new assignments.
- [Cache baseline walkthrough](docs/learning/06-cache-baseline.md): measure real prefix reuse and existing routing policies.
- [Prefix affinity walkthrough](docs/learning/07-prefix-affinity.md): preserve locality with capacity escape and real-model checks.
- [Load and goodput walkthrough](docs/learning/08-load-and-goodput.md): compare fixed arrivals and changing hot prefixes.
- [Mixed request costs](docs/learning/09-mixed-costs.md): measure short/long generations and different CPU allocations.
- [Restart under load](docs/learning/10-restart-under-load.md): verify real worker recovery while arrivals continue.
- [Decisions](docs/DECISIONS.md): choices and alternatives, including where Raft belongs.
- [Durable configuration](docs/learning/11-durable-configuration.md): journal recovery and revision contracts.
- [Applied configuration](docs/learning/12-applied-configuration.md): distinguish saved, applied, and drained state.
- [Deterministic state machine](docs/learning/13-state-machine.md): separate ordered decisions from journal I/O.
- [Raft election core](docs/learning/14-raft-elections.md): durable voting and deterministic election/failure scenarios.
- [Raft log replication](docs/learning/15-raft-replication.md): durable proposals, conflict repair, and quorum commit.
- [Evidence](docs/EVIDENCE.md): implemented behavior and validation records.

## Run the foundation

With Go 1.26 or later:

```sh
go run ./cmd/gateway -listen 127.0.0.1:8080
```

In another terminal:

```sh
curl http://127.0.0.1:8080/healthz
```

This returns `{"status":"ok"}`. It means the HTTP process responds; it does not mean a
model is loaded. Without `-backend`, generation returns 503. Stop with Ctrl+C.
To stream through the gateway, start the test worker and run
`go run ./cmd/gateway -backend http://127.0.0.1:8081`; follow the forwarding walkthrough above.

```sh
go test ./...
go vet ./...
go build -o bin/gateway ./cmd/gateway
python scripts/smoke.py bin/gateway
```

On Linux with a C compiler, also run `go test -race ./...`.
On Windows, use `bin/gateway.exe` for the build output and smoke-test argument.
`scripts/smoke.py` checks the gateway process over HTTP; Go network tests check the synthetic worker.

## Planned system

```text
Application -> gateway -> inference servers running real model weights
                  |
                  +-- local view of versioned fleet configuration
                                      ^
                           replicated control plane (later)
```

Start with static configuration and one backend. Add multiple workers, bounded admission,
failure handling, and measured routing policies. Then make administrative configuration
durable and replicate it with Raft. Token streams never pass through the consensus log.

Existing systems already address this problem, including
[vLLM's load-aware router](https://docs.vllm.ai/projects/production-stack/en/latest/use_cases/loadaware-routing.html).
We will compare against them before claiming a useful improvement. A focused upstream
contribution remains a valid outcome.

## Verification

Commits describe changes to behavior, design, and validation. The evidence ledger records
commands, environments, raw results, and limitations. Walkthroughs connect each increment
to a runnable experiment. Engine and model sources are pinned and credited in the setup
configuration and real inference walkthrough.
