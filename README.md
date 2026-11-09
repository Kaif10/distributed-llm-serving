# Inference Fleet

A project to build and understand reliable routing across a pool of LLM inference servers.

![Recorded working demo: real LLM routing, cancellation, and controller recovery](docs/demo/demo.gif)

32-second edited playback of the real run. [Commands, raw results, and limitations](docs/demo/README.md).

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
Real-model experiments cover drain, worker process loss, and recovery. A separate replicated
configuration API supports committed updates and quorum-backed reads; gateways can poll a
fixed peer list and switch controller sources after leadership changes. A repeated-document experiment measures real prefix-cache reuse against disabled
caching and the existing routing policies; its evidence includes an unresolved timeout from
an interrupted run. No production usage is claimed. The operational problem still
needs validation with a real application workload.

A durable controller persists desired configuration with CAS and idempotent retries.
Gateways can consume its drain snapshots for a fixed worker pool, report applied revisions,
and reject new work when their confirmed view expires. A fixed-membership Raft group now
applies configuration updates, replays successful request-ID results after restart, and
serves fresh reads through majority confirmations without growing the journal. The write
log remains bounded without compaction in the default journal mode. The alternative
two-slot mode supports owner-driven local retention, application restoration, and snapshot
catch-up across fixed controller peers. Dynamic membership remains outside scope.

A versioned application checkpoint preserves configuration plus successful request-ID
history. A separate publication journal selects synced images across process crashes, with
identity/history checks and idempotent retries. Configuration peers can opt into manual
checkpoint capture and checkpoint-plus-suffix recovery, verified against retained-prefix
replay. The alternative retained-image mode removes committed prefixes manually and
transfers images to lagging peers while preserving historical successful results.

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
- [Raft peer processes](docs/learning/16-raft-processes.md): bounded HTTP transport, leader loss, and journal recovery.
- [Replicated configuration](docs/learning/17-replicated-configuration.md): commit/apply, retry replay, and the initial read contract.
- [Read quorums](docs/learning/18-read-quorum.md): fresh reads without per-poll journal growth.
- [Controller failover](docs/learning/19-controller-failover.md): bounded peer polling, stale-view expiry, and real-model recovery.
- [Application checkpoints](docs/learning/20-application-checkpoints.md): preserve retry results before log retention.
- [Checkpoint publication](docs/learning/21-checkpoint-publication.md): sync-before-selection, process-kill recovery, and uncertain retries.
- [Controller checkpoint recovery](docs/learning/22-controller-checkpoint-recovery.md): owner capture, confirmed-prefix installation, and full-replay comparison.
- [Controller capacity](docs/learning/23-controller-capacity.md): measured write limits, journal growth, and restart failure at entry exhaustion.
- [Retained log boundary](docs/learning/24-retained-log-boundary.md): absolute Raft indices and catch-up requirements.
- [Retained storage](docs/learning/25-retained-storage.md): two bounded file slots recover a checkpoint and suffix together, with native publication checks.
- [Controller local retention](docs/learning/26-controller-local-retention.md): opt-in single-member service recovery, manual retention, and restart/retry checks.
- [Peer snapshot recovery](docs/learning/27-peer-snapshot-recovery.md): three-controller retention, lagging-peer installation, and real-model quorum recovery.
- [Recorded demo](docs/demo/README.md): real-model routing/cancellation, controller snapshots, failure/recovery, and the complete capture evidence.
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
                           replicated control plane (fixed membership)
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
