# Milestones

Progress is based on demonstrated behavior and understanding, with no calendar estimates.
The foundation, single-worker streaming path, and milestone 2's local CPU integration are
implemented. Milestone 3 now has static worker-pool routing and bounded admission, checked
against two real CPU model servers. Its health-probe and local draining increment is now
implemented and verified through real process failures and recovery. Milestones 4 through 9
remain planned. CPU checks establish mechanisms, not GPU capacity.

## Problem and first user

Target a developer or small infrastructure team serving one model from multiple replicas.
Use a document-question-answering workload: shared instruction/document prefixes, distinct
questions, varied response lengths, and bursts. Do not build a RAG application as part of this project.

Working hypothesis: a router can trade cache reuse against queue delay to improve the fraction
of requests meeting a declared latency objective at fixed serving capacity. This is an
experiment, not a promised improvement or a claim of algorithmic novelty.

Before optimizing, obtain a permissioned workload from an actual application or reproduce
a documented workload. Record who would use this, their current setup, and a measurable pain
point. Synthetic traffic is useful for controlled tests but does not establish user demand.

## 0. Service foundation — initial increment

Build a small Go HTTP process, health endpoint, explicit listener configuration, and bounded
graceful shutdown. Document how to run it. No inference endpoints yet.

Evidence: build, tests, vet, and a real-process HTTP smoke check. Learning: address versus
port, process versus server, handler, concurrency, liveness versus readiness, shutdown.

## 1. Trace one request through one backend

Add a controllable fake backend and a gateway forwarding a documented subset of the
chat-completions protocol, including streaming. Bound request sizes and deadlines. Forward
cancellation and preserve response status. Do not silently retry generation.

Evidence: streamed chunks arrive before completion; a slow/disconnected client does not
leave an unbounded queue; malformed input is rejected; cancelled upstream work releases
capacity. Use network tests against separate HTTP servers.

Learning: HTTP, TCP connections, streaming, contexts, backpressure, transport failures.

Build this milestone in three small increments:

1. Run a controllable worker directly. It emits delayed, labelled test chunks over HTTP;
   observe the first chunk before the last one is generated. These are synthetic chunks,
   not model tokens or evidence of inference performance.
2. Forward that same stream through the gateway. Compare the direct and forwarded responses
   and verify that the gateway does not buffer the whole response before sending it.
3. Disconnect the client during a long response. Verify that the gateway cancels its upstream
   request and the worker observes cancellation and stops producing chunks. Also test a worker
   failure mid-stream, deadlines, and slow-reader behavior before closing the milestone.

All three increments are implemented for the synthetic worker; see the
[worker walkthrough](learning/01-streaming-worker.md) and
[gateway walkthrough](learning/02-gateway-streaming.md). The next milestone is real inference
integration on available hardware, now exercised separately in milestone 2. Tests of the
fixture alone do not establish model-engine behavior.

## 2. Connect real inference and establish a baseline

Use an existing model engine rather than write a new inference runtime. vLLM is the initial
GPU candidate; confirm hardware and engine support before pinning a version. A small CPU
backend may validate integration locally. No paid compute is provisioned automatically.

Explain tokenization, prefill, decode, KV cache, batching, and memory limits from an actual
request trace. Record engine/model versions and hardware. CPU integration cannot substantiate
GPU throughput claims. Fake workers remain explicitly labelled test fixtures.

Evidence: real streamed generation, cancellation integration, and direct-versus-gateway latency
measurements. Validate engine-side work termination separately from client disconnection.

Implemented locally with pinned llama.cpp b12110 and SmolLM2-135M-Instruct Q4_K_M on CPU;
see the [real inference walkthrough](learning/03-real-inference.md). The checker records
direct/gateway timings and observes the engine slot becoming idle after disconnect. GPU
integration and load/capacity measurement remain future work.

## 3. Route across a worker pool and bound overload

Add round-robin and least-inflight baselines, per-worker in-flight accounting, health probes,
bounded admission, and draining. Begin with one gateway and static worker addresses. Reject
excess work explicitly (503 with retry guidance); avoid an unbounded waiting room.

Evidence: kill/slow/drain a worker during traffic; healthy workers continue to serve new requests;
in-flight counts return to zero after completion and cancellation; all-unhealthy produces a
bounded failure. Report partial stream failures without pretending generation was resumed.

Learning: partial failure, unreliable failure detectors, load balancing, Little's Law,
queueing, admission control, retries and duplicate execution.

First increment complete: atomic worker selection/reservation, round-robin and least-inflight,
per-worker admission limits, immediate overload rejection, and capacity release on all exits.
See [the real worker pool experiment](learning/04-worker-pool.md). The second increment is
also complete: probe-based exclusion/recovery, readiness, and local drain/resume operations.
See [the real health and draining experiment](learning/05-health-and-draining.md). Durable
or fleet-wide administration remains part of the later control-plane milestones.

## 4. Test cache locality against load

Measure whether repeated prefixes matter for the selected workload. Add a simple affinity
policy only after baselines exist, then a load escape rule. Estimated cache presence must
be labelled estimated; actual cache eviction/restart can invalidate it. Do not cache answers.

Evidence: compare round-robin, least-inflight, affinity, bounded-load affinity, and a suitable
existing router. Keep weights, hardware, arrival schedule, cache state, and concurrency
comparable. Include hot-prefix shifts, long requests, worker restarts, and heterogeneous load.
Report policies that lose as well as those that win.

Learning: locality, hashing, skew, prefill/decode interference, throughput versus tail latency.

## 5. Make fleet configuration durable

Introduce a small control API for model-to-worker assignments and drain state. Use versioned
compare-and-swap updates and one durable state machine before replication. Write down when an
acknowledged update survives restart; test torn/truncated storage records and retries.

Gateways consume snapshots and retain an explicitly bounded-age local view. Transient load
and health observations stay separate from desired configuration. A drain operation needs
defined propagation semantics; committing it does not mean every gateway has applied it.

Evidence: restart the controller without losing acknowledged changes; stale concurrent updates
are rejected; a gateway can resync after missing updates; expiry of its view is visible.

Learning: write-ahead logging, fsync, crash recovery, idempotency, revisions, state machines.

## 6. Understand and integrate Raft

Purpose: administrative changes should survive a controller failure and have one committed
order despite concurrent writers. Use a fixed three-node configuration initially. Build the
educational implementation in small increments: elections, replication, durable term/vote/log,
commit/apply rules, recovery, then snapshots. Read the [Raft paper](https://raft.github.io/).

First test the state machine through a deterministic transport and clock, then through real
processes. Integration is gated on safety tests; the functioning single-controller service
remains useful while the educational Raft implementation is incomplete. An established Raft
library is the alternative for operational use and should be discussed explicitly.

Evidence: one leader per term; no conflicting applied entries at an index; no loss of acknowledged
updates under supported crashes; isolated minorities cannot acknowledge writes; leader failover
with a majority; recovery from persisted state; stale-leader reads cannot masquerade as fresh.
Check concurrent control histories against a sequential model. Fault tests must detect seeded bugs.

Learning: terms, elections, majority quorum, log matching, commit versus apply, linearizability,
network partitions, persistence ordering, safety versus liveness. Dynamic membership is deferred.

## 7. Exercise the whole system and decide what is useful

Run multiple gateways and inference workers with a three-node controller. Test stale views,
controller partitions, worker loss, slow clients, and rolling drains. Explain that per-gateway
in-flight counters do not impose a fleet-wide limit; use worker admission as the final bound.

Evidence: a reproducible demo, configuration-change correctness checks, and a report comparing
serving behavior during failures. Keep same-machine process failures separate from multi-host
network/storage failures. Document authentication and deployment requirements before exposing
anything outside local development.

Identify a user-confirmed improvement or document that existing systems are sufficient and
select a concrete upstream issue. Publish only measured claims, including limitations.

## 8. Durable batch scheduling — later extension

Gate: the serving path works and a concrete offline workload needs restartable jobs, such as
summarizing a collection of documents. Record why ordinary client-side request submission is
insufficient before implementing a scheduler. Reuse the worker fleet with explicit capacity
isolation so batch traffic cannot silently consume all interactive capacity.

Start with submit, claim, and completion states in one durable queue. Then introduce worker
leases, retry policy, idempotent submissions, and monotonically increasing fencing tokens.
Specify cancellation, terminal failures, retention, and admission limits. Reuse the durable
state-machine and replication work where its guarantees fit.

Evidence: restart after accepting a job without losing it; retry a timed-out submission without
creating another job; pause a worker beyond its lease and reject its stale completion after
reassignment. Distinguish repeated execution from one accepted result: fencing the completion
record does not make arbitrary external side effects exactly-once.

Learning: durable scheduling, leases, fencing, retry backoff, idempotency, fairness, and the
difference between at-least-once execution and deduplicated result publication.

## 9. Shard the job queue — conditional on a measured bottleneck

Gate: benchmark the durable scheduler and identify queue storage or scheduling coordination
as a capacity limit. First consider simpler indexing, batching, and independent queues. Record
why partitioning is justified. If no bottleneck appears, keep sharding as an isolated learning
experiment and label it accordingly.

Partition job ownership across a fixed number of shards before adding live rebalancing.
Define assignment revisions, ownership epochs, routing, and transfer of deduplication state.
A queue partition on one replicated group does not by itself distribute storage or consensus
work; explain the boundary before adding independent replicated shard groups.

Evidence: ownership changes under concurrent submissions and claims do not lose accepted jobs;
stale owners cannot publish completions; retried submissions remain deduplicated across moves;
recovery resumes an interrupted transfer. Measure skew and rebalancing cost alongside throughput.

Learning: partitioning, ownership, hot shards, migration protocols, and consistency during
reconfiguration. Model tensor/pipeline parallelism remains the inference engine's responsibility.

## Experiment contract

- Record the commit, command, dependencies, hardware, model, workload, seeds, and raw results.
- Define a latency objective before evaluating policies. Include time to first token and
  completion latency; measure inter-token latency if the workload needs it.
- Measure offered, admitted, completed, rejected, failed, and cancelled requests separately.
- Count successful requests within the declared objective (goodput), alongside total throughput.
- Use an open-loop arrival schedule to expose overload; record generator lag. Closed-loop
  tests answer a different question and must be labelled.
- Repeat trials, randomize policy order, control warm/cold cache conditions, and report variation.
- Report sample counts and timeouts; do not hide failures by taking percentiles only over successes.
- Keep synthetic, CPU-model, and GPU-model measurements in separate tables.

## Learning loop

For each small increment: state the problem; draw one request or failure trace; implement it;
run a counterexample; explain the result; record the evidence; commit the change.
Exercises are invitations to build understanding, not a claim that understanding has already
been demonstrated. Do not generate first-person accounts of work the owner did not perform.

## Scope boundaries

No custom tensor runtime, model training, sharded general-purpose database, semantic answer
cache, Kubernetes operator, or speculative hedging in the initial scope. Each future addition
needs a measured problem and a decision record. No guarantee of a hiring outcome.
