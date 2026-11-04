# Milestones

Progress is based on demonstrated behavior and understanding, with no calendar estimates.
Only the initial foundation is implemented. Every later milestone below is planned.

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

## 2. Connect real inference and establish a baseline

Use an existing model engine rather than write a new inference runtime. vLLM is the initial
GPU candidate; confirm hardware and engine support before pinning a version. A small CPU
backend may validate integration locally. No paid compute is provisioned automatically.

Explain tokenization, prefill, decode, KV cache, batching, and memory limits from an actual
request trace. Record engine/model versions and hardware. CPU integration cannot substantiate
GPU throughput claims. Fake workers remain explicitly labelled test fixtures.

Evidence: real streamed generation, cancellation integration, and direct-versus-gateway latency
measurements. Validate engine-side work termination separately from client disconnection.

## 3. Route across a worker pool and bound overload

Add round-robin and least-inflight baselines, per-worker in-flight accounting, health probes,
bounded admission, and draining. Begin with one gateway and static worker addresses. Reject
excess work explicitly (503 with retry guidance); avoid an unbounded waiting room.

Evidence: kill/slow/drain a worker during traffic; healthy workers continue to serve new requests;
in-flight counts return to zero after completion and cancellation; all-unhealthy produces a
bounded failure. Report partial stream failures without pretending generation was resumed.

Learning: partial failure, unreliable failure detectors, load balancing, Little's Law,
queueing, admission control, retries and duplicate execution.

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
