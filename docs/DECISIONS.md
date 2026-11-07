# Decisions

## 008: Finish recovery integration before expanding scope

The product remains reliable routing across model replicas. Each added mechanism needs
an observable failure or a measured constraint, a simpler alternative, and a reproducible
check of the resulting behavior.

| Mechanism | Failure or constraint addressed | Current evidence/boundary |
| --- | --- | --- |
| Bounded admission and streaming cancellation | Overload and abandoned requests consume worker capacity | Real CPU-engine admission and cancellation checks |
| Durable desired configuration and successful request IDs | Restart loses administrative changes; uncertain retries repeat them | Journal recovery, CAS, and original retry-result checks |
| Replicated configuration, fresh reads, gateway peer fallback | A controller dies or an isolated old leader cannot confirm current state | Real peer failures/partitions and a real-model gateway failover run |
| Checkpoint codec and publication journal | Future prefix removal must retain configuration and successful retry history | Offline recovery and process-kill tests; live integration and log trimming remain pending |

Custom consensus and checkpoint internals are an explicit educational investment. They
add implementation and maintenance cost; their existence does not establish that an
operational deployment should replace an established implementation. Same-host correctness
checks do not establish multi-host resilience or large-scale inference capacity.

The next bounded change is controller-owned checkpoint capture and recovery while retaining
the full Raft log. Completion requires an actual controller restart to recover the same
configuration and historical retry results as full replay, reject invalid boundaries, and
preserve gateway freshness behavior. Do not add another independent subsystem as a substitute
for closing this integration loop.

Before extending retention or claiming a capacity improvement, record the current limits
and the failure being removed. Prototype bounds are resource guardrails, not evidence of
customer demand or a production bottleneck. Checkpoint publication remains capped and
compaction is not implemented; preserve those limitations in status and evidence.

After this recovery path is explainable end to end, use workload/capacity measurements and
selected multi-machine checks to decide the next investment. Durable scheduling requires
an actual offline workload; sharding requires its stated bottleneck gate. Dynamic membership,
deployment machinery, or additional scheduling policies do not enter merely to broaden the
architecture. Validation with an external application workload/user remains outstanding.

## 007: Explicit prefix hints with capacity-bounded affinity

Use a client-supplied X-Prefix-Key label and SHA-256 rendezvous hashing over worker origins
to obtain stable placement without storing a cache directory. Skip full, unhealthy, and
draining workers. Missing hints use least-inflight. The engine validates token reuse; hints
affect placement only and never authorize serving a cached answer.

Automatic prefix extraction would require an explicit message/token-prefix contract and
engine-aware cache observations. Keep that as a separate design step. Labels should include
model and document versions. The admission limit is local to each gateway, and the current
policy only escapes at that limit; it does not predict queue delay or prefill cost.

## 001: One product problem, incremental learning

Build reliable request routing for an inference fleet. Components enter when their role can
be explained through an operational failure or measured behavior. The project also teaches
distributed systems; educational components are explicitly identified.

Alternative: build storage, sharding, scheduling, and inference simultaneously. Rejected
because the result has too many independent correctness contracts for incremental learning.

## 002: Go service, existing inference engine

Use Go for network services and concurrency. Use an existing engine for tensor computation,
tokenization, batching, and KV-cache management. Python is available for later workload tooling
where useful. Start with the standard library and add dependencies for concrete needs.

Alternative: implement model execution ourselves. Rejected because it distracts from the
request-routing and distributed-control problem. No engine version is selected before checking
available hardware and a runnable integration. See decision 006 for the first verified backend.

## 006: Small pinned CPU backend for local integration

The available machine has about 8 GB RAM and Intel integrated graphics. Select llama.cpp
b12110's Windows x64 CPU release with SmolLM2-135M-Instruct Q4_K_M to validate real generation
within that budget. Preserve the existing gateway and speak the same chat-completions path.
Pin the archive checksum and model revision/checksum in config/llama-cpu.json.

vLLM remains a GPU candidate when suitable hardware is available. CPU integration is enough
for request-flow and engine cancellation checks; it cannot substantiate GPU serving or
multi-worker throughput claims. Existing Python ML packages remain untouched. The local
setup downloads verified artifacts into ignored .tools/ and requires no paid service.

## 003: Consensus governs configuration

Raft will replicate desired model/worker assignments and draining decisions. It will not
replicate tokens, every request, ephemeral load counters, or model caches. Start with a durable
single controller and introduce replication when studying controller failure.

A custom Raft implementation is an educational choice, not evidence that existing libraries
are unsuitable. Integrate only after safety checks. For operational deployment, compare the
maintenance and correctness costs against an established implementation.

## 004: Measure usefulness against existing routers

vLLM's production stack already combines cache locality with live load. Baselines must include
an existing implementation when the hardware supports a fair comparison. There is no novelty
claim. Improvements, negative results, and focused upstream fixes are all legitimate outcomes.

Reference: [vLLM load-aware routing](https://docs.vllm.ai/projects/production-stack/en/latest/use_cases/loadaware-routing.html).

## 005: Accurate evidence and normal history

Keep changes in ordinary, descriptive commits. Distinguish plans from implemented behavior;
record actual contributors and tools without inventing manual authorship or elapsed effort.
Correct mistakes explicitly. Preserve attribution for any future reused code. Do not fabricate
timelines, benchmark results, user adoption, or experience claims.
