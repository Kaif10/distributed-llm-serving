# Decisions

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
