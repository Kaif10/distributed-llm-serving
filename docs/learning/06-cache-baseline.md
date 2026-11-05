# Increment 6: measure computation reuse before changing routing

Several questions about the same document share a long input prefix. A model server may
reuse the attention KV state for that prefix and process only the changed suffix. The
benefit depends on that state still being available on the selected worker.

We now measure this with real SmolLM2 inference, through the existing llama.cpp setup.
There is no answer cache: every request still generates a new response. The gateway has
not yet gained an affinity policy.

## Reproduce the experiment

After the pinned model setup, build the gateway and run:

```powershell
$fleetGo = "$env:LOCALAPPDATA\inference-fleet\toolchains\go1.27.3\go\bin\go.exe"
& $fleetGo build -o bin/gateway.exe ./cmd/gateway
python scripts/bench_cache.py --trials 3
```

The script uses available ports 18080 through 18082, owns its child processes, and cleans
them up after each condition. Commands, samples, generated text, engine token counts, and
logs go to `artifacts/cache-baseline/`. This is a Windows CPU experiment with real weights.

`examples/cache-workload.json` contains constructed documents and questions for controlled
measurements. They are not an application-user trace or evidence of product demand. Questions
change while the document text remains identical. The schedule is A, A, B, B, C, C, A, A.
Each request generates 16 tokens with EOS stopping disabled for comparable decode work.

## Four conditions

| Condition | Workers | Request placement | Cache behavior |
| --- | --- | --- | --- |
| Direct, cache disabled | 1 | Direct | Reprocess every prompt |
| Direct, cache enabled | 1 | Direct | Reuse matching prefix in the current slot |
| Round-robin | 2 | Existing gateway policy | Slot-local reuse enabled |
| Least-inflight | 2 | Existing gateway policy | Slot-local reuse enabled |

Each condition starts fresh engines with two CPU threads per engine, one slot each, and a
2048-token context. The engine's additional RAM prompt-cache store is disabled with
`--cache-ram 0` and `--no-cache-idle-slots`. This isolates reuse from each slot's current KV
state rather than allowing older documents to be restored from another cache tier.

Each engine runs one unrelated warmup request before measurement. Target document prefixes
are cold; small common chat-template prefixes may remain. Conditions are shuffled using
seed 7 in each trial, with three trials by default. Requests are sequential with no overlap.
Fresh processes reset cache contents between conditions and trials.

## What the measurements mean

The engine reports `usage.prompt_tokens_details.cached_tokens`. We record those alongside
total prompt tokens and actual worker identity, inferred from the changed engine task ID.
The script rejects missing usage, wrong output counts, ambiguous worker attribution, cache
reuse in the disabled control, or absent substantial reuse for the repeated direct prefix.
It also checks that an unrelated document does not reuse most of its prompt.

Cached-token fraction is total cached prompt tokens divided by total prompt tokens. TTFT
measures client elapsed time to first generated content and includes transport, scheduling,
and engine processing; it is not a pure prefill timer. Raw engine logs include prompt timings.
The disabled control helps distinguish cache reuse from mere runtime warmup. Timing alone
would not prove that computation was skipped.

With sequential requests, both pool policies usually see equal in-flight counts and rotate
ties. Adjacent questions about one document can land on different workers. Look at the
recorded worker IDs and cached-token counts to verify whether that happened. Direct serving
uses one worker; its comparison illustrates locality, not a superior production deployment.

## Limits and next experiment

The recorded run completed 96 requests across three trials. Direct serving reused 48.654%
of prompt tokens across the schedule with caching enabled, versus 0% with it disabled.
Both existing pool policies reused 0.577% on this sequential schedule. See the
[evidence ledger](../EVIDENCE.md) for raw results, timings, and the exact conditions.

An earlier run encountered a 60-second incomplete generation. That failure, its logs, and
a successful isolated replay are preserved separately. Its cause is unresolved. A later
successful run is not a fix, and this result cannot establish reliability under load.

This run has no concurrent arrival schedule, overload, latency objective, or affinity policy.
It cannot show that cache-aware routing improves throughput or tail latency, or that these
results generalize to GPUs. It also does not evaluate answer quality.

Next, add an explicit prefix-affinity policy with a capacity escape rule, then compare it
against these baselines under repeated-prefix traffic and skew. Keep estimated cache presence
distinct from the engine's measured reuse, and report when affinity loses to load balancing.
Carry the unresolved timeout forward with diagnostics in those experiments. Use
`--output-dir` to preserve separate runs and `--conditions` to isolate a specific condition.

- Why does repeating a document on another worker miss a local cache?
- Why can unrelated prompts still share a few cached tokens?
- Why can a high cache-hit rate coexist with poor latency under load?
- What extra experiment would separate queue delay from prefill computation?

Engine semantics: [pinned llama.cpp server documentation](https://github.com/ggml-org/llama.cpp/blob/b12110/tools/server/README.md).
