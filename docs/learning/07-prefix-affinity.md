# Increment 7: prefer locality without waiting for a full worker

The `prefix-affinity` policy uses rendezvous hashing to rank worker origins for a supplied
`X-Prefix-Key`. It selects the highest-ranked healthy, undrained worker with capacity.
Selection and reservation still share one mutex. If all workers are unavailable, it returns
503. A missing key falls back to least-inflight with rotating ties.

The client supplies a stable label for a shared prefix, for example `model-v1-document-A`.
Keys are limited to 256 bytes, removed before forwarding, and are not logged. This is an
explicit application hint, not automatic prefix detection or a claim that cache contents
are known. Scope labels by model/version and document revision. Unrelated prompts with the
same label can lose locality; the engine still validates actual token prefixes before reuse.
No response or prompt text is stored by the gateway.

```powershell
go run ./cmd/gateway -backends http://127.0.0.1:18081,http://127.0.0.1:18082 -policy prefix-affinity -max-inflight 1
curl.exe --no-buffer -H "Content-Type: application/json" -H "X-Prefix-Key: model-v1-document-A" --data-binary "@examples/real-stream.json" http://127.0.0.1:8080/v1/chat/completions
```

The engine processes need prompt caching enabled for locality to reduce prefill work. The
older `run_llama.ps1` baseline disables caching intentionally; use the experiment below to
launch the required cache-enabled configuration. Hashing a label does not create engine cache.

## Real-model checks

```powershell
go build -o bin/gateway.exe ./cmd/gateway
python scripts/check_real_pool.py
python scripts/bench_cache.py --trials 3 --conditions least-inflight prefix-affinity --output-dir artifacts/affinity-comparison
```

Use the local Go executable path from earlier walkthroughs if Go is absent from PATH.
The first script now includes a hot-prefix burst against two real engines: one stream uses
the preferred worker, the next uses available capacity on the other, and a third receives
503. Disconnecting releases engine slots, and new generation succeeds. Controlled tests
also check stable placement and exclusion of full, unhealthy, and draining workers.

The second script compares cache-enabled policies using identical sequential document
requests, fresh engines per condition/trial, shuffled policy order, and engine token counts.
It sends the same prefix labels under both policies, so only placement differs. Actual
workers are inferred from engine task IDs, not from the gateway's preferred worker estimate.

A returned preferred worker can have an empty or evicted cache. Falling back can duplicate
prefill computation on multiple workers. Higher cache reuse alone does not establish higher
goodput or better tail latency; a concurrent arrival and skew experiment remains required.

## Failure diagnostics

Forwarding-end logs include the worker index, elapsed time, and context error. They report
termination, not successful generation. An incomplete stream still fails the experiment.
The cache benchmark now preserves the failing request, elapsed time, and engine slot snapshots
alongside raw logs. Observations happen after the failure and cannot reconstruct every
intermediate engine state. The earlier 60-second timeout remains unresolved.

Next: compare these policies under a fixed concurrent arrival schedule and changing hot
prefixes. Record offered/admitted/completed/rejected work and latency objectives, including
failures, before drawing a policy-performance conclusion.
