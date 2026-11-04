# Increment 3: replace synthetic chunks with real model generation

The gateway already speaks HTTP and forwards SSE responses. Connecting a compatible engine
requires configuration, not a new forwarding implementation:

```text
Client -> Go gateway -> llama.cpp -> SmolLM2 model weights on CPU
       <- SSE stream <- generated text
```

The initial machine has an Intel i5-1235U, Intel Iris Xe graphics, and about 8 GB RAM.
We selected a small CPU model to validate integration without paid infrastructure. This
does not establish GPU throughput, multi-machine reliability, or model answer quality.

## Setup on this Windows x64 machine

From the repository root, run:

```powershell
python scripts/setup_llama.py
```

This downloads about 130 MB: the pinned llama.cpp b12110 CPU archive and the roughly 105 MB
SmolLM2-135M-Instruct Q4_K_M GGUF model. Both SHA-256 hashes are checked. The model repository,
immutable revision, download URLs, and hashes are recorded in `config/llama-cpu.json`.
Files live under ignored `.tools/`; weights and engine binaries are not committed.
The setup script needs Python 3.11 or later and is specific to Windows x64.

In terminal one:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_llama.ps1
```

In terminal two:

```powershell
$fleetGo = "$env:LOCALAPPDATA\inference-fleet\toolchains\go1.27.3\go\bin\go.exe"
& $fleetGo run ./cmd/gateway -listen 127.0.0.1:18080 -backend http://127.0.0.1:18081 -request-timeout 60s
```

If Go is on PATH, `go run` works instead. In terminal three:

```powershell
curl.exe --no-buffer -H "Content-Type: application/json" --data-binary "@examples/real-stream.json" http://127.0.0.1:18080/v1/chat/completions
```

This time the text comes from model inference. The tiny model can produce poor or incomplete
answers; a successful integration means correctly transporting the generated stream.
Both services bind to loopback. These commands provide no authenticated external deployment.

For other systems, obtain an appropriate engine build from the pinned release and verify
the model hash. The Windows setup script and CPU results do not establish portability.

## What the engine does

1. **Tokenization:** converts message text and the chat template into token IDs. Tokens need
   not correspond to whole words; network chunks need not correspond one-to-one to tokens.
2. **Prefill:** processes the prompt and builds attention key/value state, the KV cache.
3. **Decode:** predicts the next token repeatedly, reusing KV state for previous tokens.
4. **Streaming:** exposes generated text as events that our gateway forwards.

Model weights and KV state both consume memory. The launch command fixes the context to
2048 tokens, uses four CPU threads, and permits one active generation slot. This experiment
does not exercise continuous batching across multiple active requests. Prompt reuse between
requests is disabled for the baseline; KV reuse within a generation is still part of decoding.

## Reproduce the checks

With the two services running and no other clients, run:

```powershell
python scripts/check_real_inference.py
```

The script makes one warmup request per route, then shuffles three direct and three gateway
requests with seed 7. It requests 32 output tokens with `ignore_eos=true` to keep the work
comparable. That engine-specific option is for the experiment; it can force generation past
natural stopping points. Prompt caching is disabled. Raw samples, generated text, token usage,
request parameters, pinned asset details, and the source commit go to
`artifacts/real-cpu/results.json`.

Time to first token (TTFT) measures from starting the client request to receiving its first
nonempty generated-content event. Completion time ends at `[DONE]`. The engine is already
loaded, so neither metric includes downloading weights or loading the model. These are
sequential, same-machine integration measurements, not a load or capacity benchmark.

The cancellation check requests 1024 tokens with EOS stopping disabled, receives initial
content, and confirms the engine's slot is still busy before closing the gateway client.
It then polls the engine's `/slots` endpoint directly and requires an idle slot within five
seconds. That checks engine capacity release in addition to the client's connection closing.
Inspect engine logs for its cancelled task. The check does not promise instantaneous
interruption of an in-progress CPU operation.

Only pinned local-model checks run here. Ordinary CI continues to use the deterministic
test worker and does not download weights or require a GPU.

## Explain the result

- Why can a long prompt increase TTFT even if the requested answer is short?
- Which memory belongs to model weights, and which depends on the active sequence?
- Why is closing an HTTP connection alone insufficient evidence that inference stopped?
- Why can three gateway measurements appear faster than three direct ones without proving
  that adding a proxy improves performance?

Next: add a small worker pool and bounded admission. We now have a real backend on which to
observe routing and overload, while retaining the synthetic worker for controlled failures.

References: [pinned engine documentation](https://github.com/ggml-org/llama.cpp/blob/b12110/tools/server/README.md),
[model repository](https://huggingface.co/lmstudio-community/SmolLM2-135M-Instruct-GGUF/tree/04f7f8a05a8e4bc208e88d11e8c9122be36c2f9b).
