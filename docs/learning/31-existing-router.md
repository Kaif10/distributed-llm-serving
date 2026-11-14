# 31: Compare an existing HTTP router on the same real inference workload

The early experiments compared policies inside our own gateway. That cannot say
whether a simpler established proxy already provides the useful routing behavior.
The experiment runners now accept NGINX round-robin and consistent-hash baselines.
The engines, weights, prompts, output lengths, warming, and arrival schedules are
shared. Each condition starts fresh engines; randomized condition order is recorded.

NGINX is a general HTTP router, not an LLM scheduler. Its consistent hash over the
same `X-Prefix-Key` is an established locality mechanism. Unlike our rendezvous hash,
it can map these three documents to different worker combinations. We preserve
observed assignments and cache-token accounting instead of selecting favorable keys.
This comparison does not substitute for a GPU-backed vLLM Production Stack study.

The helper saves the exact rendered configuration and executable SHA-256. It uses
one owned foreground process, one upstream connection per engine, no upstream
generation retries, response buffering off, and body buffering before dispatch.
An unavailable upstream's local 502/503 is normalized to 503 with `Retry-After: 1`.
We do not enable NGINX passive failure ejection during these healthy-worker tests.
These are configured comparison conditions, not assertions of identical internal
admission or health semantics.

On Windows, download the pinned official archive and verify the recorded checksum:

```powershell
$pin = Get-Content config/nginx-baseline.json | ConvertFrom-Json
Invoke-WebRequest -Uri $pin.archive_url -OutFile .tools/nginx-baseline.zip
if ((Get-FileHash .tools/nginx-baseline.zip -Algorithm SHA256).Hash.ToLower() -ne $pin.archive_sha256) { throw 'NGINX checksum mismatch' }
Expand-Archive .tools/nginx-baseline.zip -DestinationPath .tools -Force
go build -o bin/gateway.exe ./cmd/gateway
python scripts/bench_cache.py --trials 2 --repeat 12 --conditions least-inflight prefix-affinity nginx-round-robin nginx-hash --nginx .tools/nginx-1.28.0/nginx.exe --output-dir artifacts/router-cache
python scripts/bench_load.py --requests 64 --trials 2 --rates 1 4 --policies least-inflight prefix-affinity nginx-round-robin nginx-hash --nginx .tools/nginx-1.28.0/nginx.exe --output-dir artifacts/router-load
```

The archive checksum was computed after an official HTTPS download; no independent
PGP verification is claimed. Binaries and weights stay ignored. The sequential
command produces 96 requests per condition/trial; the fixed-arrival command includes
all rejected and failed requests in its denominator. A finished experiment is not
the same as a statistically established performance ranking. Raw trial records,
dispatch lag, successful-response latency, goodput, and losing outcomes matter.
The load runner marks trials invalid when maximum dispatch lag exceeds the declared
budget (100 ms by default). It preserves those trials and their failures; a laptop
pause must not silently turn a fixed-arrival test into a burst. Report any repeats
separately and never use an invalid arrival schedule to rank routers.

Published measurements belong in the evidence ledger with the exact source commit.
They remain small-model CPU experiments using constructed traffic on one laptop.
They cannot establish GPU scale or external application demand.

References: [NGINX upstream hash and max_conns](https://nginx.org/en/docs/http/ngx_http_upstream_module.html),
[NGINX proxy buffering and retries](https://nginx.org/en/docs/http/ngx_http_proxy_module.html),
[Windows implementation limitations](https://nginx.org/en/docs/windows.html),
[vLLM Production Stack routing](https://docs.vllm.ai/projects/production-stack/en/latest/use_cases/loadaware-routing.html).
