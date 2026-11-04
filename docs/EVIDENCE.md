# Evidence ledger

The gateway now forwards streaming requests to one configured synthetic worker. Milestone 1
is implemented for that fixture; real inference, routing, and milestones 2 through 9 remain
unimplemented. Historical entries below describe the checks at their original increments.

## Single-worker gateway validation

Environment: Windows amd64, Go 1.27.3; Python 3.12 for executable smoke checks.

- `go test ./...` and `go vet ./...`: passed.
- `go test ./internal/gateway -v` reproduces the committed network checks: first-chunk
  delivery before worker completion, disconnect propagation through two HTTP connections,
  direct-versus-forwarded complete content, preserved worker rejection without application
  retry, no-backend 503, unreachable-worker 502, and pre-stream timeout 504.
- The same suite checks a worker aborting mid-stream: the client receives a read error and
  no fabricated completion marker. Invalid JSON and oversized requests never reach a worker.
- A non-reading-client test floods the stream until socket buffers fill, then verifies that
  the gateway handler and worker return within bounded waits. This is a cleanup assertion,
  not an application-level memory or fleet admission benchmark.
- `go build -o bin/gateway.exe ./cmd/gateway` and
  `python scripts/smoke.py bin/gateway.exe`: passed. The updated smoke check expects 503
  for generation without a configured worker; liveness and occupied-port checks still pass.
- A manual Python HTTP client against separate gateway and worker executables received a
  full stream, then disconnected a second stream after its first chunk. The worker logged
  `outcome=cancelled chunks=1`. Stopping the worker made a subsequent gateway request return
  502. The committed tests above reproduce these behavior checks using separate HTTP servers.

Local race detection was not run; Linux CI requests it. Active-stream gateway shutdown,
real model cancellation, and performance under load have not been established here.

## Direct streaming worker validation

Environment: Windows amd64, Go 1.27.3, Python 3.12. No external Go dependencies.

- `go test ./...`: passed. Worker tests use real HTTP connections to check incremental
  delivery, prompt cancellation during a long inter-chunk wait, complete SSE framing, and
  rejection of malformed, oversized, unsupported, and out-of-range requests.
- `go vet ./...`: passed.
- `go build -o bin/testworker.exe ./cmd/testworker`: passed.
- A local Python HTTP client against the compiled executable received a complete three-chunk
  response, then disconnected a second request after its first chunk. The worker log recorded
  `outcome=cancelled chunks=1`. This was a manual executable smoke check, not a committed test runner.

The committed network tests reproduce the streaming and cancellation assertions with
`go test ./internal/testworker -v`. Local race detection was not run; Linux CI requests it.
No slow-reader saturation, gateway propagation, inference-engine cancellation, or performance
claim is established by these checks.

## Foundation validation

Environment: Windows amd64, Go 1.27.3, Python 3.12; Ubuntu under WSL for the Linux process check.
The Go archive was verified against its official download checksum. No external Go dependencies.

| Check | Result |
| --- | --- |
| `go vet ./...` | Passed |
| `go test ./...` | Compiled successfully; no Go test files yet |
| `go build -o bin/gateway.exe ./cmd/gateway` | Passed on Windows |
| `python scripts/smoke.py bin/gateway.exe` | Health JSON, generation 404, method 405, occupied-port failure passed |
| Cross-compile with `GOOS=linux GOARCH=amd64 go build -o bin/gateway-linux ./cmd/gateway` | Passed |
| `wsl -d Ubuntu -- python3 /mnt/d/inference-fleet/scripts/smoke.py /mnt/d/inference-fleet/bin/gateway-linux` | HTTP checks and SIGTERM shutdown passed |

SIGTERM was checked with no active requests. Graceful draining of active inference streams
is not yet tested because inference forwarding is not implemented. Windows smoke testing
skips graceful shutdown; terminating a Windows subprocess is not the same as delivering SIGTERM.
Local race detection was not run. The Linux CI workflow requests race detection, but with
no Go tests this does not establish exercised race coverage.

No inference, routing, cache, or consensus performance results exist for this repository.

Future experiment entries must include the commit, exact command, environment, raw artifacts,
outcome, limitations, and whether inference was synthetic, CPU, or GPU.
