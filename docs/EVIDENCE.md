# Evidence ledger

The foundation is a local HTTP process with a liveness endpoint. It is not yet an inference
gateway or a distributed system. Milestones 1 through 7 are unimplemented.

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
