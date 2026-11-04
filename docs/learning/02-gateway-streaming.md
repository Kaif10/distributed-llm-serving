# Increment 2: carry a stream across two connections

We now connect the existing services:

```text
Client -- POST --> gateway -- POST --> test worker
Client <-- SSE --- gateway <-- SSE --- test worker
```

The gateway accepts a small JSON request, forwards it to one configured worker, and copies
the response incrementally. It does not perform model inference or choose among workers yet.

## Run the path

Run each command from the repository root in a separate terminal. With Go on PATH:

```sh
go run ./cmd/testworker -interval 300ms
```

```sh
go run ./cmd/gateway -backend http://127.0.0.1:8081
```

```powershell
curl.exe --no-buffer -H "Content-Type: application/json" --data-binary "@examples/stream.json" http://127.0.0.1:8080/v1/chat/completions
```

On Linux use `curl`. On the initial Windows machine, if Go is absent from PATH, replace
`go` with `& "$env:LOCALAPPDATA\inference-fleet\toolchains\go1.27.3\go\bin\go.exe"` in PowerShell.

The same eight test chunks arrive, now through port 8080. Compare with a request directly
to the worker on port 8081. These remain synthetic strings, not model-generated tokens.

## Disconnect across the extra hop

Add `--max-time 1` to the curl command. Curl exits with timeout code 28 and the worker should
log cancellation with fewer than eight chunks. A write failure may be observed instead if
the disconnect races with a send.

There are two connections. Closing the client's connection cancels the gateway request
context. The outgoing worker request inherits that context. Its cancellation closes that
connection, so the worker's context is cancelled too. This propagation is an explicit
property of how the gateway constructs its outgoing request.

## Read the implementation

`cmd/gateway/main.go` owns flags, the listener, and shutdown. `internal/gateway/handler.go`
owns validation and forwarding. It uses Go's `httputil.ReverseProxy` to manage transport,
hop-by-hop header handling, streaming, and propagation of cancellation.

The gateway buffers at most 64 KiB of request JSON to reject invalid syntax or excessive
size before dispatch. The worker still validates the supported request fields. The response
is copied through bounded buffers, flushed on writes, and is never collected in full.
Network buffering means one proxy read is not guaranteed to equal one SSE event.

## Failures and time bounds

| Situation | Client-visible behavior |
| --- | --- |
| Gateway started without `-backend` | Generation returns 503; health still responds |
| Invalid JSON / request larger than 64 KiB | 400 / 413 before dispatch |
| Worker rejects a request | Worker status, body, and applicable headers are forwarded |
| Worker cannot be reached before response headers | 502 |
| Forwarding deadline expires before response headers | 504 |
| Worker fails or deadline expires after streaming starts | Stream is aborted; no replacement status or fabricated `[DONE]` |

Try stopping the worker and submitting another request through the still-running gateway.
Before failure detection you may receive 502; after the health probe excludes the worker,
you receive 503. A green `/healthz` describes the gateway process, not worker readiness.

`-request-timeout` defaults to 30s and starts after the bounded request body is read. The
server separately limits request reading to 10s. Downstream writes have a fixed deadline of
the forwarding timeout plus one second, allowing a pre-stream 504 to be delivered. It is a
total budget, not an idle timeout reset for each chunk. Long legitimate generations will
need an appropriate configuration; this fixture is not a production timeout recommendation.

The gateway performs no application-level retries or failover. Once headers are sent it
cannot change HTTP 200 to 502. Clients must handle read errors and check the stream's expected
completion marker. Cancellation of this synthetic worker does not establish cancellation
behavior inside a future inference engine.

## Evidence and exercises

Run `go test ./internal/gateway -v`. Network tests cover direct-versus-forwarded content,
first-chunk delivery before completion, cancellation across both connections, worker rejection,
worker loss, pre-stream deadlines, truncated responses, and a client that never reads the
response. The latter fills socket buffers and checks that the gateway and worker return.

1. Explain why an HTTP status can report an error before streaming but cannot replace a
   status after the first event has been delivered.
2. Start the gateway with `-request-timeout 1s` and run the normal eight-chunk request.
   Does it contain `[DONE]`? Compare with `--max-time 1`, where the client triggers cancellation.
3. Explain which body is buffered and why buffering the entire response would break streaming.

Next milestone: choose a real inference engine that runs on available hardware, repeat these
integration checks, and establish a direct-versus-gateway baseline before adding routing.
