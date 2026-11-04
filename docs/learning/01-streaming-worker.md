# Increment 1: watch a response arrive in pieces

The problem: before forwarding a model response, we need to see whether bytes reach the
client as they are produced, and whether work stops when that client leaves.

Run from the repository root. With Go on PATH:

```sh
go run ./cmd/testworker -interval 300ms
```

On the initial Windows machine, the locally installed toolchain can also be used:

```powershell
$fleetGo = "$env:LOCALAPPDATA\inference-fleet\toolchains\go1.27.3\go\bin\go.exe"
& $fleetGo run ./cmd/testworker -interval 300ms
```

In a second terminal:

```powershell
curl.exe --no-buffer -H "Content-Type: application/json" --data-binary "@examples/stream.json" http://127.0.0.1:8081/v1/chat/completions
```

On Linux use `curl` in place of `curl.exe`. You should see eight JSON events containing
`test-chunk-1` through `test-chunk-8`, followed by a finish event and `data: [DONE]`.
The first chunk is immediate; each subsequent chunk waits 300 ms. The worker terminal logs
`outcome=completed chunks=8`. These strings are synthetic; the worker performs no model inference.

## Follow the request

```text
Client POST -> validate JSON -> emit chunk -> flush -> wait -> emit next chunk
                                                         |
Client disconnect -> request context cancelled -----------+-> handler returns
```

`data: ...` plus a blank line frames each server-sent event (SSE). SSE is the response format;
this example sends the request using POST. Flushing asks the HTTP server to send buffered
bytes now. The client's `--no-buffer` option prevents curl from hiding incremental output.
Neither setting by itself promises that an arbitrary proxy will forward immediately.

The worker uses a small subset of chat-completions fields: `model=test-worker`, nonempty
`messages`, `stream=true`, and `max_tokens` from 1 to 128. The field `max_tokens` controls the
number of synthetic content chunks here, not tokenizer output. Unknown fields, multiple JSON
objects, and bodies over 64 KiB are rejected with 400 before streaming starts. Prompt content
is ignored. This is a test fixture, not full API compatibility.

## Disconnect deliberately

Run the same request with a one-second client deadline:

```powershell
curl.exe --no-buffer --max-time 1 -H "Content-Type: application/json" --data-binary "@examples/stream.json" http://127.0.0.1:8081/v1/chat/completions
```

Curl exits with timeout code 28. The worker should log `outcome=cancelled` and a chunk count
below eight. A connection failure can instead be noticed during a write and logged as
`write_error`. Network timing determines exactly when disconnection is observed.

The handler waits on both a timer and the request context. That lets cancellation stop the
wait immediately, rather than waiting for the next scheduled chunk. Stopping the worker
itself cancels active streams and allows up to five seconds for handlers to return.

## What the tests establish

`go test ./internal/testworker -v` uses real loopback HTTP connections. One test sets the next
chunk an hour away, receives the first event, then cancels the request. The handler must return
within two seconds. This catches both whole-response buffering and a wait that ignores cancellation.
Other tests parse a completed stream and reject invalid requests before events are written.

This does not yet test a slow reader that fills network buffers, gateway forwarding, or model
engine cancellation. Those remain later increments. The executable has bounded headers,
request reads, and stream write duration; this fixture has no admission limit yet.

## Try and explain

1. Change `-interval` to `1s`. What changes about completion time and the first chunk?
2. Set `max_tokens` to 129. Why is the HTTP error still possible before streaming starts?
3. Explain the difference between flushing the response and cancelling the request context.

Next: put the gateway between this worker and the client, and preserve the same streaming
and cancellation behavior across the extra network connection.
