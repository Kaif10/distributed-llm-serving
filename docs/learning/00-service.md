# Increment 0: a request reaches a process

Read `cmd/gateway/main.go` alongside this note. This increment contains no model, routing,
or consensus. Its purpose is to make the network-service lifecycle concrete.

## Run it

With Go on your PATH, run `go run ./cmd/gateway` from the repository root. The default address
is `127.0.0.1:8080`. On the initial Windows machine, a verified local toolchain is also available:

```powershell
$fleetGo = "$env:LOCALAPPDATA\inference-fleet\toolchains\go1.27.3\go\bin\go.exe"
& $fleetGo run ./cmd/gateway
```

In another terminal, run `curl.exe -i http://127.0.0.1:8080/healthz`. You should receive HTTP
200 and a short JSON response. With no backend configured, generation now receives 503.

## Follow the request

1. The process binds a TCP listener. The address identifies the interface and the port.
2. The HTTP server accepts a connection and parses the request.
3. The multiplexer selects the handler using the method and path.
4. The handler writes a status, headers, and bytes back to the client.

Multiple connections may be served concurrently. There is no shared application state in
this handler. Later, admission counters and routing state will make concurrency relevant.

## Follow shutdown

Ctrl+C cancels the signal context. The server stops accepting new connections and lets
existing handlers finish within five seconds. A fresh context provides that grace period;
reusing the already-cancelled signal context would cancel shutdown immediately. If the grace
period expires, connections are closed and the process reports failure.

`GET /healthz` means the HTTP process can respond. A future readiness check must additionally
consider whether it can accept inference traffic. A responding process can still have no
available model servers.

The default write timeout fits a short health response. The streaming increment now sets
a separate deadline for generation; see the gateway forwarding walkthrough for that design.

## Try these experiments

- Start a second copy on the same port. Explain why it fails before announcing readiness.
- Start the second copy with `-listen 127.0.0.1:8081`. Both should now work.
- Request `/v1/chat/completions`. Explain why health can be green while generation is absent.
- Stop the first process. Explain the difference between a connection error and an HTTP 404.

Next increment: forward one streaming request to a controllable worker, then observe what
happens when the client disconnects. Do not skip directly to a cluster.
