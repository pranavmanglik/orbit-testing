# Orbit Testing operations

Use `TestClient` as an async context manager around each test scope:

```python
async with TestClient(ASGIApplication(application)) as client:
    response = await client.request("GET", "/health")
```

The context drives application startup before yielding and shutdown before exiting. It surfaces
lifespan failure acknowledgements and fails if the application exits without the protocol response.
The default startup/shutdown wait is 60 seconds; lower it in focused tests that need fast failure.
The client is single-use because application lifecycle state is single-use; construct a new client
and application for a new test.

## Starting local infrastructure

Use `ContainerService` as an async context manager around integration tests. Select a pinned image,
container port, and only the environment variables that service needs:

```python
service = ContainerService(
    ContainerSpec("redis:7.4.2", 6379), engine="podman", startup_timeout=45
)
async with service as redis:
    host, port = redis.endpoint.rsplit(":", 1)
    ...
```

The helper requires a working local Docker or Podman engine, publishes the port on `127.0.0.1`,
checks TCP readiness, and stops its container on context exit. Startup, engine commands, and stop
waits are bounded. If cleanup fails, call `close()` again; the service keeps the container handle
for retry. A startup cancelled during container creation attempts to locate the resource by its
unique label and remove it. Keep integration tests isolated and do not reuse a service instance
after it has closed.

Pin images by immutable version or digest. The test runner's access to the container engine is a
privileged host capability. Run trusted images only; do not mount host paths or expose service ports
outside loopback through custom engine configuration. The helper does not provision databases,
brokers, cloud accounts, or test data.

## Request and response capacity

Requests are fully buffered and limited by Core's public HTTP path, query, and header bounds. The
helper sends one `http.request` with `more_body=False`, then models disconnect. It lowercases header
names, preserves repeats, validates method tokens, and rejects absolute URLs and fragments.

Responses are fully buffered. Default limits are 16 MiB and 65,536 body frames, configurable from
zero bytes/one frame up to 1 GiB/1,000,000 frames. Raise them only when the test intentionally
covers large but finite output. The limit protects the test process; it is not an application
response limit or streaming backpressure mechanism.

## What requires host-level testing

Use the real server/client stack to validate socket behavior, partial/chunked request bodies,
streaming, long-lived connections, concurrency, TLS, proxies, forwarded identity, keep-alive,
client abort timing, and deployment-level request limits. The in-process harness deliberately does
not model those features or generate performance/load evidence.

When a test fails during shutdown, inspect application lifespan cleanup rather than ignoring the
exception. The client cancels a lifespan task that exceeds the configured timeout; applications
that suppress cancellation can still delay test-process termination.
