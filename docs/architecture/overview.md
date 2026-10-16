# Orbit Testing architecture

`orbit-testing` provides an in-process asynchronous client that drives an Orbit ASGI callable
through HTTP and lifespan protocol messages. It does not open network sockets or install a second
web framework. It is useful for route, middleware, auth, dependency, response, and application
lifecycle tests that should exercise Core's actual ASGI boundary.

```text
test ── TestClient ── ASGI scope/receive/send ── ASGIApplication ── Orbit application
                     └── lifespan startup/shutdown
```

The client owns one application lifespan task and is single-use. Entering the async context sends
startup and requires a successful lifespan acknowledgement. Leaving sends shutdown and waits for
the application task. Startup/shutdown failures surface in the test; shutdown is bounded by the
configured lifespan timeout.

## Request model

`request()` sends one complete buffered body frame, then returns `http.disconnect` on subsequent
receive calls. It builds an origin-relative ASGI request scope, lowercases request header names,
preserves repeated header pairs, and checks Core's public path, query, and header bounds before
building the scope. URL fragments and absolute URLs are rejected instead of silently normalized.
Request method names are validated as bounded HTTP tokens.

This request model does not emulate a TCP connection, partial request-body streaming, browser
cookies/cache, network timing, TLS, proxy forwarding, client IP selection, or socket backpressure.
Use a real HTTP client against the production server/ingress for those properties.

## Response model

The client requires response-start before body frames, rejects duplicate starts and frames after
completion, and requires a final body frame. It captures repeated response headers through Core's
`Headers` type; ASGI names are decoded as ASCII and field values with the HTTP Latin-1 mapping.
Captured status/body/header values are validated by `TestResponse`.

Default capture limits are 16 MiB and 65,536 body frames. Tests may configure lower limits or raise
them to hard ceilings of 1 GiB and 1,000,000 frames. The body is fully buffered in memory. This
client is not suitable for large-transfer load testing or indefinite streaming tests.

## Lifespan and cleanup

Application startup and shutdown events are sent over a dedicated task. If startup fails, the
context cannot be entered. If shutdown fails or exceeds its timeout, cleanup errors fail the test.
Always use `async with` so app resources are stopped even when assertions fail. Construct a fresh
application and `TestClient` for each test that needs isolated lifecycle state.

## Local development services

`ContainerSpec` is an immutable, bounded description of one image, container TCP port, and
environment. `ContainerService` invokes the installed Docker or Podman CLI with an argument vector
and no shell, binds the assigned host port only to loopback, and waits for TCP readiness. It owns
one detached `--rm` container, retries failed stops while retaining its identifier, and removes an
orphan by a unique label if startup is cancelled before the engine returns its ID. There is no
container engine dependency in Core or the package. Each container starts with CPU, memory, and
process-count limits; engine-specific provisioning and image trust remain host responsibilities.

The test client and local service helpers do not establish production server conformance,
performance, transport security, or release readiness.
