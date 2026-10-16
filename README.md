# Orbit Testing

`orbit-testing` provides an in-process client for testing Orbit applications through the real
ASGI protocol, without opening sockets or installing another web framework.

```bash
pip install orbit-core orbit-testing
```

```python
from orbit import Application, ApplicationConfig
from orbit.asgi import ASGIApplication, Response
from orbit_testing import TestClient

application = Application(ApplicationConfig(name="example"))


@application.router.route("/", name="home")
async def home(request):
    return Response.text("hello")


async def test_home():
    async with TestClient(ASGIApplication(application)) as client:
        response = await client.request("GET", "/")
    assert response.status == 200
    assert response.body == b"hello"
```

The client sends ASGI lifespan startup and shutdown messages, bounds request path/query/header
inputs using Core's public HTTP limits, preserves repeated response headers, and rejects malformed
response framing. It accepts origin-relative request targets only, lowercases ASGI request header
names, and decodes response header values using HTTP's Latin-1 mapping. Captured responses default to a 16 MiB body limit and 65,536 frames; applications
can lower or raise these within the hard 1 GiB and 1,000,000-frame bounds. Its requests are fully
buffered and it models a client disconnect after the request body; it is not a browser, socket
client, load generator, or substitute for host-level integration tests. Use
`TestClient(app, scheme="https")` to exercise TLS-gated handlers with an HTTPS ASGI scope; this does
not establish TLS or validate certificates, reverse proxies, or ingress configuration.

## Local development services

`orbit-testing` can start an explicitly configured Docker or Podman container for integration tests.
The engine must already be installed and running; this package does not install or configure it.

```python
from orbit_testing import ContainerService, ContainerSpec

redis = ContainerService(
    ContainerSpec(image="redis:7.4.2", container_port=6379),
    engine="docker",  # or "podman"
)
async with redis as service:
    host, port = service.endpoint.rsplit(":", 1)
    # Connect a client to host and int(port), then exercise the integration.
    ...
```

The helper binds a random host port to `127.0.0.1`, defaults to one CPU, 512 MiB of memory, and 256
processes per container, waits for TCP readiness within a configured deadline, and stops the
container when the async context exits. Resource limits can be adjusted within validated bounds.
Use immutable image tags or digests for repeatable tests. Environment values are passed through the engine process environment, never
interpolated into shell commands or command-line arguments; exceptions omit engine output. The
helper is for trusted local images and does not create cloud resources or validate provider
behavior. Access to a Docker or Podman engine can grant broad host control, so only run trusted
images and test code.

## Status

This package is alpha; its API is not stable and it is not yet a published release. It supports
Python 3.11 through 3.14.

## Workspace development

For the Orbit multi-repository development workspace, install this package editable before running
Core or sibling integration tests:

```bash
python -m pip install -e ../orbit-testing
```

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

Licensed under Apache-2.0.

