# Orbit Testing security guidance

`TestClient` invokes application code in the test process. Treat test fixtures and captured
responses as real application data: they can include tokens, credentials, personal information,
or production-like records.

## Fixture and response handling

Use synthetic credentials and data wherever possible. Do not commit production secrets or copy
real user data into fixtures, assertion messages, snapshots, or CI logs. `TestResponse.body` and
`TestResponse.json()` expose response content directly; repr redaction is not provided or implied.
Avoid printing whole responses when a failure may contain sensitive values.

## Protocol fidelity and limits

The helper applies Core's path/query/header input bounds, bounds captured body/frame counts, rejects
invalid framing, and maps ASGI response header bytes to text according to the package contract.
These checks prevent accidental test-runner resource exhaustion; they do not secure the application
or validate a network server. Test malformed requests deliberately when testing Core validation,
but do not mistake an in-process test for a firewall, TLS, proxy, or production input control.

Use the host's real authentication configuration in integration tests where practical, while keeping
credentials ephemeral and isolated. Do not disable authorization in tests that claim to validate
protected route behavior.

## Scope

The ASGI test client opens no sockets, manages no external credentials, and runs application code
in process. `ContainerService` invokes a local Docker or Podman engine, a privileged host interface.
Only use trusted images and test code. It binds published ports to loopback and passes configured
environment values through the child process environment rather than command arguments, but the
engine and container can still access host resources according to the local daemon's policy. Avoid
production data and credentials, do not mount sensitive host paths, and inspect custom daemon
settings before using the helper. Follow the repository [security policy](../../SECURITY.md) for
vulnerability reporting; remove secrets and confidential payloads from public issue reports.
