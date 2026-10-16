# Orbit Testing: development

This package declares support for `>=3.11,<3.15` in `pyproject.toml`. That declaration is distinct from evidence that hosted CI ran.

Use the checks for each language manifest present in this repository:

### Python package

```bash
python -m pip install -e '.[dev]' build
pytest
ruff check src tests
ruff format --check src tests
mypy
```

Optional SDKs and live-service tests are described by the package README. Do not run an opt-in live test against a shared or production service unless its own documentation explicitly supports that use.

Container-backed integration tests use `ContainerService` and require a local Docker or Podman
engine. Keep these tests opt-in when the engine is not part of the standard CI runner. Pin images,
publish ports on loopback, use synthetic credentials, and always use the async context manager so
the service is stopped after a failure. Fake the subprocess and socket boundaries for deterministic
unit coverage; a fake does not count as live-service validation.

To exercise TLS-gated application behavior, construct `TestClient(app, scheme="https")`. This sets
the ASGI scope scheme and default server port for the in-process request; it does not establish a TLS
connection, validate certificates, or test proxy and ingress configuration. Keep the default
`http` scheme for tests that should verify secure handlers reject non-TLS requests.

The HTTPS-scheme option and request-target contract passed the package suite on CPython
**3.11.14, 3.12.14, 3.13.15, and 3.14.7**: 43 tests per interpreter, Ruff lint/format, strict mypy,
and wheel/sdist builds. These checks exercise the declared Python support range; they do not validate
real TLS transport, certificates, proxy headers, or ingress configuration.

## Contribution expectations

Add focused regression coverage for behavior changes. Update the README and relevant guide when a contract, configuration option, security property, lifecycle rule, or operational assumption changes. Report the exact versions and services exercised; local checks do not imply hosted CI or release validation.
