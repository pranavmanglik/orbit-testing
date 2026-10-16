# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Test the public harness against lifespan, framing, and bounded input contracts."""

import asyncio

import pytest
from orbit import Application, ApplicationConfig
from orbit.asgi import (
    MAX_HEADER_BYTES,
    MAX_HEADER_COUNT,
    MAX_PATH_BYTES,
    MAX_QUERY_BYTES,
    ASGIApplication,
    Headers,
    Response,
)

from orbit_testing import TestClient
from orbit_testing import TestResponse as CapturedResponse


class FalseyHeaderSequence(list[tuple[str, str]]):
    """A populated header sequence whose truth value is intentionally false."""

    def __bool__(self) -> bool:
        return False


def test_test_response_validates_direct_construction() -> None:
    """The public testing response cannot represent malformed ASGI output."""
    response = CapturedResponse(200, Headers({"content-type": "text/plain"}), b"ok")
    assert response.body == b"ok"
    with pytest.raises(ValueError):
        CapturedResponse(100, Headers(), b"")
    with pytest.raises(TypeError):
        CapturedResponse(200, {}, b"")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        CapturedResponse(200, Headers(), "ok")  # type: ignore[arg-type]


async def test_client_requires_context_and_rejects_reuse():
    client = TestClient(ASGIApplication(Application(ApplicationConfig(name="client"))))
    with pytest.raises(RuntimeError, match="context"):
        await client.request("GET", "/")
    async with client:
        pass
    with pytest.raises(RuntimeError, match="single-use"):
        await client.__aenter__()


async def test_lifespan_crash_is_observed_immediately():
    class Crashed:
        async def __call__(self, scope, receive, send):
            raise ValueError("lifespan crash")

    async with asyncio.timeout(1):
        with pytest.raises(ValueError, match="lifespan crash"):
            async with TestClient(Crashed()):
                pytest.fail("startup must fail")


async def test_lifespan_failure_wins_over_same_turn_startup_ack():
    class AcknowledgedThenCrashed:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                await receive()
                await send({"type": "lifespan.startup.complete"})
                raise ValueError("post-ack failure")

    async with asyncio.timeout(1):
        with pytest.raises(ValueError, match="post-ack failure"):
            async with TestClient(AcknowledgedThenCrashed()):
                pytest.fail("startup failure must not be hidden by an acknowledgement")


async def test_lifespan_failure_acknowledgement_is_reported() -> None:
    """A structured startup failure must surface its operator-facing message."""

    class Failed:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                await receive()
                await send({"type": "lifespan.startup.failed", "message": "configuration failed"})

    with pytest.raises(RuntimeError, match="configuration failed"):
        async with TestClient(Failed()):
            pytest.fail("startup failure must abort the context")


async def test_lifespan_ending_without_acknowledgement_is_reported() -> None:
    """A host that exits without an acknowledgement cannot silently pass startup."""

    class Silent:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                await receive()

    with pytest.raises(RuntimeError, match="without a protocol acknowledgement"):
        async with TestClient(Silent()):
            pytest.fail("silent lifespan termination must fail startup")


async def test_shutdown_failure_acknowledgement_is_reported() -> None:
    """A structured shutdown failure must not be mistaken for successful cleanup."""

    class Failed:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                message = await receive()
                await send({"type": f"lifespan.{message['type'].split('.', 1)[1]}.complete"})
                await receive()
                await send({"type": "lifespan.shutdown.failed", "message": "cleanup failed"})

    with pytest.raises(RuntimeError, match="cleanup failed"):
        async with TestClient(Failed()):
            pass


async def test_shutdown_timeout_cancels_the_lifespan_task() -> None:
    """A stuck host is cancelled after the configured bounded shutdown window."""

    class Stuck:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                await receive()
                await send({"type": "lifespan.startup.complete"})
                await receive()
                await asyncio.Event().wait()

    with pytest.raises(TimeoutError):
        async with TestClient(Stuck(), lifespan_timeout=0.01):
            pass


async def test_client_models_disconnect_after_the_complete_request() -> None:
    """The harness exposes a client disconnect after the one buffered request frame."""

    class ReadsTwice:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                first = await receive()
                await send({"type": f"lifespan.{first['type'].split('.', 1)[1]}.complete"})
                second = await receive()
                await send({"type": f"lifespan.{second['type'].split('.', 1)[1]}.complete"})
            else:
                first = await receive()
                second = await receive()
                assert first["type"] == "http.request"
                assert second["type"] == "http.disconnect"
                await send({"type": "http.response.start", "status": 200, "headers": []})
                await send({"type": "http.response.body", "body": b"ok", "more_body": False})

    async with TestClient(ReadsTwice()) as client:
        assert (await client.request("GET", "/")).body == b"ok"


async def test_client_rejects_an_application_that_sends_no_response() -> None:
    """An application that returns without response frames must fail the test."""

    class NoResponse:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                first = await receive()
                await send({"type": f"lifespan.{first['type'].split('.', 1)[1]}.complete"})
                second = await receive()
                await send({"type": f"lifespan.{second['type'].split('.', 1)[1]}.complete"})

    async with TestClient(NoResponse()) as client:
        with pytest.raises(AssertionError, match="did not start a response"):
            await client.request("GET", "/")


@pytest.mark.parametrize(
    "frames",
    [
        [{"type": "http.response.body", "body": b""}],
        [{"type": "http.response.start", "status": 200, "headers": []}],
        [
            {"type": "http.response.start", "status": 200, "headers": []},
            {"type": "http.response.start", "status": 200, "headers": []},
        ],
        [
            {"type": "http.response.start", "status": 200, "headers": []},
            {"type": "http.response.body", "body": b""},
            {"type": "http.response.body", "body": b""},
        ],
    ],
)
async def test_invalid_response_framing_fails_the_test(frames):
    class Broken:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                await receive()
                await send({"type": "lifespan.startup.complete"})
                await receive()
                await send({"type": "lifespan.shutdown.complete"})
            else:
                for frame in frames:
                    await send(frame)

    async with TestClient(Broken()) as client:
        with pytest.raises(AssertionError):
            await client.request("GET", "/")


async def test_encoded_path_and_repeated_headers_reach_handler():
    app = Application(ApplicationConfig(name="wire"))

    @app.router.route("/café", method="GET", name="cafe")
    async def route(request):
        return Response.json({"values": request.headers.getall("x-value")})

    async with TestClient(ASGIApplication(app)) as client:
        response = await client.request(
            "get", "/caf%C3%A9", headers=[("X-Value", "one"), ("x-value", "two")]
        )
    assert response.status == 200
    assert response.json() == {"values": ["one", "two"]}


@pytest.mark.parametrize("timeout", [0, True, "1", float("nan"), float("inf"), 10**1000])
def test_invalid_lifespan_timeout(timeout):
    with pytest.raises(ValueError):
        TestClient(
            ASGIApplication(Application(ApplicationConfig(name="timeout"))),
            lifespan_timeout=timeout,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"method": 1},
        {"path": 1},
        {"body": "text"},
        {"headers": {"x-test": 1}},
    ],
)
async def test_client_request_rejects_non_typed_inputs(kwargs):
    app = Application(ApplicationConfig(name="client-inputs"))
    async with TestClient(ASGIApplication(app)) as client:
        with pytest.raises(TypeError):
            await client.request(**{"method": "GET", "path": "/", **kwargs})


async def test_client_rejects_non_latin1_headers_explicitly():
    app = Application(ApplicationConfig(name="client-header-encoding"))
    async with TestClient(ASGIApplication(app)) as client:
        with pytest.raises(ValueError, match="Latin-1"):
            await client.request("GET", "/", headers={"x-test": "€"})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path", ["https://example.test/resource", "/resource#fragment", "//other/resource"]
)
async def test_client_rejects_absolute_or_fragmented_request_targets(path: str) -> None:
    app = Application(ApplicationConfig(name="invalid-request-target"))
    async with TestClient(ASGIApplication(app)) as client:
        with pytest.raises(ValueError, match="origin-relative"):
            await client.request("GET", path)


@pytest.mark.parametrize("scheme", ["", "ftp", "HTTPS", "https://example.test", None, True])
def test_client_rejects_invalid_schemes(scheme: object) -> None:
    app = Application(ApplicationConfig(name="invalid-scheme"))
    with pytest.raises(ValueError, match="scheme must be either"):
        TestClient(ASGIApplication(app), scheme=scheme)  # type: ignore[arg-type]


async def test_client_sets_https_scheme_and_default_server_port() -> None:
    observed: list[tuple[str, tuple[str, int]]] = []

    class ScopeCapture:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                startup = await receive()
                await send({"type": f"lifespan.{startup['type'].split('.', 1)[1]}.complete"})
                shutdown = await receive()
                await send({"type": f"lifespan.{shutdown['type'].split('.', 1)[1]}.complete"})
                return
            observed.append((scope["scheme"], scope["server"]))
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok", "more_body": False})

    async with TestClient(ScopeCapture(), scheme="https") as client:
        response = await client.request("GET", "/")

    assert response.status == 200
    assert observed == [("https", ("testserver", 443))]


async def test_client_rejects_invalid_http_method_token() -> None:
    app = Application(ApplicationConfig(name="invalid-method"))
    async with TestClient(ASGIApplication(app)) as client:
        with pytest.raises(ValueError, match="HTTP token"):
            await client.request("GET\\r\\nInjected", "/")


async def test_client_decodes_response_header_values_as_latin1() -> None:
    class Latin1Response:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                startup = await receive()
                await send({"type": f"lifespan.{startup['type'].split('.', 1)[1]}.complete"})
                shutdown = await receive()
                await send({"type": f"lifespan.{shutdown['type'].split('.', 1)[1]}.complete"})
                return
            await send(
                {"type": "http.response.start", "status": 200, "headers": [(b"x-value", b"\xe9")]}
            )
            await send({"type": "http.response.body", "body": b"ok", "more_body": False})

    async with TestClient(Latin1Response()) as client:
        response = await client.request("GET", "/")

    assert response.headers["x-value"] == "é"


async def test_client_bounds_headers_before_building_the_asgi_scope() -> None:
    """The harness applies Core header limits before copying caller-owned input."""
    app = Application(ApplicationConfig(name="client-header-limits"))
    async with TestClient(ASGIApplication(app)) as client:
        with pytest.raises(ValueError, match="safety limit"):
            await client.request(
                "GET", "/", headers=[(f"x-{index}", "v") for index in range(MAX_HEADER_COUNT + 1)]
            )
        with pytest.raises(ValueError, match="safety limit"):
            await client.request("GET", "/", headers={"x-large": "x" * MAX_HEADER_BYTES})


async def test_client_bounds_url_components_before_building_the_asgi_scope() -> None:
    """The harness applies Core path and query byte limits before URL normalization."""
    app = Application(ApplicationConfig(name="client-url-limits"))
    async with TestClient(ASGIApplication(app)) as client:
        with pytest.raises(ValueError, match="path"):
            await client.request("GET", "/" + "a" * MAX_PATH_BYTES)
        with pytest.raises(ValueError, match="query string"):
            await client.request("GET", "/?q=" + "a" * MAX_QUERY_BYTES)


async def test_client_bounds_captured_response_bytes() -> None:
    """Large buffered responses stop at the configured test-harness memory limit."""
    app = Application(ApplicationConfig(name="client-response-byte-limit"))

    @app.router.route("/large", name="large")
    async def large(request):
        return Response(body=b"large")

    async with TestClient(ASGIApplication(app), max_response_bytes=4) as client:
        with pytest.raises(ValueError, match="body limit"):
            await client.request("GET", "/large")


async def test_client_bounds_captured_response_frames() -> None:
    """Many empty stream frames cannot grow the in-memory response collector unboundedly."""
    app = Application(ApplicationConfig(name="client-response-frame-limit"))

    @app.router.route("/stream", name="stream")
    async def stream(request):
        async def chunks():
            for _ in range(5):
                yield b""

        return Response(stream=chunks())

    async with TestClient(ASGIApplication(app), max_response_frames=2) as client:
        with pytest.raises(ValueError, match="frame limit"):
            await client.request("GET", "/stream")


async def test_client_preserves_populated_falsey_header_sequences() -> None:
    """Only ``None`` means that optional headers were omitted."""
    app = Application(ApplicationConfig(name="falsey-headers"))

    @app.router.route("/headers", name="headers")
    async def headers(request):
        return Response.json({"value": request.headers.get("x-test")})

    async with TestClient(ASGIApplication(app)) as client:
        response = await client.request(
            "GET", "/headers", headers=FalseyHeaderSequence([("x-test", "present")])
        )
    assert response.json() == {"value": "present"}
