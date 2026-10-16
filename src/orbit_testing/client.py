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
"""In-process ASGI harness that owns lifespan and captures bounded HTTP messages."""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import unquote, urlsplit

from orbit.asgi import (
    MAX_HEADER_BYTES,
    MAX_HEADER_COUNT,
    MAX_PATH_BYTES,
    MAX_QUERY_BYTES,
    ASGIApplication,
    Headers,
    Message,
    Response,
)

_MAX_TEST_RESPONSE_BYTES = 1024 * 1024 * 1024
_MAX_TEST_RESPONSE_FRAMES = 1_000_000
_METHOD_TOKEN = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]{1,32}\Z")


@dataclass(frozen=True)
class TestResponse:
    """Captured HTTP status, repeated headers and body bytes."""

    status: int
    headers: Headers
    body: bytes

    def __post_init__(self) -> None:
        """Validate captured response data even when a test constructs it directly."""
        if (
            isinstance(self.status, bool)
            or not isinstance(self.status, int)
            or not 200 <= self.status <= 599
        ):
            raise ValueError("Test response status must be final (200..599).")
        if not isinstance(self.headers, Headers):
            raise TypeError("Test response headers must be validated Headers.")
        if not isinstance(self.body, bytes):
            raise TypeError("Test response body must be bytes.")

    def json(self) -> Any:
        """Decode response JSON for assertions."""
        return json.loads(self.body)


class TestClient:
    """Exercise an Orbit ASGI callable without sockets or a separate web framework.

    Use ``async with`` to send real lifespan messages. Application startup/shutdown failures fail
    the context manager instead of being silently ignored. Requests are complete buffered messages;
    use host-level tests for network transport, concurrency, or streaming behavior.
    """

    __test__ = False

    def __init__(
        self,
        application: ASGIApplication,
        *,
        scheme: Literal["http", "https"] = "http",
        lifespan_timeout: float = 60,
        max_response_bytes: int = Response.MAX_BODY_BYTES,
        max_response_frames: int = 65_536,
    ) -> None:
        """Configure the simulated ASGI scheme and bounded test-harness resources.

        ``scheme="https"`` lets tests exercise TLS-gated application behavior. It only sets the
        ASGI scope and does not create a TLS connection or verify transport configuration.
        """
        if scheme not in ("http", "https"):
            raise ValueError("TestClient scheme must be either 'http' or 'https'.")
        if not _is_finite_number(lifespan_timeout) or lifespan_timeout <= 0:
            raise ValueError("lifespan_timeout must be finite and positive.")
        if (
            isinstance(max_response_bytes, bool)
            or not isinstance(max_response_bytes, int)
            or not 0 <= max_response_bytes <= _MAX_TEST_RESPONSE_BYTES
        ):
            raise ValueError("max_response_bytes must be between zero and 1 GiB.")
        if (
            isinstance(max_response_frames, bool)
            or not isinstance(max_response_frames, int)
            or not 1 <= max_response_frames <= _MAX_TEST_RESPONSE_FRAMES
        ):
            raise ValueError("max_response_frames must be between 1 and 1,000,000.")
        self.application = application
        self._scheme = scheme
        self._timeout = lifespan_timeout
        self._max_response_bytes = max_response_bytes
        self._max_response_frames = max_response_frames
        self._active = False
        self._incoming: asyncio.Queue[Message] = asyncio.Queue()
        self._outgoing: asyncio.Queue[Message] = asyncio.Queue()
        self._lifespan: asyncio.Task[None] | None = None

    async def __aenter__(self) -> TestClient:
        if self._lifespan is not None:
            raise RuntimeError("TestClient is single-use; construct a new client and application.")
        self._lifespan = asyncio.create_task(
            self.application({"type": "lifespan"}, self._incoming.get, self._outgoing.put)
        )
        await self._incoming.put({"type": "lifespan.startup"})
        try:
            response = await self._wait_lifespan()
            if response["type"] != "lifespan.startup.complete":
                raise RuntimeError(response.get("message", "Startup failed."))
        except BaseException:
            self._lifespan.cancel()
            await asyncio.gather(self._lifespan, return_exceptions=True)
            raise
        self._active = True
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._lifespan is None:
            raise RuntimeError("TestClient lifespan was not started.")
        self._active = False
        await self._incoming.put({"type": "lifespan.shutdown"})
        try:
            async with asyncio.timeout(self._timeout):
                response = await self._wait_lifespan()
                await self._lifespan
            if response["type"] != "lifespan.shutdown.complete":
                raise RuntimeError(response.get("message", "Shutdown failed."))
        finally:
            if not self._lifespan.done():
                self._lifespan.cancel()
                await asyncio.gather(self._lifespan, return_exceptions=True)

    async def _wait_lifespan(self) -> Message:
        if self._lifespan is None:
            raise RuntimeError("TestClient lifespan was not started.")
        message = asyncio.create_task(self._outgoing.get())
        try:
            async with asyncio.timeout(self._timeout):
                await asyncio.wait((message, self._lifespan), return_when=asyncio.FIRST_COMPLETED)
                if self._lifespan.done() and not self._lifespan.cancelled():
                    error = self._lifespan.exception()
                    if error is not None:
                        raise error
                if message.done():
                    return message.result()
                if not self._outgoing.empty():
                    return self._outgoing.get_nowait()
                await self._lifespan
                raise RuntimeError("Lifespan ended without a protocol acknowledgement.")
        finally:
            if not message.done():
                message.cancel()
            await asyncio.gather(message, return_exceptions=True)

    async def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes = b"",
        headers: Mapping[str, str] | Sequence[tuple[str, str]] | None = None,
    ) -> TestResponse:
        """Send one complete buffered HTTP request and verify response framing."""
        if not self._active:
            raise RuntimeError("Send requests inside the TestClient context manager.")
        if not isinstance(method, str):
            raise TypeError("Request method must be a string.")
        if _METHOD_TOKEN.fullmatch(method) is None:
            raise ValueError("Request method must be a valid HTTP token of at most 32 characters.")
        if not isinstance(path, str):
            raise TypeError("Request path must be a string.")
        if not isinstance(body, bytes):
            raise TypeError("Request body must be bytes.")
        url = urlsplit(path)
        if url.scheme or url.netloc or url.fragment or not path.startswith("/"):
            raise ValueError("Request path must be an origin-relative target without a fragment.")
        raw_path = url.path or "/"
        if raw_path.startswith("//"):
            raise ValueError("Request path must not begin with multiple slashes.")
        try:
            raw_path_bytes = raw_path.encode("utf-8")
            query_bytes = url.query.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("Request URL must contain valid Unicode text.") from exc
        if len(raw_path_bytes) > MAX_PATH_BYTES:
            raise ValueError("Request path exceeds the maximum supported length.")
        if len(query_bytes) > MAX_QUERY_BYTES:
            raise ValueError("Request query string exceeds the maximum supported length.")
        pairs = (
            headers.items()
            if isinstance(headers, Mapping)
            else (headers if headers is not None else ())
        )
        wire_headers: list[tuple[bytes, bytes]] = []
        header_bytes = 0
        for name, value in pairs:
            if not isinstance(name, str) or not isinstance(value, str):
                raise TypeError("Request header names and values must be strings.")
            try:
                encoded_name = name.lower().encode("ascii")
                encoded_value = value.encode("latin-1")
            except UnicodeEncodeError as exc:
                raise ValueError("Request headers must be Latin-1 encodable.") from exc
            header_bytes += len(encoded_name) + len(encoded_value) + 2
            if len(wire_headers) >= MAX_HEADER_COUNT or header_bytes > MAX_HEADER_BYTES:
                raise ValueError("Request headers exceed the safety limit.")
            wire_headers.append((encoded_name, encoded_value))
        response_start: Message | None = None
        response_body = bytearray()
        response_frames = 0
        response_complete = False
        received = False

        async def receive() -> Message:
            """Provide one complete request frame, then model client disconnect."""
            nonlocal received
            if received:
                return {"type": "http.disconnect"}
            received = True
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message: Message) -> None:
            """Validate framing while collecting a bounded response body."""
            nonlocal response_start, response_frames, response_complete
            if response_start is None:
                if message.get("type") != "http.response.start":
                    raise AssertionError("Response body preceded response start.")
                response_start = message
                return
            if response_complete or message.get("type") != "http.response.body":
                raise AssertionError("Unexpected response frame or duplicate response start.")
            chunk = message.get("body", b"")
            if not isinstance(chunk, bytes):
                raise AssertionError("Response body chunks must be bytes.")
            response_frames += 1
            if response_frames > self._max_response_frames:
                raise ValueError("ASGI response exceeds the configured frame limit.")
            if len(response_body) + len(chunk) > self._max_response_bytes:
                raise ValueError("ASGI response exceeds the configured body limit.")
            response_body.extend(chunk)
            more_body = message.get("more_body", False)
            if not isinstance(more_body, bool):
                raise AssertionError("Response more_body flag must be boolean.")
            response_complete = not more_body

        await self.application(
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "method": method.upper(),
                "path": unquote(raw_path, encoding="utf-8", errors="strict"),
                "raw_path": raw_path_bytes,
                "scheme": self._scheme,
                "http_version": "1.1",
                "root_path": "",
                "client": ("127.0.0.1", 54321),
                "server": ("testserver", 443 if self._scheme == "https" else 80),
                "query_string": query_bytes,
                "headers": wire_headers,
            },
            receive,
            send,
        )
        if response_start is None:
            raise AssertionError("ASGI application did not start a response.")
        if not response_complete:
            raise AssertionError("ASGI response stream did not finish.")
        return TestResponse(
            response_start["status"],
            Headers(
                [
                    (key.decode("ascii"), value.decode("latin-1"))
                    for key, value in response_start["headers"]
                ]
            ),
            bytes(response_body),
        )


def _is_finite_number(value: object) -> bool:
    """Validate finite numeric timeout input without depending on Core private utilities."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


__all__ = ["TestClient", "TestResponse"]
