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
"""Opt-in local container services for integration tests and development."""

from __future__ import annotations

import asyncio
import math
import os
import re
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from types import MappingProxyType
from uuid import uuid4

_IMAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/:@+-]{0,254}\Z")
_ENVIRONMENT_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_CONTAINER_ID = re.compile(r"[a-fA-F0-9]{12,64}\Z")
_LOOPBACK_PORT = re.compile(r"127\.0\.0\.1:(\d{1,5})\Z")
_MAX_ENVIRONMENT_BYTES = 131_072


class DevServiceError(Exception):
    """Sanitized local container lifecycle failure without command or environment details."""

    def __init__(self, operation: str) -> None:
        """Expose a stable operation label without leaking subprocess output."""
        self.operation = operation
        super().__init__(f"Development service {operation} operation failed.")


@dataclass(frozen=True, slots=True)
class ContainerSpec:
    """Validated image, container port, and environment for one local development service.

    Environment values are passed through the subprocess environment rather than command-line
    arguments, keeping credentials out of process listings and command logs. Use immutable version
    tags or image digests when reproducible test infrastructure matters.
    """

    image: str
    container_port: int
    environment: Mapping[str, str] = field(default_factory=dict, repr=False)
    cpu_limit: float = 1.0
    memory_limit_mb: int = 512
    process_limit: int = 256

    def __post_init__(self) -> None:
        """Reject unsafe image references, ports, and unbounded environment settings."""
        if not isinstance(self.image, str) or _IMAGE.fullmatch(self.image) is None:
            raise ValueError(
                "Container image must be a bounded image reference without whitespace."
            )
        if (
            isinstance(self.container_port, bool)
            or not isinstance(self.container_port, int)
            or not 1 <= self.container_port <= 65_535
        ):
            raise ValueError("Container port must be an integer between 1 and 65535.")
        if (
            isinstance(self.cpu_limit, bool)
            or not isinstance(self.cpu_limit, (int, float))
            or not math.isfinite(self.cpu_limit)
            or not 0 < self.cpu_limit <= 16
        ):
            raise ValueError("Container CPU limit must be finite and between 0 and 16 cores.")
        for name, resource_value, maximum in (
            ("memory_limit_mb", self.memory_limit_mb, 32_768),
            ("process_limit", self.process_limit, 8_192),
        ):
            if (
                isinstance(resource_value, bool)
                or not isinstance(resource_value, int)
                or not 1 <= resource_value <= maximum
            ):
                raise ValueError(f"Container {name} must be an integer from 1 to {maximum}.")
        if not isinstance(self.environment, Mapping):
            raise TypeError("Container environment must be a mapping of names to string values.")
        normalized: dict[str, str] = {}
        size = 0
        for key, environment_value in self.environment.items():
            if not isinstance(key, str) or _ENVIRONMENT_KEY.fullmatch(key) is None:
                raise ValueError("Container environment names must be valid ASCII identifiers.")
            if key in {"PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "CONTAINER_HOST"}:
                raise ValueError("Container environment cannot override local engine settings.")
            if not isinstance(environment_value, str) or "\x00" in environment_value:
                raise ValueError("Container environment values must be strings without NUL bytes.")
            try:
                size += len(key.encode("ascii")) + len(environment_value.encode("utf-8"))
            except UnicodeEncodeError:
                raise ValueError(
                    "Container environment values must be valid Unicode strings."
                ) from None
            if size > _MAX_ENVIRONMENT_BYTES:
                raise ValueError("Container environment exceeds the configured size bound.")
            normalized[key] = environment_value
        object.__setattr__(self, "environment", MappingProxyType(normalized))


class ContainerService:
    """Own one temporary loopback-only Docker or Podman container.

    Startup waits for a TCP connection to the published port within a fixed deadline. The service
    does not install an engine, pull credentials, or configure provider resources; image supply
    chain and local engine security remain the developer's responsibility. Construct a fresh
    instance for each test lifecycle and call :meth:`close` in ``finally`` or an async context.
    """

    def __init__(
        self,
        spec: ContainerSpec,
        *,
        engine: str = "docker",
        startup_timeout: float = 30.0,
        command_timeout: float = 15.0,
        stop_timeout: float = 10.0,
    ) -> None:
        """Configure a bounded service lifecycle without starting a subprocess."""
        if not isinstance(spec, ContainerSpec):
            raise TypeError("ContainerService requires a ContainerSpec.")
        if not isinstance(engine, str) or engine not in {"docker", "podman"}:
            raise ValueError("Container engine must be 'docker' or 'podman'.")
        for name, value, maximum in (
            ("startup_timeout", startup_timeout, 600.0),
            ("command_timeout", command_timeout, 120.0),
            ("stop_timeout", stop_timeout, 120.0),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(
                    f"{name} must be a finite positive number no greater than {maximum:g}."
                )
            if not math.isfinite(value) or not 0 < value <= maximum:
                raise ValueError(
                    f"{name} must be positive and no greater than {maximum:g} seconds."
                )
        self.spec = spec
        self.engine = engine
        self.startup_timeout = float(startup_timeout)
        self.command_timeout = float(command_timeout)
        self.stop_timeout = float(stop_timeout)
        self._label = f"orbit.devservices.id={uuid4().hex}"
        self._container_id: str | None = None
        self._host_port: int | None = None
        self._creation_attempted = False
        self._orphan_cleanup_pending = False
        self._closed = False
        self._lock = asyncio.Lock()

    @property
    def endpoint(self) -> str:
        """Return the ready loopback endpoint or raise when the service is not running."""
        if self._host_port is None:
            raise DevServiceError("endpoint")
        return f"127.0.0.1:{self._host_port}"

    async def __aenter__(self) -> ContainerService:
        """Start the container and return this service for use as an async context manager."""
        await self.start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Stop the container regardless of whether the test body succeeded."""
        await self.close()

    async def start(self) -> str:
        """Start one container and return its ready loopback endpoint."""
        async with self._lock:
            if self._closed:
                raise DevServiceError("start")
            if self._host_port is not None:
                return self.endpoint
            try:
                environment = os.environ.copy()
                environment.update(self.spec.environment)
                environment_arguments = tuple(
                    argument for key in self.spec.environment for argument in ("--env", key)
                )
                self._creation_attempted = True
                container_id = await self._command(
                    "run",
                    "--detach",
                    "--rm",
                    "--cpus",
                    str(self.spec.cpu_limit),
                    "--memory",
                    f"{self.spec.memory_limit_mb}m",
                    "--pids-limit",
                    str(self.spec.process_limit),
                    "--label",
                    self._label,
                    "--publish",
                    f"127.0.0.1::{self.spec.container_port}/tcp",
                    *environment_arguments,
                    self.spec.image,
                    environment=environment,
                    timeout=self.command_timeout,
                )
                if _CONTAINER_ID.fullmatch(container_id) is None:
                    raise DevServiceError("start")
                self._container_id = container_id
                binding = await self._command(
                    "port",
                    container_id,
                    f"{self.spec.container_port}/tcp",
                    timeout=self.command_timeout,
                )
                match = _LOOPBACK_PORT.fullmatch(binding.strip())
                if match is None or not 1 <= int(match.group(1)) <= 65_535:
                    raise DevServiceError("port discovery")
                self._host_port = int(match.group(1))
                await self._wait_ready()
                return self.endpoint
            except asyncio.CancelledError:
                with suppress(Exception):
                    await asyncio.shield(self._cleanup_after_start_failure())
                raise
            except Exception:
                await self._cleanup_after_start_failure()
                raise DevServiceError("start") from None

    async def close(self) -> None:
        """Stop the owned container; retain its ID when cleanup fails so callers can retry."""
        async with self._lock:
            self._closed = True
            container_id = self._container_id
            if container_id is None:
                if self._orphan_cleanup_pending:
                    try:
                        await self._cleanup_by_label()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        raise DevServiceError("stop") from None
                self._host_port = None
                return
            try:
                await self._command(
                    "stop",
                    "--time",
                    str(max(1, int(self.stop_timeout))),
                    container_id,
                    timeout=self.stop_timeout + self.command_timeout,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise DevServiceError("stop") from None
            self._container_id = None
            self._host_port = None

    async def _cleanup_after_start_failure(self) -> None:
        """Remove a container that started but never reached the requested readiness state."""
        if self._container_id is None:
            if self._creation_attempted:
                self._orphan_cleanup_pending = True
                await self._cleanup_by_label()
            return
        try:
            await self._command(
                "stop",
                "--time",
                str(max(1, int(self.stop_timeout))),
                self._container_id,
                timeout=self.stop_timeout + self.command_timeout,
            )
        except Exception:
            # Keep the container ID so a caller can still explicitly retry close().
            self._closed = True
            raise DevServiceError("cleanup") from None
        self._container_id = None
        self._host_port = None

    async def _cleanup_by_label(self) -> None:
        """Find and remove a detached container if startup was cancelled before returning its ID."""
        identifiers = await self._command(
            "ps",
            "--all",
            "--quiet",
            "--filter",
            f"label={self._label}",
            timeout=self.command_timeout,
        )
        if not identifiers:
            self._orphan_cleanup_pending = False
            return
        container_ids = tuple(identifiers.splitlines())
        if any(_CONTAINER_ID.fullmatch(value) is None for value in container_ids):
            raise DevServiceError("cleanup")
        await self._command("rm", "--force", *container_ids, timeout=self.command_timeout)
        self._orphan_cleanup_pending = False

    async def _wait_ready(self) -> None:
        """Probe the published loopback socket until it accepts or the startup deadline expires."""
        if self._host_port is None:
            raise DevServiceError("readiness")
        deadline = time.monotonic() + self.startup_timeout
        while True:
            try:
                _reader, writer = await asyncio.wait_for(
                    asyncio.open_connection("127.0.0.1", self._host_port), timeout=0.25
                )
            except (OSError, TimeoutError):
                if time.monotonic() >= deadline:
                    raise DevServiceError("readiness") from None
                await asyncio.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
            else:
                writer.close()
                with suppress(OSError, TimeoutError):
                    await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
                return

    async def _command(
        self,
        *arguments: str,
        environment: Mapping[str, str] | None = None,
        timeout: float,
    ) -> str:
        """Run one engine command without a shell and return bounded, decoded stdout."""
        process_environment = None if environment is None else dict(environment)
        process: asyncio.subprocess.Process | None = None
        try:
            async with asyncio.timeout(timeout):
                process = await asyncio.create_subprocess_exec(
                    self.engine,
                    *arguments,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=process_environment,
                )
                if process.stdout is None:
                    raise DevServiceError("command")
                output = bytearray()
                while chunk := await process.stdout.read(4097 - len(output)):
                    output.extend(chunk)
                    if len(output) > 4096:
                        raise DevServiceError("command")
                await process.wait()
        except asyncio.CancelledError:
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
            raise
        except (OSError, TimeoutError, DevServiceError):
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
            raise DevServiceError("command") from None
        if process is None or process.returncode != 0:
            raise DevServiceError("command")
        try:
            return bytes(output).decode("ascii").strip()
        except UnicodeDecodeError:
            raise DevServiceError("command") from None


__all__ = ["ContainerService", "ContainerSpec", "DevServiceError"]
