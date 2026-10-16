"""Container service tests verify safe arguments and recoverable lifecycle behavior."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from orbit_testing import ContainerService, ContainerSpec, DevServiceError


class _Stream:
    def __init__(self, value: bytes) -> None:
        self.value = value

    async def read(self, limit: int) -> bytes:
        result, self.value = self.value[:limit], self.value[limit:]
        return result


class _Process:
    def __init__(self, output: bytes, returncode: int = 0) -> None:
        self.stdout = _Stream(output)
        self.returncode: int | None = None
        self._result = returncode
        self.killed = False

    async def wait(self) -> int:
        self.returncode = self._result
        return self._result

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


class _Writer:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


def test_spec_validates_bounds_and_hides_environment_from_repr() -> None:
    """Configuration is immutable, bounded, and does not print environment values."""
    env = {"POSTGRES_PASSWORD": "private-value"}
    spec = ContainerSpec("postgres:17", 5432, env)
    env["POSTGRES_PASSWORD"] = "changed"
    assert spec.environment["POSTGRES_PASSWORD"] == "private-value"
    assert "private-value" not in repr(spec)
    with pytest.raises(TypeError):
        spec.environment["POSTGRES_PASSWORD"] = "mutated"  # type: ignore[index]
    with pytest.raises(ValueError, match="image"):
        ContainerSpec("--privileged", 5432)
    with pytest.raises(ValueError, match="engine settings"):
        ContainerSpec("redis:7", 6379, {"DOCKER_HOST": "tcp://remote:2375"})


@pytest.mark.asyncio
async def test_service_publishes_only_loopback_and_stops_on_context_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI receives argument arrays and secrets through env, never shell or args."""
    calls: list[tuple[tuple[str, ...], dict[str, Any]]] = []

    async def create_process(*args: str, **kwargs: Any) -> _Process:
        calls.append((args, kwargs))
        operation = args[1]
        output = {
            "run": b"a" * 64,
            "port": b"127.0.0.1:38421\n",
            "stop": b"a" * 64,
        }[operation]
        return _Process(output)

    async def connect(_host: str, _port: int) -> tuple[object, _Writer]:
        return object(), _Writer()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(asyncio, "open_connection", connect)
    spec = ContainerSpec("postgres:17", 5432, {"POSTGRES_PASSWORD": "private-value"})
    service = ContainerService(spec)

    async with service as running:
        assert running.endpoint == "127.0.0.1:38421"
        run_args, run_options = calls[0]
        assert "127.0.0.1::5432/tcp" in run_args
        assert tuple(run_args[run_args.index("--cpus") :][:2]) == ("--cpus", "1.0")
        assert tuple(run_args[run_args.index("--memory") :][:2]) == ("--memory", "512m")
        assert tuple(run_args[run_args.index("--pids-limit") :][:2]) == ("--pids-limit", "256")
        assert tuple(run_args[run_args.index("--env") :][:2]) == ("--env", "POSTGRES_PASSWORD")
        assert run_options["env"]["POSTGRES_PASSWORD"] == "private-value"
        assert all(
            not isinstance(argument, str) or argument != "private-value" for argument in run_args
        )

    assert calls[-1][0][1] == "stop"
    with pytest.raises(DevServiceError, match="endpoint"):
        _ = service.endpoint
    await service.close()


@pytest.mark.asyncio
async def test_readiness_failure_cleans_up_container_without_leaking_cli_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A service that never opens its port is stopped before startup reports failure."""
    operations: list[str] = []

    async def create_process(*args: str, **_kwargs: Any) -> _Process:
        operation = args[1]
        operations.append(operation)
        output = b"b" * 64 if operation == "run" else b"127.0.0.1:38421"
        return _Process(output)

    async def unavailable(_host: str, _port: int) -> tuple[object, _Writer]:
        raise OSError("private daemon diagnostics")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(asyncio, "open_connection", unavailable)
    service = ContainerService(ContainerSpec("redis:7", 6379), startup_timeout=0.01)
    with pytest.raises(DevServiceError) as failure:
        await service.start()
    assert "private daemon diagnostics" not in str(failure.value)
    assert operations == ["run", "port", "stop"]
    with pytest.raises(DevServiceError, match="endpoint"):
        _ = service.endpoint


@pytest.mark.asyncio
async def test_failed_stop_retains_container_for_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """A transient engine error does not discard the resource handle needed for cleanup retry."""
    stop_attempts = 0

    async def create_process(*args: str, **_kwargs: Any) -> _Process:
        nonlocal stop_attempts
        operation = args[1]
        if operation == "run":
            return _Process(b"c" * 64)
        if operation == "port":
            return _Process(b"127.0.0.1:38421")
        stop_attempts += 1
        return _Process(b"secret engine error", returncode=1 if stop_attempts == 1 else 0)

    async def connect(_host: str, _port: int) -> tuple[object, _Writer]:
        return object(), _Writer()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(asyncio, "open_connection", connect)
    service = ContainerService(ContainerSpec("redis:7", 6379))
    await service.start()
    with pytest.raises(DevServiceError, match="stop"):
        await service.close()
    assert service.endpoint == "127.0.0.1:38421"
    await service.close()
    with pytest.raises(DevServiceError, match="endpoint"):
        _ = service.endpoint
    assert stop_attempts == 2


@pytest.mark.asyncio
async def test_cancelled_container_creation_cleans_up_by_unique_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation during detached startup discovers and removes any orphan by its label."""
    creation_started = asyncio.Event()
    operations: list[str] = []
    ps_attempts = 0

    async def create_process(*args: str, **_kwargs: Any) -> _Process:
        nonlocal ps_attempts
        operation = args[1]
        operations.append(operation)
        if operation == "run":
            creation_started.set()
            await asyncio.Event().wait()
        if operation == "ps":
            ps_attempts += 1
            output = b"private engine failure" if ps_attempts == 1 else b"d" * 64
            return _Process(output, returncode=1 if ps_attempts == 1 else 0)
        if operation == "rm":
            return _Process(b"")
        return _Process(b"")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    service = ContainerService(ContainerSpec("redis:7", 6379))
    task = asyncio.create_task(service.start())
    await creation_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await service.close()
    assert operations == ["run", "ps", "ps", "rm"]
