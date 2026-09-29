"""@trace must be transparent to the decorated function in live mode."""

from __future__ import annotations

import asyncio
import inspect
import types

import pytest

import logram


def test_trace_without_parentheses() -> None:
    @logram.trace
    def add(x: int) -> int:
        return x + 1

    assert add(1) == 2
    assert add.__name__ == "add"


def test_trace_with_parentheses_and_name() -> None:
    @logram.trace(name="custom")
    def add(x: int) -> int:
        return x + 1

    assert add(1) == 2


def test_async_function() -> None:
    @logram.trace()
    async def double(x: int) -> int:
        await asyncio.sleep(0)
        return x * 2

    assert inspect.iscoroutinefunction(double)
    assert asyncio.run(double(3)) == 6


def test_sync_generator() -> None:
    @logram.trace
    def count(n: int):
        yield from range(n)

    assert list(count(3)) == [0, 1, 2]


def test_async_generator() -> None:
    @logram.trace()
    async def count(n: int):
        for i in range(n):
            yield i

    async def collect() -> list[int]:
        return [i async for i in count(3)]

    assert asyncio.run(collect()) == [0, 1, 2]


def test_exceptions_propagate_unchanged() -> None:
    class Boom(Exception):
        pass

    @logram.trace()
    def fail() -> None:
        raise Boom("original")

    with pytest.raises(Boom, match="original"):
        fail()


def test_signature_is_preserved() -> None:
    def f(a: int, *, b: str = "x") -> str:
        return f"{a}{b}"

    assert inspect.signature(logram.trace()(f)) == inspect.signature(f)


def test_tracing_failure_never_breaks_the_call(monkeypatch: pytest.MonkeyPatch) -> None:
    import logram.decorators as decorators

    def broken(*args, **kwargs):
        raise RuntimeError("instrumentation bug")

    monkeypatch.setattr(decorators, "_prepare_step_ctx", broken)

    @logram.trace()
    def add(x: int) -> int:
        return x + 1

    assert add(1) == 2


def test_unusable_cache_entry_falls_back_to_a_live_call(monkeypatch: pytest.MonkeyPatch) -> None:
    import logram.decorators as decorators

    # A plain object is fingerprinted by type only, so counting calls does not
    # change the step's fingerprint (a list in the closure would, by value).
    calls = types.SimpleNamespace(n=0)

    @logram.trace()
    def add(x: int) -> int:
        calls.n += 1
        return x + 1

    async def run() -> int:
        logram.init(project="fallback", input_id="doc")
        result = add(1)
        await logram.finalize(status="success")
        return result

    assert asyncio.run(run()) == 2
    monkeypatch.setenv("LOGRAM_REPLAY", "true")
    assert asyncio.run(run()) == 2
    assert calls.n == 1  # served from the cache

    def corrupt(func, cached):
        raise ValueError("cannot rehydrate")

    monkeypatch.setattr(decorators, "_rehydrate_cached", corrupt)
    assert asyncio.run(run()) == 2
    assert calls.n == 2
