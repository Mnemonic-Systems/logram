"""@trace must be transparent to the decorated function in live mode."""

from __future__ import annotations

import asyncio
import inspect

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
