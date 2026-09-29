"""Argument part of the cache key: distinct inputs must never share a key."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from pydantic import BaseModel

from logram.decorators import _logical_args_for_vcr


def step(payload: Any, request_id: str = "") -> Any:
    return payload


def key(payload: Any, *, ignore: list[str] | None = None, **kwargs: Any) -> str:
    args, extra = _logical_args_for_vcr(step, (payload,), kwargs, ignore_in_hash=ignore)
    return repr((args, extra))


class Document(BaseModel):
    name: str
    text: str


@dataclass
class Page:
    number: int
    words: list[str]


class Tile:
    """Plain object without any identity protocol (repr is address-based)."""

    def __init__(self, tile_id: int, pixels: bytes) -> None:
        self.tile_id = tile_id
        self.pixels = pixels


class KeyedTile(Tile):
    def __logram_trace_key__(self) -> dict[str, Any]:
        return {"tile_id": self.tile_id}


class LoggedTile(Tile):
    def __logram_trace_log__(self) -> dict[str, Any]:
        return {"tile_id": self.tile_id}


class LongRepr:
    def __init__(self, value: str) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"LongRepr({'x' * 300}{self.value})"


@pytest.mark.parametrize(
    ("a", "b"),
    [
        pytest.param(list(range(20)), [*range(19), -1], id="list-item-beyond-16"),
        pytest.param(tuple(range(20)), (*range(19), -1), id="tuple-item-beyond-16"),
        pytest.param(
            {f"k{i:02d}": i for i in range(30)},
            {**{f"k{i:02d}": i for i in range(30)}, "k29": -1},
            id="dict-key-beyond-24",
        ),
        pytest.param([[[[[[1]]]]]], [[[[[[2]]]]]], id="nested-beyond-depth-4"),
        pytest.param("a" * 300, "a" * 299 + "b", id="long-string"),
        pytest.param(b"\x00" * 300, b"\x00" * 299 + b"\x01", id="bytes"),
        pytest.param(
            Document(name="invoice", text="total: 10"),
            Document(name="invoice", text="total: 99"),
            id="pydantic-same-name-different-content",
        ),
        pytest.param(Page(1, ["a"] * 20), Page(1, ["a"] * 19 + ["b"]), id="dataclass-long-field"),
        pytest.param(LongRepr("1"), LongRepr("2"), id="repr-differs-beyond-220-chars"),
        pytest.param(
            LoggedTile(1, b"old"),
            LoggedTile(1, b"new"),
            id="trace-log-hook-does-not-define-identity",
        ),
    ],
)
def test_distinct_arguments_produce_distinct_keys(a: Any, b: Any) -> None:
    assert key(a) != key(b)


def test_equal_arguments_produce_equal_keys() -> None:
    assert key({"b": [1, 2], "a": "x"}) == key({"b": [1, 2], "a": "x"})
    assert key(Document(name="n", text="t")) == key(Document(name="n", text="t"))
    assert key(Page(1, ["a"])) == key(Page(1, ["a"]))


def test_trace_key_hook_defines_identity() -> None:
    assert key(KeyedTile(1, b"old")) == key(KeyedTile(1, b"new"))
    assert key(KeyedTile(1, b"x")) != key(KeyedTile(2, b"x"))


def test_ignore_in_hash_excludes_argument() -> None:
    assert key("p", ignore=["request_id"], request_id="r1") == key(
        "p", ignore=["request_id"], request_id="r2"
    )
    assert key("p", request_id="r1") != key("p", request_id="r2")


def test_self_referencing_argument_does_not_crash() -> None:
    loop: list[Any] = [1]
    loop.append(loop)
    assert key(loop)
