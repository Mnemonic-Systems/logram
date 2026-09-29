from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest
from typer.testing import CliRunner

import logram
import logram.cli as cli


@pytest.fixture
def cli_paths(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(cli, "DB_PATH", workspace / ".logram" / "logram.db")
    monkeypatch.setattr(cli, "ASSETS_DIR", workspace / ".logram_assets")
    return workspace


def _record_run_with_blobs() -> None:
    @logram.stateful(include=["cache"])
    class Pipeline:
        def __init__(self) -> None:
            self.cache: bytes | None = None

        @logram.trace()
        def run(self, image: bytes) -> bytes:
            self.cache = b"state bytes"
            return b"output bytes"

    async def main() -> None:
        logram.init(project="blobs", input_id="doc")
        Pipeline().run(b"input bytes")
        await logram.finalize(status="success")

    asyncio.run(main())


def _blob_file(assets: Path, payload: bytes) -> Path:
    return assets / f"{hashlib.sha256(payload).hexdigest()}.bin"


def test_clean_only_deletes_unreferenced_blobs(cli_paths: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _record_run_with_blobs()
    assets = cli_paths / ".logram_assets"
    referenced = [_blob_file(assets, p) for p in (b"input bytes", b"output bytes", b"state bytes")]
    assert all(p.exists() for p in referenced)

    orphan = _blob_file(assets, b"nobody points at me")
    orphan.write_bytes(b"nobody points at me")
    unrelated = assets / "notes.txt"
    unrelated.write_text("not a blob")

    # Run from a subdirectory: references must still resolve against the project root.
    subdir = cli_paths / "src"
    subdir.mkdir()
    monkeypatch.chdir(subdir)
    result = CliRunner().invoke(cli.app, ["clean"], input="y\n")

    assert result.exit_code == 0, result.output
    assert all(p.exists() for p in referenced)
    assert not orphan.exists()
    assert unrelated.exists()
