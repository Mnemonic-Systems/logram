from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI


def create_app(*, db_path: Path | None = None, assets_dir: Path | None = None) -> FastAPI:
    from .app import create_app as _create_app

    return _create_app(db_path=db_path, assets_dir=assets_dir)


__all__ = ["create_app"]
