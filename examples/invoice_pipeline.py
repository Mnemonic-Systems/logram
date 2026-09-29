"""A multi-step "LLM" pipeline that runs without any API key.

``FakeLLM`` sleeps like a real model call and returns deterministic text plus
token usage, so live runs, replays and ``logram stats`` behave as they would
with a real provider.

    python examples/invoice_pipeline.py                         # live: ~4 s
    LOGRAM_REPLAY=true python examples/invoice_pipeline.py      # replay: well under a second

Then edit SUMMARY_PROMPT below and replay again: only ``summarize`` runs live.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import time
from dataclasses import dataclass

from pydantic import BaseModel

import logram

EXTRACTION_PROMPT = "List every line item on this invoice page with its quantity and unit price."
SUMMARY_PROMPT = "Summarize the invoice in one sentence for an accountant."

PAGES = [f"Invoice INV-{page:03d}, page {page}: widgets, bolts, shipping." for page in range(1, 7)]
LATENCY_SECONDS = float(os.environ.get("FAKE_LLM_LATENCY", "0.6"))


@dataclass
class Usage:
    prompt_tokens: int
    completion_tokens: int


@dataclass
class Completion:
    text: str
    usage: Usage


class FakeLLM:
    """Deterministic stand-in for a chat completion API."""

    def complete(self, prompt: str) -> Completion:
        time.sleep(LATENCY_SECONDS)
        digest = hashlib.sha256(prompt.encode()).hexdigest()[:8]
        return Completion(
            text=f"answer-{digest}",
            usage=Usage(prompt_tokens=len(prompt.split()), completion_tokens=12),
        )


class LineItems(BaseModel):
    page: int
    items: list[str]
    tokens: int


llm = FakeLLM()


@logram.trace()
def ask(prompt: str) -> Completion:
    # Returning the provider's response object lets Logram read its token usage.
    return llm.complete(prompt)


@logram.trace()
def extract_page(page_number: int, text: str) -> LineItems:
    completion = ask(f"{EXTRACTION_PROMPT}\n\n{text}")
    items = [f"{completion.text}-{i}" for i in range(3)]
    return LineItems(page=page_number, items=items, tokens=completion.usage.prompt_tokens)


@logram.trace()
def merge(pages: list[LineItems]) -> list[str]:
    return [item for page in sorted(pages, key=lambda p: p.page) for item in page.items]


@logram.trace()
def summarize(items: list[str]) -> str:
    return ask(f"{SUMMARY_PROMPT}\n\n" + "\n".join(items)).text


async def main() -> None:
    logram.init(project="invoice_demo", input_id=os.environ.get("LOGRAM_INPUT_ID", "INV-001"))
    started = time.perf_counter()
    try:
        pages = [extract_page(n, text) for n, text in enumerate(PAGES, start=1)]
        summary = summarize(merge(pages))
        await logram.finalize(status="success")
    except Exception:
        await logram.finalize(status="failed")
        raise
    print(f"summary: {summary}")
    print(f"elapsed: {time.perf_counter() - started:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
