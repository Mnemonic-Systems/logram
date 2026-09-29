"""Deterministic stand-ins for the steps of an LLM pipeline.

Each function has the shape of real pipeline code (prompt templates, few-shot
examples, scoring weights, parsing rules, helpers, methods, closures) but runs
without any model, so the mutation benchmark can observe its exact output.
"""

import functools
import re
import types

SYSTEM_PROMPT = "You extract line items from invoices. Answer in JSON."
TEMPERATURE = 0.2
MAX_ITEMS = 12

FEW_SHOT = [f"Example {i}: widget x{i} at {i * 1.5:.2f}" for i in range(20)]

FIELD_WEIGHTS = {f"field_{i:02d}": round(0.1 + i * 0.03, 2) for i in range(30)}

UNITS = {"kg": 1000.0, "g": 1.0, "mg": 0.001, "lb": 453.6}

ROUTING = {"invoice": {"model": "large", "budget": {"tokens": 4096, "retries": {"max": 3, "backoff": [1, 2, 4]}}}}

PRICE_PATTERN = re.compile(r"(\d+)\s*x\s*([\d.]+)")


def build_prompt(document: str, n_examples: int = 3) -> str:
    """Prompt with a few-shot prefix."""
    shots = "\n".join(FEW_SHOT[-n_examples:])
    return f"{SYSTEM_PROMPT}\n{shots}\n\nDocument:\n{document}\nTemperature: {TEMPERATURE}"


def parse_line_items(response: str) -> list[tuple[int, float]]:
    items = [(int(q), float(p)) for q, p in PRICE_PATTERN.findall(response)]
    return items[:MAX_ITEMS]


def _to_grams(value: float, unit: str) -> float:
    return value * UNITS[unit]


def normalize_weights(rows: list[tuple[float, str]]) -> list[float]:
    return [round(_to_grams(v, u), 3) for v, u in rows]


def score_fields(present: list[str]) -> float:
    total = sum(FIELD_WEIGHTS[name] for name in present if name in FIELD_WEIGHTS)
    return round(total / (1 + len(present)), 4)


def rank_candidates(candidates: list[tuple[str, float]]) -> list[str]:
    ranked = sorted(candidates, key=lambda c: c[1] * TEMPERATURE - len(c[0]) * 0.01, reverse=True)
    return [name for name, _ in ranked]


def retry_schedule(kind: str) -> list[int]:
    policy = ROUTING[kind]["budget"]["retries"]
    return [delay * 2 for delay in policy["backoff"][: policy["max"]]]


def chunk_text(text: str, size: int = 40, overlap: int = 5) -> list[str]:
    step = max(1, size - overlap)
    return [text[i : i + size] for i in range(0, len(text), step)]


def make_threshold_filter(threshold: float):
    def keep(values: list[float]) -> list[float]:
        return [v for v in values if v >= threshold]

    return keep


filter_confident = make_threshold_filter(0.75)


def select_confident(values: list[float]) -> list[float]:
    return filter_confident(values)


class Extractor:
    def __init__(self, currency: str = "EUR") -> None:
        self.currency = currency

    def _format(self, amount: float) -> str:
        return f"{amount:.2f} {self.currency}"

    def total(self, items: list[tuple[int, float]]) -> str:
        subtotal = sum(q * p for q, p in items)
        if subtotal > 100:
            subtotal *= 0.95
        return self._format(subtotal)


def extract_total(items: list[tuple[int, float]]) -> str:
    return Extractor().total(items)


def _weighted(values: list[float], weight: float, bias: float = 0.0) -> float:
    return round(sum(v * weight for v in values) + bias, 4)


weighted_score = functools.partial(_weighted, weight=0.3, bias=0.05)


def aggregate_scores(values: list[float]) -> float:
    return weighted_score(values)


@functools.lru_cache(maxsize=None)
def _stopwords() -> frozenset[str]:
    return frozenset({"the", "a", "of", "and"})


def keywords(text: str) -> list[str]:
    return [w for w in text.lower().split() if w not in _stopwords() and len(w) > 2]


GENERATION = types.SimpleNamespace(top_p=0.9, max_tokens=256, stop="###")


def generation_params(prompt: str) -> dict[str, object]:
    return {"prompt": prompt[: GENERATION.max_tokens], "top_p": GENERATION.top_p, "stop": GENERATION.stop}


class Tokenizer:
    SEPARATOR = " | "

    def __init__(self, lowercase: bool = True) -> None:
        self.lowercase = lowercase
        self.vocab = {"invoice": 1, "total": 2}

    @staticmethod
    def _clean(token: str) -> str:
        return token.strip(".,;:")

    @classmethod
    def join(cls, tokens: list[str]) -> str:
        return cls.SEPARATOR.join(tokens)

    def encode(self, text: str) -> str:
        tokens = [self._clean(t) for t in text.split()]
        if self.lowercase:
            tokens = [t.lower() for t in tokens]
        return self.join([str(self.vocab.get(t, 0)) for t in tokens])


def encode_document(text: str) -> str:
    return Tokenizer().encode(text)


# Steps and the inputs each one is run on. Several inputs per step, so that a
# mutation that changes behaviour is unlikely to go unobserved.
STEPS = {
    "build_prompt": [("Invoice 42: 3 x 9.99",), ("",), ("Invoice 7", 5)],
    "parse_line_items": [("3 x 9.99, 2 x 120.0",), ("1 x 1.5 " * 20,), ("no items",)],
    "normalize_weights": [([(2.0, "kg"), (500.0, "g")],), ([(1.0, "lb"), (3.0, "mg")],)],
    "score_fields": [(["field_00", "field_29"],), ([f"field_{i:02d}" for i in range(0, 30, 3)],), ([],)],
    "rank_candidates": [([("alpha", 0.9), ("beta", 0.95), ("gamma_long_name", 0.97)],), ([("a", 1.0), ("bb", 1.0)],)],
    "retry_schedule": [("invoice",)],
    "chunk_text": [("lorem ipsum dolor sit amet " * 6,), ("short",), ("x" * 100, 30, 10)],
    "select_confident": [([0.5, 0.74, 0.75, 0.9],), ([0.1],)],
    "extract_total": [([(3, 9.99), (2, 45.0)],), ([(1, 150.0)],), ([],)],
    "aggregate_scores": [([0.5, 0.9, 1.2],), ([],)],
    "keywords": [("The total of the invoice and a fee",), ("a of and",)],
    "generation_params": [("x" * 300,), ("short prompt",)],
    "encode_document": [("Invoice total: 42.",), ("TOTAL, invoice; other",)],
}
