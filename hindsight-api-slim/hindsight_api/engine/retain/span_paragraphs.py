"""Paragraph segmentation for the bank-wide source-span filter (fork mitigation E3).

Pure functions with no database or engine imports, shared by the retain-time filter
(``span_filter.py``) and the Postgres store's index (``memories/pg/span_index.py``) so both
segment a chunk the same way.

A chunk is one of three shapes, and a paragraph is always a blank-line-separated block of the
TEXT a person or tool wrote, never of the JSON around it:

* a JSON array of turns (``[{"role": ..., "content": "..."}]``, how conversation chunks are
  stored): each turn's string ``content`` is segmented;
* JSON Lines, one turn object per line: each line's string ``content`` is segmented;
* anything else: the whole text is segmented.

:func:`rewrite_paragraphs` rebuilds the chunk with chosen paragraphs replaced. JSON shapes are
re-serialised the way the chunker writes them (``json.dumps(..., ensure_ascii=False)``); the
rebuilt text is only ever shown to the LLM, never stored.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

MARKER_PREFIX = "[already retained: "
# How much of a replaced paragraph its marker quotes, so the model keeps the thread of the text.
MARKER_QUOTE_CHARS = 80

_PARA_SPLIT = re.compile(r"(\n[ \t]*\n\s*)")


def para_hash(paragraph: str) -> bytes:
    """Identity of a paragraph: 12-byte blake2b of its stripped text."""
    return hashlib.blake2b(paragraph.strip().encode("utf-8"), digest_size=12).digest()


def marker_for(paragraph: str) -> str:
    text = " ".join(paragraph.split())
    quote = text[:MARKER_QUOTE_CHARS]
    return f"{MARKER_PREFIX}{quote}{'…' if len(text) > MARKER_QUOTE_CHARS else ''}]"


def _rewrite_text(text: str, replace: Callable[[str], str | None]) -> str:
    """Segment ``text`` on blank lines; ``replace`` gets each stripped paragraph and returns a
    substitute or None to keep it. Separators are preserved byte for byte."""
    parts = _PARA_SPLIT.split(text)
    changed = False
    for i in range(0, len(parts), 2):  # even slots are paragraphs, odd slots separators
        raw = parts[i]
        stripped = raw.strip()
        if not stripped:
            continue
        sub = replace(stripped)
        if sub is not None:
            lead = raw[: len(raw) - len(raw.lstrip())]
            trail = raw[len(raw.rstrip()) :]
            parts[i] = f"{lead}{sub}{trail}"
            changed = True
    return "".join(parts) if changed else text


def _is_turn(obj: object) -> bool:
    return isinstance(obj, dict) and isinstance(obj.get("content"), str)


def rewrite_paragraphs(text: str, replace: Callable[[str], str | None]) -> str:
    """Apply ``replace`` to every paragraph of a chunk (see module docstring for shapes)."""
    head = text.lstrip()
    if head.startswith("["):
        try:
            turns = json.loads(text)
        except ValueError:
            turns = None
        if isinstance(turns, list) and turns and all(isinstance(t, dict) for t in turns):
            changed = False
            for turn in turns:
                if _is_turn(turn):
                    new = _rewrite_text(turn["content"], replace)
                    if new is not turn["content"]:
                        turn["content"] = new
                        changed = True
            return json.dumps(turns, ensure_ascii=False) if changed else text
    if head.startswith("{"):
        lines = text.split("\n")
        parsed: list[object] = []
        for line in lines:
            if not line.strip():
                parsed.append(None)
                continue
            try:
                parsed.append(json.loads(line))
            except ValueError:
                parsed = []
                break
        if parsed and any(_is_turn(p) for p in parsed):
            changed = False
            for i, obj in enumerate(parsed):
                if _is_turn(obj):
                    assert isinstance(obj, dict)
                    new = _rewrite_text(obj["content"], replace)
                    if new is not obj["content"]:
                        obj["content"] = new
                        lines[i] = json.dumps(obj, ensure_ascii=False)
                        changed = True
            return "\n".join(lines) if changed else text
    return _rewrite_text(text, replace)


def iter_paragraphs(text: str, min_chars: int) -> Iterator[str]:
    """Every stripped paragraph of a chunk at least ``min_chars`` long, in order."""
    found: list[str] = []

    def _collect(p: str) -> None:
        if len(p) >= min_chars and not p.startswith(MARKER_PREFIX):
            found.append(p)
        return None

    rewrite_paragraphs(text, _collect)
    yield from found


@dataclass
class SpanChunk:
    """What one stored chunk contributes to the index: the paragraphs its extraction saw in full
    (``shown``, it is now a source for them) and the ones it saw as markers (``refs``: hash,
    source chunk id, paragraph length)."""

    chunk_id: str
    chunk_hash: str | None
    shown: list[bytes] = field(default_factory=list)
    refs: list[tuple[bytes, str, int]] = field(default_factory=list)


@dataclass
class SpanMaintenance:
    """Counts from one maintenance pass over a schema's span index, plus the documents whose
    marked chunks lost their source past the grace period: those chunks must be re-extracted."""

    indexed: int = 0
    repointed: int = 0
    orphaned: int = 0
    cleared: int = 0
    restore: list[tuple[str, str, str]] = field(default_factory=list)  # (bank_id, document_id, chunk_id)
