"""Reference implementation of the *old* continuous-event rule engine (v1).

This file is intentionally frozen: it models what shipped before the state was
extended. It is the source of truth for (a) the legacy checkpoint format and
(b) the expected contributions from replaying historical events. Do not import
it from the live pipeline.

Old rule: keep a running list of recent amounts per account; emit a ``flag``
contribution when the >=3 most recent amounts for the account satisfy
``max - min >= threshold``. A rule fires once per *input event* at most.
The checkpoint only stored the last ``window`` amounts and the last rule id,
which is why it cannot be read directly by the new structure.
"""
from __future__ import annotations

from typing import Iterable

VERSION = "v1"
WINDOW = 3
THRESHOLD = 100


def rule_version() -> str:
    return VERSION


def contributions_for(events: Iterable[dict], window: int = WINDOW,
                      threshold: int = THRESHOLD) -> list[dict]:
    """Pure replay: return ordered contributions for the given events."""
    recent: dict[str, list[int]] = {}
    out: list[dict] = []
    for ev in events:
        acct = ev["account"]
        recent.setdefault(acct, []).append(int(ev["amount"]))
        recent[acct] = recent[acct][-window:]
        amounts = recent[acct]
        if len(amounts) >= window and max(amounts) - min(amounts) >= threshold:
            out.append({
                "kind": "flag",
                "account": acct,
                "event_id": ev["id"],
                "rule": VERSION,
                "detail": {"window": list(amounts), "spread": max(amounts) - min(amounts)},
            })
    return out


def legacy_checkpoint(events: list[dict], window: int = WINDOW) -> dict:
    """Build the checkpoint the v1 worker would have written for these events."""
    recent: dict[str, list[int]] = {}
    for ev in events:
        acct = ev["account"]
        recent.setdefault(acct, []).append(int(ev["amount"]))
        recent[acct] = recent[acct][-window:]
    return {
        "format": "checkpoint/v1",
        "rule": VERSION,
        "last_offset": len(events),
        "windows": {acct: list(vals) for acct, vals in sorted(recent.items())},
    }
