"""Deterministic demo data: seeds the v1 checkpoint + history + live input.

``seed_pipeline(root, legacy_offset=12, total=24)``:
  * writes events 1..total to the input log,
  * writes a genuine v1 checkpoint at ``legacy_offset`` that the *v1* worker
    would have produced (computed with the frozen reference implementation),
  * leaves events legacy_offset+1..total pending, as if the ingester kept
    running across the upgrade.

``scenario(kind)`` builds event lists for the tests/web:
  * ``basic``  - flags, a cooldown clear, and an account that stays calm,
  * ``quiet``  - never crosses the threshold,
  * ``mixed``  - long, alternating accounts, several flag/clear cycles.
"""
from __future__ import annotations

import json
import os
import sys

from .util import atomic_write_json, sha256_hex

_REFERENCE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reference", "v1")
if _REFERENCE_DIR not in sys.path:
    sys.path.insert(0, _REFERENCE_DIR)

import engine as v1_engine  # noqa: E402  (frozen reference implementation)


def _ev(i: int, account: str, amount: int) -> dict:
    return {"id": f"ev-{i:04d}", "account": account, "amount": amount,
            "ts": f"2026-01-01T00:00:{i % 60:02d}.000Z"}


def scenario(kind: str = "basic") -> list[dict]:
    if kind == "quiet":
        return [_ev(i, "calm", 10 + i % 3) for i in range(1, 21)]
    if kind == "mixed":
        rows = []
        accts = ["alpha", "beta", "gamma"]
        for i in range(1, 61):
            acct = accts[i % 3]
            if i % 11 == 0:
                amount = 400 + i
            elif i % 7 == 0:
                amount = 5
            else:
                amount = 50 + (i * 37) % 30
            rows.append(_ev(i, acct, amount))
        return rows
    # basic: v1 checkpoint at offset 12, pending input (incl. a reversal) after it
    rows = [
        _ev(1, "alpha", 100),
        _ev(2, "alpha", 120),
        _ev(3, "alpha", 260),   # alpha flags (spread 160)
        _ev(4, "beta", 50),
        _ev(5, "beta", 50),
        _ev(6, "beta", 50),
        _ev(7, "alpha", 120),
        _ev(8, "alpha", 120),
        _ev(9, "alpha", 120),   # window 120,120,120: calm, cooldown clears alpha
        _ev(10, "alpha", 121),
        _ev(11, "beta", 50),
        _ev(12, "beta", 50),    # v1 checkpoint is taken here
        _ev(13, "gamma", 300),
        _ev(14, "gamma", 90),
        _ev(15, "gamma", 190),  # gamma flags under v2
        _ev(16, "beta", 50),
        _ev(17, "alpha", 500),  # alpha flags again
        {"id": "ev-0018", "reversal_of": "ev-0017",
         "ts": "2026-01-01T00:00:18.000Z"},  # reverses alpha's second flag
        _ev(19, "alpha", 510),
        _ev(20, "alpha", 520),
        _ev(21, "gamma", 300),
        _ev(22, "gamma", 301),
        _ev(23, "beta", 50),
        _ev(24, "beta", 50),
    ]
    return rows


def seed_pipeline(root: str, *, kind: str = "basic", legacy_offset: int = 12,
                  total: int | None = None) -> dict:
    from .store import Pipeline

    events = scenario(kind)
    total = len(events) if total is None else total
    if legacy_offset > total:
        raise ValueError("legacy_offset cannot exceed total")
    pipeline = Pipeline(root)
    if os.path.exists(pipeline.input_path):
        raise FileExistsError(f"{pipeline.input_path} already exists; use a fresh data dir")
    for ev in events[:total]:
        pipeline.ingest(ev)
    prefix = events[:legacy_offset]
    legacy = v1_engine.legacy_checkpoint(prefix)
    legacy["seeded_at"] = "2026-01-01T00:00:00.000Z"
    legacy["history_digest"] = sha256_hex(json.dumps(prefix, sort_keys=True))
    # The old worker's live checkpoint sits at the normal path; the new worker
    # must refuse it until an explicit upgrade has been committed.
    atomic_write_json(pipeline.ckpt_path, legacy)
    atomic_write_json(os.path.join(root, "legacy", "checkpoint_v1.json"), legacy)
    # Contributions the v1 worker already published for the prefix. They stay
    # in the results log and must never be re-emitted by v2.
    index_of = {ev["id"]: i + 1 for i, ev in enumerate(events)}
    historical = []
    for c in v1_engine.contributions_for(prefix):
        rec = dict(c)
        rec["offset"] = index_of[rec["event_id"]]
        rec["index"] = 0
        historical.append(rec)
    pipeline.append_contributions(historical)
    return {
        "events": total,
        "legacy_offset": legacy_offset,
        "legacy_checkpoint": legacy,
        "pending": total - legacy_offset,
        "historical_contributions": len(historical),
    }
