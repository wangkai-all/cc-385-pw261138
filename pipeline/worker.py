"""Rule worker process: consume new input offsets, emit contributions, checkpoint.

The worker is one of three independent processes (ingester, worker, publisher).
Its only durable write order is: contributions first, checkpoint second. That
ordering plus contribution-key dedupe makes a crash anywhere between the two
safe: replay of the same offsets produces the same ordered contributions, and
the publisher rejects already-seen keys.
"""
from __future__ import annotations

import copy
import time

from . import checkpoint as checkpoint_mod
from . import engine, store
from .checkpoint import LogTruncated
from .util import file_lock, now_iso

MAX_VERIFY_EVENTS = 10_000


class UpgradeRequired(RuntimeError):
    """The live checkpoint is in an old/incompatible format and needs conversion."""

    def __init__(self, reasons: list[str]):
        self.reasons = reasons
        super().__init__(
            "live checkpoint cannot be read by the new worker; run the web upgrade "
            "preview/commit (conversion or log replay) first: " + "; ".join(reasons)
        )


def _load_state(pipeline: store.Pipeline) -> tuple[dict, int, int]:
    """Return (state, offset, emit_from). ``emit_from`` is the first offset whose
    contributions are not yet settled; it only exceeds offset+1 after an
    upgrade whose old rule already published for newer offsets."""
    try:
        ckpt = pipeline.load_checkpoint()
    except LogTruncated:
        raise
    except checkpoint_mod.IncompatibleCheckpoint as exc:
        raise UpgradeRequired(exc.reasons) from exc
    if ckpt is None:
        return engine.empty_state(), 0, 1
    return copy.deepcopy(ckpt["state"]), ckpt["offset"], ckpt["emit_from"]


def verify_checkpoint_against_log(pipeline: store.Pipeline, ckpt: dict) -> dict:
    """Recompute v2 state from the log prefix and compare with the checkpoint."""
    events = pipeline.read_events()[: ckpt["offset"]]
    state, _ = engine.replay(events)
    matches = engine.canonical_state(state) == engine.canonical_state(ckpt["state"])
    return {
        "offset": ckpt["offset"],
        "events_available": len(events),
        "state_matches_log": matches,
        "state_digest": engine.state_digest(ckpt["state"]),
        "log_digest": engine.state_digest(state),
    }


def run_once(
    pipeline: store.Pipeline,
    *,
    batch: int = 25,
    crash_at: str = store.CRASH_NONE,
) -> dict:
    if batch < 1:
        raise ValueError("batch must be >= 1")
    crash_at = store._check_crash_point(crash_at)
    with file_lock(pipeline.engine_lock):
        state, offset, emit_from = _load_state(pipeline)
        events = pipeline.read_events()
        if offset > len(events):
            raise LogTruncated(
                f"checkpoint is at offset {offset} but the input log has only "
                f"{len(events)} events; refusing to continue"
            )
        pending = events[offset:]
        if not pending:
            return {
                "processed": 0,
                "from_offset": offset,
                "to_offset": offset,
                "contributions": 0,
                "pending": 0,
                "emit_from": emit_from,
                "crashed": None,
                "rule": engine.VERSION,
                "at": now_iso(),
            }
        batch_events = pending[:batch]
        work_state = copy.deepcopy(state)
        emitted: list[dict] = []
        for i, event in enumerate(batch_events):
            event_offset = offset + i + 1
            produced = engine.apply_event(work_state, event, event_offset)
            if event_offset < emit_from:
                # Already published by the old rule: advance state only.
                continue
            for contribution in produced:
                record = dict(contribution)
                record["rule"] = engine.VERSION
                record["offset"] = event_offset
                emitted.append(record)
        engine.assign_indices(emitted)
        written = pipeline.append_contributions(emitted)
        new_ckpt = checkpoint_mod.build(
            work_state, offset + len(batch_events), emit_from=max(emit_from, offset + len(batch_events) + 1)
        )
        try:
            pipeline.save_checkpoint(new_ckpt, crash_at=crash_at, run_id=None)
        except store.CrashInjected as exc:
            return {
                "processed": 0,
                "from_offset": offset,
                "to_offset": offset,
                "attempted_to_offset": offset + len(batch_events),
                "contributions": written,
                "contributions_emitted": len(emitted),
                "pending": len(pending),
                "emit_from": emit_from,
                "crashed": exc.point,
                "rule": engine.VERSION,
                "at": now_iso(),
            }
        return {
            "processed": len(batch_events),
            "from_offset": offset,
            "to_offset": offset + len(batch_events),
            "contributions": written,
            "contributions_emitted": len(emitted),
            "pending": len(pending) - len(batch_events),
            "emit_from": emit_from,
            "crashed": None,
            "rule": engine.VERSION,
            "at": now_iso(),
        }


def run_loop(pipeline: store.Pipeline, *, interval: float = 0.25, batch: int = 25) -> None:
    while True:
        result = run_once(pipeline, batch=batch)
        if result["processed"] == 0 and result["crashed"] is None:
            time.sleep(interval)
        elif result["crashed"]:
            raise RuntimeError(f"worker simulated crash at {result['crashed']}")
