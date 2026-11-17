"""Checkpoint format v2 plus conversion from the frozen v1 checkpoint format.

A checkpoint is a full snapshot: ``state`` (the complete v2 rule state after
``offset`` input events) plus ``rule`` version and ``offset``.  The pair
(rule, offset) is the contract: a worker may resume from it, and a result
consumer can tell which version produced contributions at which offset.

Conversion rules
----------------
v1 lacks latch/cooldown state and cumulative totals.  Some of it can be
reconstructed from a deterministic replay of the events up to the checkpoint
offset; the rest can only be *approximated*.  We only accept a conversion when
the log proves the approximation exact:

* the retained v1 windows must equal the windows that a clean v2 replay up to
  ``last_offset`` produces (proof that no older history is missing), and
* the v1 rule id must be a known version (``v1``), otherwise no conversion
  semantics exist and we refuse.

If exact, the converted state is identical to what v2 would have held at that
offset.  Counters (latch/total) are replayed, not invented.  Anything else
raises :class:`IncompatibleCheckpoint` instead of pretending conversion
succeeded.
"""
from __future__ import annotations

import copy
from typing import Any

from . import engine

FORMAT = "checkpoint/v2"
SUPPORTED_SOURCE_FORMATS = ("checkpoint/v1",)
KNOWN_RULES = ("v1", "v2")


class IncompatibleCheckpoint(ValueError):
    """Raised when a checkpoint cannot be converted without lying about state."""

    def __init__(self, reasons: list[str]):
        self.reasons = reasons
        super().__init__("; ".join(reasons))


class LogTruncated(IncompatibleCheckpoint):
    """The checkpoint is ahead of the input log: the log was lost or truncated.

    Refusing is the only safe answer -- replaying a shorter prefix would produce
    state that disagrees with the checkpoint, and restarting from zero would
    re-contribute every offset.
    """


def build(state: dict, offset: int, emit_from: int | None = None) -> dict:
    """Snapshot ``state`` at ``offset``.

    ``emit_from`` is the first offset whose contributions are *not yet*
    settled. It is normally ``offset + 1``. An upgrade sets it higher when the
    old rule already published for some offsets beyond its checkpoint (crash
    after publish, before checkpoint): those events must still advance the new
    state, but must never be contributed again.
    """
    engine.validate_state(state)
    if state["totals"]["events"] != offset:
        raise ValueError(
            f"checkpoint offset {offset} does not match state totals.events {state['totals']['events']}"
        )
    if emit_from is None:
        emit_from = offset + 1
    if not isinstance(emit_from, int) or isinstance(emit_from, bool) or emit_from < 1:
        raise ValueError("emit_from must be a positive integer")
    return {
        "format": FORMAT,
        "rule": engine.VERSION,
        "offset": offset,
        "emit_from": emit_from,
        "state": copy.deepcopy(state),
    }


def validate(checkpoint: Any) -> None:
    if not isinstance(checkpoint, dict):
        raise IncompatibleCheckpoint(["checkpoint is not a JSON object"])
    if checkpoint.get("format") != FORMAT:
        raise IncompatibleCheckpoint(
            [f"format {checkpoint.get('format')!r} is not {FORMAT!r}"]
        )
    if checkpoint.get("rule") != engine.VERSION:
        raise IncompatibleCheckpoint(
            [f"checkpoint rule {checkpoint.get('rule')!r} is not {engine.VERSION!r}"]
        )
    offset = checkpoint.get("offset")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise IncompatibleCheckpoint(["checkpoint offset must be a non-negative integer"])
    emit_from = checkpoint.get("emit_from")
    if not isinstance(emit_from, int) or isinstance(emit_from, bool) or emit_from < 1:
        raise IncompatibleCheckpoint(["checkpoint emit_from must be a positive integer"])
    # emit_from may legitimately exceed offset + 1 when the old rule already
    # published for offsets past its checkpoint. The upper bound is the input
    # log length, which is checked by the store on save/load.

    try:
        engine.validate_state(checkpoint.get("state"))
        if checkpoint["state"]["totals"]["events"] != offset:
            raise ValueError(
                f"offset {offset} != state totals.events {checkpoint['state']['totals']['events']}"
            )
    except ValueError as exc:
        raise IncompatibleCheckpoint([f"state invalid: {exc}"]) from exc


def detect_format(raw: Any) -> str:
    if not isinstance(raw, dict):
        return "unknown"
    fmt = raw.get("format")
    if isinstance(fmt, str):
        return fmt
    return "unknown"


def convert_v1(
    legacy: Any,
    events: list[dict],
    *,
    settled: list[dict] | None = None,
) -> tuple[dict, list[dict], dict]:
    """Convert a v1 checkpoint into a v2 checkpoint using the event log.

    ``events`` must be the log prefix the v1 checkpoint was taken over (its
    length may exceed ``last_offset``; the excess is ignored here).  Returns
    ``(checkpoint, emitted_contributions, report)``.

    ``settled`` is the results log (or any subset). If the old rule already
    published contributions for offsets beyond its checkpoint, the converted
    checkpoint's ``emit_from`` is placed past them so the new rule advances the
    state without contributing those offsets a second time.

    ``emitted_contributions`` are the v2 contributions a full replay would
    have produced for the prefix. They are returned for the diff/preview only
    and must never be published: those offsets were already settled by v1.
    """
    reasons: list[str] = []
    if not isinstance(legacy, dict):
        raise IncompatibleCheckpoint(["legacy checkpoint is not a JSON object"])
    fmt = detect_format(legacy)
    if fmt not in SUPPORTED_SOURCE_FORMATS:
        reasons.append(f"unsupported source format {fmt!r}")
    rule = legacy.get("rule")
    if rule not in KNOWN_RULES:
        reasons.append(f"unknown rule version {rule!r}")
    offset = legacy.get("last_offset")
    if isinstance(offset, bool) or not isinstance(offset, int):
        reasons.append("last_offset must be an integer")
        offset = -1
    elif offset < 0:
        reasons.append("last_offset must be >= 0")
    elif offset > len(events):
        reasons.append(
            f"log only has {len(events)} events but checkpoint claims offset {offset}"
        )
    windows = legacy.get("windows")
    if not isinstance(windows, dict):
        reasons.append("windows must be an object")
        windows = {}
    else:
        for acct, vals in windows.items():
            if not isinstance(acct, str) or not acct:
                reasons.append(f"invalid account name {acct!r}")
                continue
            if not isinstance(vals, list) or len(vals) > engine.WINDOW:
                reasons.append(f"account {acct!r} window must be a list of <= {engine.WINDOW}")
                continue
            if not all(isinstance(v, int) and not isinstance(v, bool) for v in vals):
                reasons.append(f"account {acct!r} window values must be ints")
    if reasons:
        raise IncompatibleCheckpoint(reasons)

    prefix = events[:offset]
    # Prove the retained windows are consistent with the log: replay with v2's
    # own bookkeeping and require byte-equal windows (hence no missing history,
    # and the same event order/log integrity).
    try:
        replayed_state, _ = engine.replay(prefix)
    except ValueError as exc:
        raise IncompatibleCheckpoint([f"log replay failed: {exc}"]) from exc
    for acct, vals in windows.items():
        expected = replayed_state["accounts"].get(acct, {}).get("window", [])
        if list(vals) != list(expected):
            reasons.append(
                f"account {acct!r}: v1 window {vals} != log replay {expected} "
                "(checkpoint predates retained log or log was rewritten)"
            )
    for acct, acc in replayed_state["accounts"].items():
        if acct not in windows:
            reasons.append(
                f"account {acct!r} appears in log prefix but is missing from v1 windows"
            )
    if reasons:
        raise IncompatibleCheckpoint(reasons)

    # Exact: the v1 window equals what v2 would have.  The remaining v2 state
    # (latch, counters) is fully determined by the same event prefix.
    converted = engine.empty_state()
    converted.update(
        {
            "rule": engine.VERSION,
            "window_size": engine.WINDOW,
            "threshold": engine.THRESHOLD,
            "cooldown": engine.COOLDOWN_EVENTS,
            "accounts": copy.deepcopy(replayed_state["accounts"]),
            "events": copy.deepcopy(replayed_state["events"]),
            "totals": copy.deepcopy(replayed_state["totals"]),
        }
    )
    settled_offsets = [
        c["offset"] for c in (settled or []) if isinstance(c.get("offset"), int)
    ]
    emit_from = max([offset] + settled_offsets) + 1
    checkpoint = build(converted, offset, emit_from=emit_from)
    _, emitted = engine.replay(prefix)  # what v2 *would* have said
    report = {
        "source_format": fmt,
        "source_rule": rule,
        "source_offset": offset,
        "converted_rule": engine.VERSION,
        "converted_offset": offset,
        "accounts": len(converted["accounts"]),
        "events_replayed": offset,
        "history_events_with_contributions": len(emitted),
        "emit_from": emit_from,
        "settled_offsets_seen": len(settled_offsets),
        "exact": True,
    }
    return checkpoint, emitted, report


def convert_v1(
    legacy: Any,
    events: list[dict],
    *,
    settled: list[dict] | None = None,
) -> tuple[dict, list[dict], dict]:
    """Convert a v1 checkpoint into a v2 checkpoint using the event log.

    ``events`` must be the log prefix the v1 checkpoint was taken over (its
    length may exceed ``last_offset``; the excess is ignored here).  Returns
    ``(checkpoint, emitted_contributions, report)``.

    ``settled`` is the results log (or any subset). If the old rule already
    published contributions for offsets beyond its checkpoint, the converted
    checkpoint's ``emit_from`` is placed past them so the new rule advances the
    state without contributing those offsets a second time.

    ``emitted_contributions`` are the v2 contributions a full replay would
    have produced for the prefix. They are returned for the diff/preview only
    and must never be published: those offsets were already settled by v1.
    """
    reasons: list[str] = []
    if not isinstance(legacy, dict):
        raise IncompatibleCheckpoint(["legacy checkpoint is not a JSON object"])
    fmt = detect_format(legacy)
    if fmt not in SUPPORTED_SOURCE_FORMATS:
        reasons.append(f"unsupported source format {fmt!r}")
    rule = legacy.get("rule")
    if rule not in KNOWN_RULES:
        reasons.append(f"unknown rule version {rule!r}")
    offset = legacy.get("last_offset")
    if isinstance(offset, bool) or not isinstance(offset, int):
        reasons.append("last_offset must be an integer")
        offset = -1
    elif offset < 0:
        reasons.append("last_offset must be >= 0")
    elif offset > len(events):
        reasons.append(
            f"log only has {len(events)} events but checkpoint claims offset {offset}"
        )
    windows = legacy.get("windows")
    if not isinstance(windows, dict):
        reasons.append("windows must be an object")
        windows = {}
    else:
        for acct, vals in windows.items():
            if not isinstance(acct, str) or not acct:
                reasons.append(f"invalid account name {acct!r}")
                continue
            if not isinstance(vals, list) or len(vals) > engine.WINDOW:
                reasons.append(f"account {acct!r} window must be a list of <= {engine.WINDOW}")
                continue
            if not all(isinstance(v, int) and not isinstance(v, bool) for v in vals):
                reasons.append(f"account {acct!r} window values must be ints")
    if reasons:
        raise IncompatibleCheckpoint(reasons)

    prefix = events[:offset]
    # Prove the retained windows are consistent with the log: replay with v2's
    # own bookkeeping and require byte-equal windows (hence no missing history,
    # and the same event order/log integrity).
    try:
        replayed_state, _ = engine.replay(prefix)
    except ValueError as exc:
        raise IncompatibleCheckpoint([f"log replay failed: {exc}"]) from exc
    for acct, vals in windows.items():
        expected = replayed_state["accounts"].get(acct, {}).get("window", [])
        if list(vals) != list(expected):
            reasons.append(
                f"account {acct!r}: v1 window {vals} != log replay {expected} "
                "(checkpoint predates retained log or log was rewritten)"
            )
    for acct, acc in replayed_state["accounts"].items():
        if acct not in windows:
            reasons.append(
                f"account {acct!r} appears in log prefix but is missing from v1 windows"
            )
    if reasons:
        raise IncompatibleCheckpoint(reasons)

    # Exact: the v1 window equals what v2 would have.  The remaining v2 state
    # (latch, counters) is fully determined by the same event prefix.
    converted = engine.empty_state()
    converted.update(
        {
            "rule": engine.VERSION,
            "window_size": engine.WINDOW,
            "threshold": engine.THRESHOLD,
            "cooldown": engine.COOLDOWN_EVENTS,
            "accounts": copy.deepcopy(replayed_state["accounts"]),
            "events": copy.deepcopy(replayed_state["events"]),
            "totals": copy.deepcopy(replayed_state["totals"]),
        }
    )
    settled_offsets = [
        c["offset"] for c in (settled or []) if isinstance(c.get("offset"), int)
    ]
    emit_from = max([offset] + settled_offsets) + 1
    checkpoint = build(converted, offset, emit_from=emit_from)
    _, emitted = engine.replay(prefix)  # what v2 *would* have said
    report = {
        "source_format": fmt,
        "source_rule": rule,
        "source_offset": offset,
        "converted_rule": engine.VERSION,
        "converted_offset": offset,
        "accounts": len(converted["accounts"]),
        "events_replayed": offset,
        "history_events_with_contributions": len(emitted),
        "emit_from": emit_from,
        "settled_offsets_seen": len(settled_offsets),
        "exact": True,
    }
    return checkpoint, emitted, report
