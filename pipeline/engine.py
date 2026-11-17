"""Rule engine v2: the new state structure.

Compared with v1 (see ``reference/v1/engine.py``) v2 adds latch/cooldown
state, cumulative totals, an event ledger for reversals, and ``retract``
contributions. A v1 checkpoint therefore cannot be loaded as v2 directly.

State per account: ``window`` (last W amounts, oldest first), ``flagged``,
``events_since_flag``, ``total_amount`` (cumulative, may be reduced by a
reversal), ``total_flagged``, ``total_cleared``, ``last_offset``,
``last_flag_event``.

Global state: ``accounts``, ``totals`` and ``events`` -- a ledger of every
input event id with the contribution it caused (or null). The ledger is what
makes reversals deterministic: a second reversal of the same event can never
retract twice, so replaying a prefix is idempotent by construction.

Event shapes::

    {"id": "...", "account": "a", "amount": 120}
    {"id": "...", "reversal_of": "original-event-id"}

A reversal compensates the *ledger* (cumulative amount, and the flag/clear the
original event caused). It deliberately does not rewrite the rolling window:
the window reflects the stream as received, and the reversal is recorded as a
separate, auditable contribution. This is documented behaviour, not a bug.

Replay is deterministic: feeding the same events from offset 1 always yields
the same state and the same ordered contributions.
"""
from __future__ import annotations

import copy
from typing import Any, Iterable

VERSION = "v2"
WINDOW = 3
THRESHOLD = 100
COOLDOWN_EVENTS = 2


def rule_version() -> str:
    return VERSION


def empty_account() -> dict:
    return {
        "window": [],
        "flagged": False,
        "events_since_flag": 0,
        "total_amount": 0,
        "total_flagged": 0,
        "total_cleared": 0,
        "last_offset": 0,
        "last_flag_event": None,
    }


def empty_state() -> dict:
    return {
        "rule": VERSION,
        "window_size": WINDOW,
        "threshold": THRESHOLD,
        "cooldown": COOLDOWN_EVENTS,
        "accounts": {},
        "events": {},
        "totals": {
            "events": 0,
            "amount": 0,
            "reversed_amount": 0,
            "flagged": 0,
            "cleared": 0,
            "retracted": 0,
        },
    }


def is_hot(window: list[int]) -> bool:
    return (
        len(window) >= WINDOW
        and max(window[-WINDOW:]) - min(window[-WINDOW:]) >= THRESHOLD
    )


def apply_event(state: dict, event: dict, offset: int) -> list[dict]:
    """Apply one event to ``state`` in place; return the contributions it caused."""
    if offset != state["totals"]["events"] + 1:
        raise ValueError(
            f"non-contiguous replay: state has {state['totals']['events']} events, "
            f"got offset {offset}"
        )
    event_id = event.get("id")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError(f"offset {offset}: event id must be a non-empty string")
    if event_id in state["events"]:
        raise ValueError(f"offset {offset}: duplicate event id {event_id!r}")
    if "reversal_of" in event:
        contributions = _apply_reversal(state, event, offset)
    else:
        contributions = _apply_normal(state, event, offset)
    state["totals"]["events"] += 1
    return contributions


def _apply_normal(state: dict, event: dict, offset: int) -> list[dict]:
    acct = event.get("account")
    amount = event.get("amount")
    if not isinstance(acct, str) or not acct:
        raise ValueError(f"offset {offset}: event account must be a non-empty string")
    if isinstance(amount, bool) or not isinstance(amount, int):
        raise ValueError(f"offset {offset}: event amount must be an int")
    acc = state["accounts"].setdefault(acct, empty_account())
    acc["window"] = (acc["window"] + [amount])[-WINDOW:]
    acc["last_offset"] = offset
    acc["total_amount"] += amount
    acc["events_since_flag"] += 1
    state["totals"]["amount"] += amount

    out: list[dict] = []
    contribution: dict | None = None
    hot = is_hot(acc["window"])
    if hot and not acc["flagged"]:
        acc["flagged"] = True
        acc["events_since_flag"] = 0
        acc["total_flagged"] += 1
        acc["last_flag_event"] = event["id"]
        state["totals"]["flagged"] += 1
        contribution = {
            "kind": "flag",
            "account": acct,
            "event_id": event["id"],
            "rule": VERSION,
            "detail": {
                "window": list(acc["window"]),
                "spread": max(acc["window"]) - min(acc["window"]),
            },
        }
    elif not hot and acc["flagged"] and acc["events_since_flag"] > COOLDOWN_EVENTS:
        acc["flagged"] = False
        acc["events_since_flag"] = 0
        acc["total_cleared"] += 1
        state["totals"]["cleared"] += 1
        contribution = {
            "kind": "clear",
            "account": acct,
            "event_id": event["id"],
            "rule": VERSION,
            "detail": {"window": list(acc["window"])},
        }
    if contribution is not None:
        out.append(contribution)
    state["events"][event["id"]] = {
        "offset": offset,
        "account": acct,
        "amount": amount,
        "contribution": None if contribution is None else {
            "kind": contribution["kind"], "offset": offset, "event_id": event["id"]
        },
        "retracted": None,
    }
    return out


def _apply_reversal(state: dict, event: dict, offset: int) -> list[dict]:
    if "account" in event or "amount" in event:
        raise ValueError(
            f"offset {offset}: a reversal must not carry account/amount "
            "(its account is taken from the reversed event)"
        )
    target_id = event["reversal_of"]
    if not isinstance(target_id, str) or not target_id:
        raise ValueError(f"offset {offset}: reversal_of must be a non-empty string")
    target = state["events"].get(target_id)
    if target is None:
        raise ValueError(
            f"offset {offset}: cannot reverse unknown or future event {target_id!r}"
        )
    if target["retracted"] is not None:
        raise ValueError(
            f"offset {offset}: event {target_id!r} was already reversed at "
            f"offset {target['retracted']['offset']}"
        )
    if target.get("reversal_of") is not None:
        raise ValueError(
            f"offset {offset}: event {target_id!r} is itself a reversal; "
            "reversing reversals is not part of the rule and is refused"
        )
    acct = target["account"]
    acc = state["accounts"][acct]
    acc["total_amount"] -= target["amount"]
    state["totals"]["amount"] -= target["amount"]
    state["totals"]["reversed_amount"] += target["amount"]
    state["totals"]["retracted"] += 1
    target["retracted"] = {"offset": offset, "event_id": event["id"]}

    out: list[dict] = []
    caused = target["contribution"]
    if caused is not None:
        kind = caused["kind"]
        if kind == "flag":
            acc["total_flagged"] = max(0, acc["total_flagged"] - 1)
            state["totals"]["flagged"] = max(0, state["totals"]["flagged"] - 1)
            if acc["last_flag_event"] == target_id:
                acc["flagged"] = False
                acc["last_flag_event"] = None
                acc["events_since_flag"] = 0
        elif kind == "clear":
            acc["total_cleared"] = max(0, acc["total_cleared"] - 1)
            state["totals"]["cleared"] = max(0, state["totals"]["cleared"] - 1)
        out.append(
            {
                "kind": "retract",
                "account": acct,
                "event_id": event["id"],
                "rule": VERSION,
                "target": {
                    "kind": kind,
                    "account": acct,
                    "offset": caused["offset"],
                    "event_id": caused["event_id"],
                },
                "detail": {
                    "reversal_of": target_id,
                    "reversed_amount": target["amount"],
                },
            }
        )
    state["events"][event["id"]] = {
        "offset": offset,
        "account": acct,
        "amount": 0,
        "reversal_of": target_id,
        "contribution": None if not out else {
            "kind": "retract", "offset": offset, "event_id": event["id"]
        },
        "retracted": None,
    }
    return out


def replay(
    events: Iterable[dict],
    *,
    emit_from: int = 1,
    start_state: dict | None = None,
    start_offset: int = 0,
) -> tuple[dict, list[dict]]:
    """Replay ``events`` (which start at ``start_offset + 1``).

    Contributions at offsets < ``emit_from`` are computed (they advance the
    state) but not returned. This is how an upgrade reconstructs v2 state over
    offsets already settled by v1 without contributing twice.
    """
    state = copy.deepcopy(start_state) if start_state is not None else empty_state()
    emitted: list[dict] = []
    offset = start_offset
    for event in events:
        offset += 1
        produced = apply_event(state, event, offset)
        if offset >= emit_from:
            for contribution in produced:
                contribution["offset"] = offset
                emitted.append(contribution)
    return state, emitted


def assign_indices(records: list[dict]) -> None:
    """Deterministic (rule, offset, kind) indices so retries reuse the same keys."""
    groups: dict[tuple, list[dict]] = {}
    for rec in records:
        rec.setdefault("index", None)
        groups.setdefault((rec["rule"], rec["offset"], rec["kind"]), []).append(rec)
    for group in groups.values():
        if all(rec["index"] is not None for rec in group):
            continue
        group.sort(
            key=lambda r: (
                r.get("account", ""),
                r.get("event_id", ""),
                (r.get("target") or {}).get("offset", -1),
            )
        )
        for i, rec in enumerate(group):
            if rec["index"] is None:
                rec["index"] = i


def canonical_state(state: dict) -> str:
    from .util import canonical_json

    return canonical_json(state)


def state_digest(state: dict) -> str:
    from .util import digest

    return digest(state)


def validate_state(state: Any) -> None:
    if not isinstance(state, dict):
        raise ValueError("state must be an object")
    if state.get("rule") != VERSION:
        raise ValueError(f"state rule {state.get('rule')!r} != {VERSION!r}")
    for key in ("window_size", "threshold", "cooldown", "accounts", "events", "totals"):
        if key not in state:
            raise ValueError(f"state missing {key!r}")
    if not isinstance(state["accounts"], dict):
        raise ValueError("state.accounts must be an object")
    if not isinstance(state["events"], dict):
        raise ValueError("state.events must be an object")
    totals = state["totals"]
    if not isinstance(totals, dict) or "events" not in totals:
        raise ValueError("state.totals must be an object with events")
    for acct, acc in state["accounts"].items():
        if not isinstance(acct, str) or not acct:
            raise ValueError("account names must be non-empty strings")
        if not isinstance(acc, dict):
            raise ValueError(f"account {acct!r} state must be an object")
        for key in empty_account():
            if key not in acc:
                raise ValueError(f"account {acct!r} missing {key!r}")
        if not isinstance(acc["window"], list) or not all(
            isinstance(x, int) and not isinstance(x, bool) for x in acc["window"]
        ):
            raise ValueError(f"account {acct!r} window must be ints")
        if len(acc["window"]) > state["window_size"]:
            raise ValueError(f"account {acct!r} window too long")
    if totals["events"] != len(state["events"]):
        raise ValueError(
            f"totals.events {totals['events']} != ledger size {len(state['events'])}"
        )
