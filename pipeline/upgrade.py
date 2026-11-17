"""Upgrade orchestration: preview, convert, commit, crash-safe recovery.

Contract for every upgrade run:

    source_rule @ source_offset  ->  target_rule @ same offset

No contribution is re-emitted for an offset that the old rule already settled.
The new rule takes over strictly from ``source_offset + 1``. Pending input that
arrived while the three processes kept running is then processed by the worker
with the usual dedupe rules.

Preview is side-effect free. Commit only ever writes the checkpoint (via a
tmp+rename swap with injectable crash points); contribution publication is the
worker/publisher's job.
"""
from __future__ import annotations

import copy
import json
import os
from typing import Any

from . import checkpoint as checkpoint_mod
from . import engine, worker
from .store import Pipeline, CrashInjected, contribution_key
from .util import digest, file_lock, new_id, now_iso

class UpgradeError(ValueError):
    pass


# --------------------------------------------------------------------- preview
def _source(pipeline: Pipeline) -> tuple[dict | None, str]:
    """Pick the raw source checkpoint: live file if it is not v2, else the legacy file."""
    if os.path.exists(pipeline.ckpt_path):
        try:
            if pipeline.load_checkpoint() is not None:
                return None, "live"  # already v2 / current format
        except checkpoint_mod.IncompatibleCheckpoint:
            with open(pipeline.ckpt_path, "r", encoding="utf-8") as fh:
                return json.load(fh), "live"
    legacy = pipeline.legacy_checkpoint_raw()
    if legacy is not None:
        return legacy, "legacy"
    return None, "none"


def preview(
    pipeline: Pipeline,
    *,
    mode: str = "auto",
    replay_offset: int | None = None,
    source_rule_override: str | None = None,
) -> dict:
    """Compute (but do not persist) an upgrade plan.

    mode: ``auto`` uses the v1 checkpoint; ``replay`` rebuilds from the log at
    ``replay_offset`` for the case where the legacy checkpoint is untrusted or
    lost. Replay still needs to know which rule settled the prefix: it reads it
    from the v1 checkpoint when present, else from ``source_rule_override``
    (the results log's own rule tags are used as a cross-check).
    """
    run_id = new_id("run")
    events = pipeline.read_events()
    settled = pipeline.read_contributions()
    raw, source_kind = _source(pipeline)
    source_offset = None
    source_rule = None
    suppressed: list[dict] = []
    reasons: list[str] = []

    if mode == "auto":
        if source_kind == "live" and raw is not None:
            legacy_copy = pipeline.legacy_checkpoint_raw()
            disagreement = _disagreement(raw, legacy_copy)
            if disagreement:
                return _refused(
                    run_id,
                    pipeline,
                    [
                        "the live old checkpoint and the archived v1 copy disagree "
                        f"({disagreement}); refusing to pick one"
                    ],
                    source_kind,
                )
        if raw is None:
            reason = (
                "live checkpoint is already the new format; nothing to convert"
                if source_kind == "live"
                else "no legacy checkpoint to convert"
            )
            return _refused(run_id, pipeline, [reason], source_kind)
        source_rule = raw.get("rule")
        source_offset = raw.get("last_offset")
        try:
            converted, _, report = checkpoint_mod.convert_v1(raw, events, settled=settled)
        except checkpoint_mod.IncompatibleCheckpoint as exc:
            return _refused(run_id, pipeline, exc.reasons, source_kind)
        reasons.extend(report.pop("reasons", []))
    elif mode == "replay":
        if replay_offset is None:
            replay_offset = raw.get("last_offset") if isinstance(raw, dict) else 0
        if not isinstance(replay_offset, int) or isinstance(replay_offset, bool) or replay_offset < 0:
            return _refused(run_id, pipeline, ["replay offset must be a non-negative integer"], source_kind)
        if replay_offset > len(events):
            return _refused(
                run_id,
                pipeline,
                [f"replay offset {replay_offset} is beyond the log ({len(events)} events)"],
                source_kind,
            )
        source_rule = (
            raw.get("rule") if isinstance(raw, dict)
            else source_rule_override
        )
        source_offset = raw.get("last_offset") if isinstance(raw, dict) else replay_offset
        if source_rule_override is not None:
            if source_rule is not None and source_rule != source_rule_override:
                return _refused(
                    run_id,
                    pipeline,
                    [
                        f"source rule override {source_rule_override!r} disagrees with "
                        f"the old checkpoint rule {source_rule!r}"
                    ],
                    source_kind,
                )
            source_rule = source_rule_override
        if source_rule not in checkpoint_mod.KNOWN_RULES:
            return _refused(
                run_id, pipeline, [f"unknown source rule {source_rule!r}"], source_kind
            )
        if isinstance(source_offset, int) and source_offset > replay_offset:
            return _refused(
                run_id,
                pipeline,
                [f"replay offset {replay_offset} is behind the old checkpoint {source_offset}"],
                source_kind,
            )
        state, _ = engine.replay(events[:replay_offset])
        settled_offsets = [
            c["offset"] for c in settled
            if isinstance(c.get("offset"), int) and c["offset"] > replay_offset
        ]
        settled_rules = {
            c.get("rule") for c in settled
            if isinstance(c.get("offset"), int) and c["offset"] <= replay_offset
        }
        unexpected = settled_rules - {source_rule, None}
        if unexpected:
            return _refused(
                run_id,
                pipeline,
                [
                    f"results log tags contributions inside the replayed prefix with "
                    f"{sorted(str(r) for r in unexpected)}, but the source rule is "
                    f"{source_rule!r}; the rule/offset mapping would be ambiguous"
                ],
                source_kind,
            )
        converted = checkpoint_mod.build(
            state, replay_offset, emit_from=max([replay_offset] + settled_offsets) + 1
        )
        # Offsets past the old checkpoint but inside the replay point are
        # deliberately not contributed for: the operator chose the replay point
        # because the old checkpoint could not be trusted. Make the gap visible
        # rather than silent.
        old_offset = raw.get("last_offset") if isinstance(raw, dict) else None
        if isinstance(old_offset, int):
            contributed = {
                c["offset"] for c in settled if isinstance(c.get("offset"), int)
            }
            gap = [
                o for o in range(old_offset + 1, replay_offset + 1)
                if o not in contributed
            ]
        else:
            gap = []
        report = {
            "source_format": "log-replay",
            "source_rule": source_rule,
            "source_offset": replay_offset,
            "converted_rule": engine.VERSION,
            "converted_offset": replay_offset,
            "accounts": len(state["accounts"]),
            "events_replayed": replay_offset,
            "uncontributed_offsets": gap,
            "emit_from": converted["emit_from"],
        }
        source_offset = replay_offset
    else:
        raise UpgradeError(f"unknown mode {mode!r}")

    converted_offset = converted["offset"]
    target_offset = converted_offset
    emit_from = converted["emit_from"]

    # What the new rule would have contributed inside the already-settled
    # prefix. These are NOT emitted; they are shown so the operator can see the
    # semantic change explicitly instead of it happening invisibly.
    _, historical = engine.replay(events[:converted_offset])
    suppressed = [
        {k: c[k] for k in ("kind", "account", "offset", "rule") if k in c}
        for c in historical
    ]

    # What the new rule will contribute for input that arrived during the
    # upgrade. Shown for review; the worker produces (and dedupes) the real ones.
    _, pending_preview = engine.replay(
        events[converted_offset:],
        start_state=copy.deepcopy(converted["state"]),
        start_offset=converted_offset,
        emit_from=emit_from,
    )
    for c in pending_preview:
        engine.assign_indices([c])

    # In replay mode the old checkpoint is anchored at a *different* offset than
    # the conversion point, so window equality is not a meaningful check; the
    # diff still shows both sides side by side.
    diff = _upgrade_diff(
        raw, converted,
        comparison=("exact_window_equality" if mode == "auto" else "different_offsets"),
    )
    run = {
        "id": run_id,
        "kind": "upgrade",
        "status": "previewed",
        "created_at": now_iso(),
        "mode": mode,
        "source_kind": source_kind,
        "source_rule": source_rule,
        "source_offset": source_offset,
        "target_rule": engine.VERSION,
        "target_offset": target_offset,
        "emit_from": emit_from,
        "events_in_log": len(events),
        "converted_checkpoint": converted,
        "converted_digest": digest(converted),
        "diff": diff,
        "report": report,
        "suppressed": suppressed,
        "suppressed_count": len(suppressed),
        "pending_preview": [
            {k: c[k] for k in ("kind", "account", "offset", "event_id", "rule", "index") if k in c}
            for c in pending_preview
        ],
        "reasons": reasons,
        "warnings": list(report.get("uncontributed_offsets") or []),
        "crash_at": None,
        "verified": None,
    }
    pipeline.save_run(run)
    return run


def _disagreement(live: Any, archived: Any) -> str | None:
    """Describe how two v1 checkpoints differ, or None if they agree."""
    if not isinstance(archived, dict):
        return None
    for field in ("format", "rule", "last_offset", "windows"):
        if live.get(field) != archived.get(field):
            return f"{field} differs"
    return None


def _refused(run_id: str, pipeline: Pipeline, reasons: list[str], source_kind: str) -> dict:
    run = {
        "id": run_id,
        "kind": "upgrade",
        "status": "refused",
        "created_at": now_iso(),
        "source_kind": source_kind,
        "source_rule": None,
        "source_offset": None,
        "target_rule": engine.VERSION,
        "target_offset": None,
        "emit_from": None,
        "converted_checkpoint": None,
        "diff": None,
        "report": {},
        "suppressed": [],
        "pending_preview": [],
        "reasons": reasons,
        "crash_at": None,
        "verified": None,
    }
    pipeline.save_run(run)
    return run


def _upgrade_diff(raw: Any, converted: dict, *, comparison: str = "exact_window_equality") -> dict:
    """Diff the old structure against the new one, field by field.

    ``comparison`` is ``exact_window_equality`` when both snapshots describe the
    same offset (conversion), or ``different_offsets`` for a log replay, where
    the old windows and new windows legitimately cover different spans.
    """
    legacy_windows = {}
    if isinstance(raw, dict) and isinstance(raw.get("windows"), dict):
        legacy_windows = raw["windows"]
    accounts: list[dict] = []
    state_accounts = converted["state"]["accounts"]
    for acct in sorted(set(legacy_windows) | set(state_accounts)):
        old_window = legacy_windows.get(acct)
        new = state_accounts.get(acct)
        entry = {
            "account": acct,
            "old_window": old_window,
            "new_window": None if new is None else new["window"],
            "new_latched": None if new is None else new["flagged"],
            "new_total_amount": None if new is None else new["total_amount"],
            "window_equal": (
                None if comparison == "different_offsets"
                else old_window == (None if new is None else new["window"])
            ),
            "fields_only_in_new": (
                [] if new is None else
                ["flagged", "events_since_flag", "total_amount", "total_flagged",
                 "total_cleared", "last_offset"]
            ),
        }
        accounts.append(entry)
    old = None
    if isinstance(raw, dict):
        old = {
            "format": raw.get("format"),
            "rule": raw.get("rule"),
            "offset": raw.get("last_offset"),
            "accounts": len(legacy_windows),
        }
    return {
        "structure": "flat per-account windows -> per-account latch + counters + global totals",
        "comparison": comparison,
        "old": old,
        "new": {
            "format": converted["format"],
            "rule": converted["rule"],
            "offset": converted["offset"],
            "accounts": len(state_accounts),
            "totals": converted["state"]["totals"],
        },
        "accounts": accounts,
        "rule_version_offsets": {
            "old_rule": None if old is None else old["rule"],
            "old_settled_through": None if old is None else old["offset"],
            "new_rule": converted["rule"],
            "new_settled_through": converted["offset"],
            "new_contributions_start_at": converted["emit_from"],
        },
    }


# ---------------------------------------------------------------------- commit
def commit(
    pipeline: Pipeline,
    run_id: str,
    *,
    crash_at: str = "none",
    expected_digest: str | None = None,
) -> dict:
    """Persist the previewed checkpoint, optionally crashing at a chosen point."""
    run = pipeline.load_run(run_id)
    if run is None:
        raise UpgradeError(f"unknown run {run_id!r}")
    if run["status"] not in ("previewed", "crashed", "committed", "recovered"):
        raise UpgradeError(f"run {run_id!r} is {run['status']!r} and cannot be committed")
    if run.get("reasons"):
        raise UpgradeError("refused runs cannot be committed: " + "; ".join(run["reasons"]))
    checkpoint = run["converted_checkpoint"]
    if expected_digest and digest(checkpoint) != expected_digest:
        raise UpgradeError("previewed checkpoint digest does not match expected digest")
    # Refuse to overwrite a v2 checkpoint that is ahead of the conversion point
    # with different history: that would break the (rule, offset) contract.
    current = _live_or_none(pipeline)
    if current is not None and current["offset"] > checkpoint["offset"]:
        raise UpgradeError(
            f"live checkpoint is already at offset {current['offset']} > conversion offset "
            f"{checkpoint['offset']}; refusing to move it backwards"
        )
    run["status"] = "committing"
    run["crash_at"] = crash_at
    pipeline.save_run(run)
    try:
        # Serialise with the worker: a concurrent batch must not interleave a
        # checkpoint write between the offset check above and the swap below.
        with file_lock(pipeline.engine_lock):
            pipeline.save_checkpoint(checkpoint, crash_at=crash_at, run_id=run_id)
    except CrashInjected as exc:
        run["status"] = "crashed"
        run["crash_at"] = exc.point
        run["crashed_at"] = now_iso()
        pipeline.save_run(run)
        return run
    except Exception:
        run["status"] = "commit_failed"
        pipeline.save_run(run)
        raise
    run["status"] = "committed"
    run["committed_at"] = now_iso()
    run["crash_at"] = None
    run["verified"] = verify(pipeline, run)
    pipeline.save_run(run)
    return run


def _live_or_none(pipeline: Pipeline) -> dict | None:
    """Live checkpoint if it is v2, else None (an old format is not "progress")."""
    try:
        return pipeline.load_checkpoint()
    except checkpoint_mod.IncompatibleCheckpoint:
        return None


def recover(pipeline: Pipeline, run_id: str) -> dict:
    """Recover after a crash at any commit point and prove what state we are in.

    Idempotent: rerunning never duplicates contributions because commit only
    replaces the checkpoint and contribution publication dedupes on keys.
    """
    run = pipeline.load_run(run_id)
    if run is None:
        raise UpgradeError(f"unknown run {run_id!r}")
    if run["status"] == "refused":
        raise UpgradeError("refused runs cannot be recovered")
    if not run.get("converted_checkpoint"):
        raise UpgradeError("run has no previewed checkpoint to recover")
    checkpoint = run["converted_checkpoint"]
    with file_lock(pipeline.engine_lock):
        current = _live_or_none(pipeline)
        if current is None:
            # Swap never happened (or was interrupted before the live file existed).
            action = "reapplied"
            pipeline.save_checkpoint(checkpoint, run_id=run_id)
        elif current["rule"] == checkpoint["rule"] and current["offset"] == checkpoint["offset"] \
                and digest(current) == digest(checkpoint):
            action = "already_committed"
        elif current["offset"] <= checkpoint["offset"]:
            # Live file is still the old structure or an older v2 point: reapplying
            # the previewed checkpoint is deterministic.
            action = "reapplied"
            pipeline.save_checkpoint(checkpoint, run_id=run_id)
        else:
            # The worker already continued past the upgrade point. Recovering
            # must not rewind it; instead prove the upgrade did land.
            events = pipeline.read_events()[: checkpoint["offset"]]
            prefix_state, _ = engine.replay(events)
            if engine.state_digest(prefix_state) != engine.state_digest(checkpoint["state"]):
                raise UpgradeError(
                    f"live checkpoint at offset {current['offset']} is ahead of the upgrade "
                    f"offset {checkpoint['offset']} and its prefix does not replay to the "
                    "previewed state; refusing to guess"
                )
            action = "superseded"
    run["status"] = "recovered"
    run["recovery"] = {"action": action, "at": now_iso()}
    run["verified"] = verify(pipeline, run)
    pipeline.save_run(run)
    return run


# ---------------------------------------------------------------------- verify
def verify(pipeline: Pipeline, run: dict) -> dict:
    """Verify an upgrade's (rule, offset) contract against the live files.

    Safe to call after the worker has continued past the upgrade point: the
    previewed checkpoint must still be exactly what a replay of the log prefix
    yields, and the live checkpoint must be a consistent continuation of it.
    """
    preview = run["converted_checkpoint"]
    target = run["target_offset"]
    reasons: list[str] = []
    live = _live_or_none(pipeline)
    if live is None:
        try:
            pipeline.load_checkpoint()
        except checkpoint_mod.IncompatibleCheckpoint as exc:
            reasons.extend(f"live checkpoint: {r}" for r in exc.reasons)
            return {
                "ok": False,
                "reasons": reasons,
                "at": now_iso(),
                "previewed_digest": run.get("converted_digest"),
            }
        return {"ok": False, "reasons": ["no live checkpoint"], "at": now_iso()}
    if live["rule"] != run["target_rule"]:
        reasons.append(f"live rule {live['rule']} != target {run['target_rule']}")
    if live["offset"] < target:
        reasons.append(f"live offset {live['offset']} is behind upgrade target {target}")

    events = pipeline.read_events()
    prefix_state, _ = engine.replay(events[:target])
    if engine.state_digest(prefix_state) != engine.state_digest(preview["state"]):
        reasons.append("log prefix no longer replays to the previewed checkpoint")

    log_check = worker.verify_checkpoint_against_log(pipeline, live)
    if live["offset"] <= len(events) and not log_check["state_matches_log"]:
        reasons.append("live state does not match a deterministic replay of the log prefix")

    contributions = pipeline.read_contributions()
    keys = [contribution_key(c) for c in contributions]
    if len(keys) != len(set(keys)):
        reasons.append("duplicate contribution keys in results log")
    # The old rule published through source_offset, and under the watermark
    # scheme may also have published up to emit_from - 1 beyond it.
    settled_by_old_rule = max(run["source_offset"] or 0, preview["emit_from"] - 1)
    bad_offsets = [
        c["offset"] for c in contributions
        if c.get("rule") == engine.VERSION and c.get("offset", 0) <= settled_by_old_rule
    ]
    if bad_offsets:
        reasons.append(
            "new-rule contributions exist at offsets already settled by the old "
            f"rule: {sorted(set(bad_offsets))[:5]}"
        )
    by_rule: dict[str, int] = {}
    for c in contributions:
        by_rule[str(c.get("rule"))] = by_rule.get(str(c.get("rule")), 0) + 1
    return {
        "ok": not reasons,
        "reasons": reasons,
        "at": now_iso(),
        "log": log_check,
        "contributions": len(contributions),
        "contributions_by_rule": by_rule,
        "previewed_digest": run["converted_digest"],
        "replayed_digest": engine.state_digest(prefix_state),
        "source_rule_offset": f"{run['source_rule']}@{run['source_offset']}",
        "live_rule_offset": f"{live['rule']}@{live['offset']}",
        "emit_from": live.get("emit_from"),
        "note": (
            "emit_from is the first offset the new rule may contribute for; it "
            "stays above offsets the old rule already published"
            if live.get("emit_from", 0) > live["offset"] + 1 else
            "new rule contributes from the next offset"
        ),
    }
