"""Storage layout + crash-safe checkpoint swap + contribution dedupe.

Layout under a data directory::

    input/events.jsonl       append-only input log (ingester writes)
    checkpoints/rule.json    live checkpoint pointer for the worker
    checkpoints/current.json -> active checkpoint (copy, not symlink)
    checkpoints/archive/     superseded checkpoints (kept for audits)
    results/contributions.jsonl  published contributions (publisher writes)
    results/acks.jsonl       offsets the publisher has observed
    runs/<id>/               preview/upgrade run artifacts

Crash-safety model
------------------
* Input log appends are fsynced; readers only consume complete lines.
* A checkpoint swap writes ``tmp`` -> fsync -> ``os.replace`` -> fsync(dir).
  An interrupted swap leaves either the old file or the new file, never a
  half-written one; ``BEFORE_REPLACE`` / ``AFTER_REPLACE`` crash points let
  tests prove the worker resumes from a consistent (rule, offset) pair.
* Contributions are keyed by ``(rule, offset, contribution_index)``.  The
  publisher has an at-least-once input; it dedupes on that key so a retried
  batch can never be counted twice.  The worker additionally never re-applies
  an offset <= its checkpoint offset, so a crash between "append
  contributions" and "checkpoint swap" replays the window deterministically
  and the duplicate keys are rejected on publication.
"""
from __future__ import annotations

import json
import os
import shutil

from . import checkpoint as checkpoint_mod
from . import engine
from .util import atomic_write_json, canonical_json, digest, file_lock, now_iso, now_ms

CONTRIB_KEY_FIELDS = ("rule", "offset", "index")

CRASH_NONE = "none"
CRASH_BEFORE_WRITE = "before_write"       # die before state is durable
CRASH_AFTER_WRITE = "after_write"         # state file written, identity not yet committed
CRASH_BEFORE_REPLACE = "before_replace"   # tmp written, live file untouched
CRASH_AFTER_REPLACE = "after_replace"     # live file swapped, process dies right after
CRASH_POINTS = (
    CRASH_NONE,
    CRASH_BEFORE_WRITE,
    CRASH_AFTER_WRITE,
    CRASH_BEFORE_REPLACE,
    CRASH_AFTER_REPLACE,
)


class CrashInjected(RuntimeError):
    """Simulated abrupt stop (used by tests and the web crash controls)."""

    def __init__(self, point: str):
        self.point = point
        super().__init__(f"simulated crash at {point}")


class Pipeline:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.input_path = os.path.join(self.root, "input", "events.jsonl")
        self.input_lock = os.path.join(self.root, "input", ".lock")
        self.ckpt_dir = os.path.join(self.root, "checkpoints")
        self.ckpt_path = os.path.join(self.ckpt_dir, "rule.json")
        self.ckpt_meta_path = os.path.join(self.ckpt_dir, "meta.json")
        self.ckpt_lock = os.path.join(self.ckpt_dir, ".lock")
        self.engine_lock = os.path.join(self.ckpt_dir, "engine.lock")
        self.archive_dir = os.path.join(self.ckpt_dir, "archive")
        self.results_dir = os.path.join(self.root, "results")
        self.contrib_path = os.path.join(self.results_dir, "contributions.jsonl")
        self.acks_path = os.path.join(self.results_dir, "acks.jsonl")
        self.results_lock = os.path.join(self.results_dir, ".lock")
        self.runs_dir = os.path.join(self.root, "runs")
        for d in (
            os.path.dirname(self.input_path),
            self.ckpt_dir,
            self.archive_dir,
            self.results_dir,
            self.runs_dir,
        ):
            os.makedirs(d, exist_ok=True)

    # ------------------------------------------------------------------ input
    def read_events(self) -> list[dict]:
        return _read_valid_jsonl(self.input_path)

    def ingest(self, event: dict, *, crash_at: str = CRASH_NONE) -> dict:
        if not isinstance(event, dict):
            raise ValueError("event must be an object")
        reserved = {"offset", "digest", "key"} & set(event)
        if reserved:
            raise ValueError(f"event carries reserved field(s): {sorted(reserved)}")
        if not isinstance(event.get("id"), str) or not event["id"]:
            raise ValueError("event id must be a non-empty string")
        if "reversal_of" in event:
            if not isinstance(event["reversal_of"], str) or not event["reversal_of"]:
                raise ValueError("reversal_of must be a non-empty event id")
            if "account" in event or "amount" in event:
                raise ValueError("a reversal must not carry account/amount")
        else:
            if not isinstance(event.get("account"), str) or not event["account"]:
                raise ValueError("event account must be a non-empty string")
            if isinstance(event.get("amount"), bool) or not isinstance(event.get("amount"), int):
                raise ValueError("event amount must be an int")
        record = dict(event)
        record.setdefault("ts", now_iso())
        crash_at = _check_crash_point(crash_at)
        if crash_at != CRASH_NONE:
            raise CrashInjected(crash_at)  # ingester never reaches durability
        with file_lock(self.input_lock):
            existing = _read_valid_jsonl(self.input_path)
            ids = {e.get("id") for e in existing}
            if record["id"] in ids:
                raise ValueError(f"duplicate event id {record['id']!r}")
            if "reversal_of" in record and record["reversal_of"] not in ids:
                raise ValueError(
                    f"reversal_of {record['reversal_of']!r} is not (yet) in the input log"
                )
            stored = dict(record)
            stored["digest"] = digest(record)
            line = json.dumps(stored, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            with open(self.input_path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            out = dict(record)
            out["offset"] = len(existing) + 1
        return out

    # ------------------------------------------------------------- checkpoint
    def load_checkpoint(self) -> dict | None:
        if not os.path.exists(self.ckpt_path):
            return None
        with open(self.ckpt_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        checkpoint_mod.validate(raw)
        self._check_watermark(raw)
        return raw

    def _check_watermark(self, checkpoint: dict) -> None:
        # The watermark can never point past the next offset to be ingested;
        # if it does, the input log is shorter than the state claims to have
        # consumed. (`emit_from == len(events) + 1` is normal.)
        limit = len(self.read_events()) + 1
        if checkpoint["emit_from"] > limit:
            raise checkpoint_mod.LogTruncated([
                f"emit_from {checkpoint['emit_from']} is beyond the input log "
                f"(next offset {limit})"
            ])

    def legacy_checkpoint_raw(self) -> dict | None:
        path = os.path.join(self.root, "legacy", "checkpoint_v1.json")
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def save_checkpoint(self, checkpoint: dict, *, crash_at: str = CRASH_NONE,
                        run_id: str | None = None) -> dict:
        checkpoint_mod.validate(checkpoint)
        crash_at = _check_crash_point(crash_at)
        self._check_watermark(checkpoint)
        with file_lock(self.ckpt_lock):
            if crash_at == CRASH_BEFORE_WRITE:
                raise CrashInjected(crash_at)
            tmp = f"{self.ckpt_path}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(checkpoint, fh, indent=2, sort_keys=True)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            if crash_at == CRASH_AFTER_WRITE:
                raise CrashInjected(crash_at)
            _archive_previous(self.ckpt_path, self.archive_dir)
            if crash_at == CRASH_BEFORE_REPLACE:
                raise CrashInjected(crash_at)
            os.replace(tmp, self.ckpt_path)
            _fsync_dir(self.ckpt_dir)
            atomic_write_json(
                self.ckpt_meta_path,
                {
                    "rule": checkpoint["rule"],
                    "offset": checkpoint["offset"],
                    "saved_at": now_iso(),
                    "saved_at_ms": now_ms(),
                    "run_id": run_id,
                    "digest": digest(checkpoint),
                },
            )
            if crash_at == CRASH_AFTER_REPLACE:
                raise CrashInjected(crash_at)
        return checkpoint

    # ---------------------------------------------------------------- results
    def append_contributions(self, contributions: list[dict]) -> int:
        """Append contributions idempotently; return number newly written."""
        with file_lock(self.results_lock):
            seen = self.contribution_keys()
            fresh = []
            for c in contributions:
                key = contribution_key(c)
                if key in seen:
                    continue
                seen.add(key)
                stored = dict(c)
                stored["key"] = key
                stored["digest"] = digest(stored)
                fresh.append(stored)
            if fresh:
                with open(self.contrib_path, "a", encoding="utf-8") as fh:
                    for c in fresh:
                        fh.write(json.dumps(c, sort_keys=True, separators=(",", ":")) + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())
        return len(fresh)

    def contribution_keys(self) -> set[str]:
        # Derive the key instead of trusting a stored one, so hand-written or
        # older result logs still dedupe correctly.
        return {contribution_key(rec) for rec in _read_valid_jsonl(self.contrib_path)}

    def read_contributions(self) -> list[dict]:
        return _read_valid_jsonl(self.contrib_path)

    def record_ack(self, offset: int, rule: str) -> None:
        with file_lock(self.results_lock):
            line = json.dumps({"offset": offset, "rule": rule, "ts": now_iso()},
                              sort_keys=True, separators=(",", ":"))
            with open(self.acks_path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())

    # ------------------------------------------------------------------- runs
    def save_run(self, run: dict) -> str:
        run_id = run["id"]
        path = os.path.join(self.runs_dir, run_id, "run.json")
        atomic_write_json(path, run)
        return path

    def load_run(self, run_id: str) -> dict | None:
        path = os.path.join(self.runs_dir, run_id, "run.json")
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def list_runs(self) -> list[dict]:
        out = []
        if not os.path.isdir(self.runs_dir):
            return out
        for name in sorted(os.listdir(self.runs_dir)):
            run = self.load_run(name)
            if run:
                out.append({
                    "id": run["id"],
                    "kind": run.get("kind"),
                    "status": run.get("status"),
                    "created_at": run.get("created_at"),
                    "source_offset": run.get("source_offset"),
                    "target_offset": run.get("target_offset"),
                    "crash_at": run.get("crash_at"),
                })
        return out

    def metrics(self) -> dict:
        events = self.read_events()
        contributions = self.read_contributions()
        ckpt = None
        live_legacy = None
        try:
            ckpt = self.load_checkpoint()
        except checkpoint_mod.IncompatibleCheckpoint:
            # The old worker's checkpoint sits at the live path; report it so
            # the UI can show exactly what needs upgrading.
            if os.path.exists(self.ckpt_path):
                with open(self.ckpt_path, "r", encoding="utf-8") as fh:
                    live_legacy = json.load(fh)
        accounted = contributions_accounted(contributions)
        flagged = sum(1 for c in contributions if c.get("kind") == "flag")
        cleared = sum(1 for c in contributions if c.get("kind") == "clear")
        legacy = self.legacy_checkpoint_raw()
        return {
            "events": len(events),
            "contributions": len(contributions),
            "flagged": flagged,
            "cleared": cleared,
            "accounted": accounted,
            "checkpoint": None if ckpt is None else {
                "rule": ckpt["rule"],
                "offset": ckpt["offset"],
                "emit_from": ckpt["emit_from"],
                "digest": digest(ckpt),
            },
            "legacy": None if legacy is None else {
                "format": legacy.get("format"),
                "rule": legacy.get("rule"),
                "last_offset": legacy.get("last_offset"),
            },
            "live_raw": None if live_legacy is None else {
                "format": live_legacy.get("format"),
                "rule": live_legacy.get("rule"),
                "last_offset": live_legacy.get("last_offset"),
            },
            "pending": max(0, len(events) - (ckpt["offset"] if ckpt else 0)),
            "contributions_by_rule": _count_by(contributions, "rule"),
            "contributions_by_offset": _count_by(contributions, "offset"),
        }


# --------------------------------------------------------------------- helpers
def contribution_key(c: dict) -> str:
    """Identity of one published contribution.

    (rule, offset) ties it to a position in the stream; kind separates a flag
    from a clear at the same event; index orders contributions of the same kind
    (e.g. two accounts flagged by the same event). Including kind means the key
    is stable even if only one of them is retried.
    """
    return canonical_json({
        "rule": c.get("rule"),
        "offset": c.get("offset"),
        "kind": c.get("kind"),
        "index": c.get("index"),
    })


def contributions_accounted(contributions: list[dict]) -> dict:
    """Net cumulative flag/clear per account, after applying retractions.

    A ``retract`` cancels exactly the contribution named in ``target`` (by
    account, kind and original offset). Counting the target set rather than a
    running total keeps the result independent of log order and of how many
    times the publisher replayed a retry.
    """
    counted: dict[tuple, int] = {}
    retracted: set[tuple] = set()
    for c in contributions:
        acct = c.get("account")
        kind = c.get("kind")
        if kind in ("flag", "clear"):
            counted[(acct, kind, c.get("offset"), c.get("rule"))] = (
                counted.get((acct, kind, c.get("offset"), c.get("rule")), 0) + 1
            )
        elif kind == "retract" and isinstance(c.get("target"), dict):
            target = c["target"]
            retracted.add((
                target.get("account"), target.get("kind"),
                target.get("offset"), c.get("rule"),
            ))
    flagged: dict[str, int] = {}
    cleared: dict[str, int] = {}
    for (acct, kind, offset, rule), n in counted.items():
        net = 0 if (acct, kind, offset, rule) in retracted else n
        if not net:
            continue
        bucket = flagged if kind == "flag" else cleared
        bucket[acct] = bucket.get(acct, 0) + net
    return {"flagged": flagged, "cleared": cleared}


def _count_by(records: list[dict], field: str) -> dict:
    out: dict[str, int] = {}
    for rec in records:
        key = str(rec.get(field))
        out[key] = out.get(key, 0) + 1
    return out


def _check_crash_point(point: str) -> str:
    if point not in CRASH_POINTS:
        raise ValueError(f"unknown crash point {point!r}")
    return point


def _archive_previous(path: str, archive_dir: str) -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as fh:
            prev = json.load(fh)
        rule = prev.get("rule", "unknown")
        offset = prev.get("offset", "unknown")
    except Exception:
        rule, offset = "unknown", "unknown"
    stamp = now_ms()
    dest = os.path.join(archive_dir, f"{stamp}-{rule}-{offset}.json")
    shutil.copy2(path, dest)


def _fsync_dir(directory: str) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _read_valid_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    out: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{lineno}: corrupt JSON: {exc}") from exc
            stored = rec.pop("digest", None)
            if stored is not None and stored != digest(rec):
                raise ValueError(f"{path}:{lineno}: digest mismatch (log was edited)")
            rec["offset"] = lineno if "offset" not in rec else rec["offset"]
            out.append(rec)
    return out
