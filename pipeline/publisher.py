"""Result publisher: at-least-once consumption with exact-once accounting.

The publisher reads contributions at offsets past its ack watermark, applies
them to a cumulative ledger, and records the ack. A crash before the ack means
the next run re-reads the same records; dedupe by contribution key guarantees
the ledger is not contributed twice.
"""
from __future__ import annotations

import json
import os
from . import store
from .util import file_lock, now_iso

LEDGER_FORMAT = "ledger/v1"


class Publisher:
    def __init__(self, pipeline: store.Pipeline):
        self.pipeline = pipeline
        self.ledger_path = os.path.join(pipeline.results_dir, "ledger.json")
        self.pub_lock = os.path.join(pipeline.results_dir, "publisher.lock")

    def _load_ledger(self) -> dict:
        if not os.path.exists(self.ledger_path):
            return {
                "format": LEDGER_FORMAT,
                "consumed_through": 0,
                "applied_keys": [],
                "accounts": {},
                "by_rule": {},
                "retractions": 0,
            }
        with open(self.ledger_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _save_ledger(self, ledger: dict) -> None:
        tmp = f"{self.ledger_path}.tmp.{os.getpid()}"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(ledger, fh, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.ledger_path)

    def publish(self, *, limit: int | None = None, crash_before_ack: bool = False) -> dict:
        with file_lock(self.pub_lock):
            ledger = self._load_ledger()
            applied = set(ledger["applied_keys"])
            records = self.pipeline.read_contributions()
            processed = 0
            new_keys = 0
            retractions = 0
            max_offset = ledger["consumed_through"]
            for rec in records:
                if limit is not None and processed >= limit:
                    break
                key = store.contribution_key(rec)
                processed += 1
                max_offset = max(max_offset, rec.get("offset", 0))
                if key in applied:
                    continue
                applied.add(key)
                new_keys += 1
                target = rec.get("target")
                if rec.get("kind") == "retract" and target:
                    self._retract(ledger, target)
                    bucket = ledger["by_rule"].setdefault(
                        str(rec.get("rule")), {"flag": 0, "clear": 0, "retract": 0, "other": 0}
                    )
                    bucket["retract"] = bucket.get("retract", 0) + 1
                    retractions += 1
                else:
                    self._apply(ledger, rec)
                ledger["applied_keys"].append(key)
            ledger["consumed_through"] = max_offset
            ledger["retractions"] += retractions
            if crash_before_ack:
                # Simulate dying after applying but before durability.
                return {
                    "crashed": True,
                    "processed": processed,
                    "new": new_keys,
                    "consumed_through": ledger["consumed_through"],
                }
            self._save_ledger(ledger)
            self.pipeline.record_ack(ledger["consumed_through"], "mixed")
            return {
                "crashed": False,
                "processed": processed,
                "new": new_keys,
                "retractions": retractions,
                "consumed_through": ledger["consumed_through"],
                "at": now_iso(),
            }

    def _apply(self, ledger: dict, rec: dict) -> None:
        acct = rec.get("account")
        acc = ledger["accounts"].setdefault(
            acct, {"flag": 0, "clear": 0, "last_offset": 0, "rule": None}
        )
        kind = rec.get("kind")
        if kind in ("flag", "clear"):
            acc[kind] = acc.get(kind, 0) + 1
        acc["last_offset"] = rec.get("offset", acc["last_offset"])
        acc["rule"] = rec.get("rule")
        rule = str(rec.get("rule"))
        bucket = ledger["by_rule"].setdefault(
            rule, {"flag": 0, "clear": 0, "retract": 0, "other": 0}
        )
        bucket[kind if kind in ("flag", "clear") else "other"] += 1

    @staticmethod
    def _retract(ledger: dict, target: dict) -> None:
        acct = target.get("account")
        kind = target.get("kind")
        acc = ledger["accounts"].get(acct)
        if acc and kind in ("flag", "clear") and acc.get(kind, 0) > 0:
            acc[kind] -= 1
            acc["retracted"] = acc.get("retracted", 0) + 1

    def snapshot(self) -> dict:
        ledger = self._load_ledger()
        return {
            "consumed_through": ledger["consumed_through"],
            "applied": len(ledger["applied_keys"]),
            "accounts": ledger["accounts"],
            "by_rule": ledger["by_rule"],
            "retractions": ledger["retractions"],
        }
