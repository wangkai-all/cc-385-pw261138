"""Tests for the emit_from watermark and the (rule, offset) contract."""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import checkpoint as ckpt_mod  # noqa: E402
from pipeline import engine, scenario, store, upgrade, worker  # noqa: E402


class WatermarkTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="pipeline-wm-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.info = scenario.seed_pipeline(self.root)
        self.pipeline = store.Pipeline(self.root)

    def _publish_ahead(self, offsets):
        """Simulate the old rule publishing contributions past its checkpoint."""
        for offset in offsets:
            self.pipeline.append_contributions([{
                "kind": "flag",
                "account": "alpha",
                "event_id": f"ev-{offset:04d}",
                "rule": "v1",
                "offset": offset,
                "index": 0,
            }])

    def test_emit_from_skips_offsets_old_rule_published(self):
        self._publish_ahead([13, 14, 15])
        run = upgrade.preview(self.pipeline)
        self.assertEqual(run["status"], "previewed")
        self.assertEqual(run["emit_from"], 16)
        self.assertEqual(run["converted_checkpoint"]["emit_from"], 16)
        committed = upgrade.commit(self.pipeline, run["id"])
        self.assertTrue(committed["verified"]["ok"], committed["verified"]["reasons"])

        result = worker.run_once(self.pipeline, batch=100)
        self.assertEqual(result["emit_from"], 16)
        after = self.pipeline.read_contributions()
        v2 = [c for c in after if c["rule"] == "v2"]
        self.assertTrue(v2, "offsets past the watermark still contribute")
        self.assertTrue(all(c["offset"] >= 16 for c in v2), [c["offset"] for c in v2])
        # Events 13..15 still advanced the state even though they emitted nothing.
        state = self.pipeline.load_checkpoint()["state"]
        self.assertEqual(state["totals"]["events"], 24)

    def test_watermark_never_regresses(self):
        self._publish_ahead([13, 14, 15])
        run = upgrade.commit(self.pipeline, upgrade.preview(self.pipeline)["id"])
        self.pipeline.append_contributions([{
            "kind": "flag", "account": "beta", "event_id": "ev-0013",
            "rule": "v1", "offset": 13, "index": 1,
        }])
        worker.run_once(self.pipeline, batch=1)  # offset 13 only
        checkpoint = self.pipeline.load_checkpoint()
        self.assertEqual(checkpoint["offset"], 13)
        self.assertEqual(checkpoint["emit_from"], 16)

    def test_watermark_beyond_log_rejected_on_save(self):
        state = engine.empty_state()
        raw = ckpt_mod.build(state, 0, emit_from=len(self.pipeline.read_events()) + 5)
        ckpt_mod.validate(raw)  # structurally fine
        with self.assertRaises(ckpt_mod.IncompatibleCheckpoint):
            self.pipeline.save_checkpoint(raw)  # but ahead of the input log
        raw["emit_from"] = 0
        with self.assertRaises(ckpt_mod.IncompatibleCheckpoint):
            ckpt_mod.validate(raw)

    def test_conversion_report_shows_emit_from(self):
        self._publish_ahead([20])
        run = upgrade.preview(self.pipeline)
        self.assertEqual(run["report"]["emit_from"], 21)
        self.assertEqual(run["diff"]["rule_version_offsets"]["new_contributions_start_at"], 21)


if __name__ == "__main__":
    unittest.main()
