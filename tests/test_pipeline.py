"""Tests: engine semantics, conversion, crash points, dedupe, web API."""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import checkpoint as ckpt_mod  # noqa: E402
from pipeline import engine, scenario, store, upgrade, worker  # noqa: E402
from pipeline.publisher import Publisher  # noqa: E402


def seed(**kwargs):
    root = tempfile.mkdtemp(prefix="pipeline-test-")
    info = scenario.seed_pipeline(root, **kwargs)
    return root, store.Pipeline(root), info


def _read_bytes(path):
    with open(path, "rb") as fh:
        return fh.read()


def ev(i: int, account: str, amount: int) -> dict:
    return {"id": f"t-{i}", "account": account, "amount": amount}


class EngineTests(unittest.TestCase):
    def test_flag_then_clear(self):
        state = engine.empty_state()
        produced = []
        for i, amount in enumerate([100, 120, 260, 120, 120, 120], 1):
            produced.extend(engine.apply_event(state, ev(i, "a", amount), i))
        self.assertEqual([c["kind"] for c in produced], ["flag", "clear"])
        self.assertEqual(state["accounts"]["a"]["total_flagged"], 1)
        self.assertEqual(state["accounts"]["a"]["total_cleared"], 1)
        self.assertEqual(state["totals"]["amount"], 840)

    def test_replay_is_deterministic(self):
        events = scenario.scenario("mixed")
        one, c1 = engine.replay(events)
        two, c2 = engine.replay(events)
        self.assertEqual(engine.state_digest(one), engine.state_digest(two))
        self.assertEqual(c1, c2)
        self.assertEqual(one["totals"]["events"], len(events))

    def test_reversal_retracts_exactly_once(self):
        state = engine.empty_state()
        events = [ev(1, "a", 100), ev(2, "a", 120), ev(3, "a", 260)]
        for i, e in enumerate(events, 1):
            engine.apply_event(state, e, i)
            self.assertEqual(state["totals"]["events"], i)
        out = engine.apply_event(state, {"id": "r1", "reversal_of": "t-3"}, 4)
        self.assertEqual([c["kind"] for c in out], ["retract"])
        self.assertEqual(out[0]["target"], {"kind": "flag", "account": "a", "offset": 3, "event_id": "t-3"})
        self.assertEqual(state["accounts"]["a"]["total_amount"], 220)
        self.assertEqual(state["totals"]["reversed_amount"], 260)
        with self.assertRaises(ValueError):
            engine.apply_event(state, {"id": "r2", "reversal_of": "t-3"}, 5)

    def test_reversal_of_unknown_event_rejected(self):
        state = engine.empty_state()
        with self.assertRaises(ValueError):
            engine.apply_event(state, {"id": "r1", "reversal_of": "nope"}, 1)

    def test_non_contiguous_replay_rejected(self):
        state = engine.empty_state()
        with self.assertRaises(ValueError):
            engine.apply_event(state, ev(1, "a", 10), 2)

    def test_invalid_state_rejected(self):
        state = engine.empty_state()
        state["accounts"]["a"] = {"window": []}
        with self.assertRaises(ValueError):
            engine.validate_state(state)

    def test_checkpoint_refuses_old_format(self):
        state = engine.empty_state()
        raw = {"format": "checkpoint/v1", "rule": "v1", "last_offset": 0,
               "windows": {}, "state": state, "offset": 0}
        with self.assertRaises(ckpt_mod.IncompatibleCheckpoint):
            ckpt_mod.validate(raw)


class ConversionTests(unittest.TestCase):
    def test_v1_converts_exactly(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        converted, historical, report = ckpt_mod.convert_v1(
            pipeline.legacy_checkpoint_raw(), pipeline.read_events()
        )
        self.assertTrue(report["exact"])
        self.assertEqual(converted["offset"], info["legacy_offset"])
        expected_state, _ = engine.replay(pipeline.read_events()[: info["legacy_offset"]])
        self.assertEqual(
            engine.state_digest(converted["state"]), engine.state_digest(expected_state)
        )
        self.assertEqual(len(historical), 2)  # alpha flag + clear in the v1 prefix

    def test_conversion_refuses_edited_window(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        legacy = pipeline.legacy_checkpoint_raw()
        legacy["windows"]["alpha"] = [1, 2, 3]
        with self.assertRaises(ckpt_mod.IncompatibleCheckpoint) as ctx:
            ckpt_mod.convert_v1(legacy, pipeline.read_events())
        self.assertIn("alpha", " ".join(ctx.exception.reasons))

    def test_conversion_refuses_checkpoint_beyond_log(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        legacy = pipeline.legacy_checkpoint_raw()
        legacy["last_offset"] = 99
        with self.assertRaises(ckpt_mod.IncompatibleCheckpoint):
            ckpt_mod.convert_v1(legacy, pipeline.read_events())

    def test_conversion_refuses_unknown_rule(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        legacy = pipeline.legacy_checkpoint_raw()
        legacy["rule"] = "v0-beta"
        with self.assertRaises(ckpt_mod.IncompatibleCheckpoint):
            ckpt_mod.convert_v1(legacy, pipeline.read_events())

    def test_conversion_refuses_missing_account(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        legacy = pipeline.legacy_checkpoint_raw()
        legacy["windows"].pop("beta")
        with self.assertRaises(ckpt_mod.IncompatibleCheckpoint):
            ckpt_mod.convert_v1(legacy, pipeline.read_events())


class UpgradeFlowTests(unittest.TestCase):
    def test_preview_is_side_effect_free(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        before = _read_bytes(pipeline.ckpt_path)
        results_before = _read_bytes(pipeline.contrib_path)
        run = upgrade.preview(pipeline)
        self.assertEqual(run["status"], "previewed")
        self.assertEqual(_read_bytes(pipeline.ckpt_path), before)
        self.assertEqual(_read_bytes(pipeline.contrib_path), results_before)
        self.assertEqual(run["emit_from"], info["legacy_offset"] + 1)
        self.assertEqual(len(run["suppressed"]), 2)

    def test_worker_blocks_until_upgrade(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        with self.assertRaises(worker.UpgradeRequired):
            worker.run_once(pipeline)

    def test_full_upgrade_does_not_double_contribute(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        existing = pipeline.read_contributions()
        run = upgrade.commit(pipeline, upgrade.preview(pipeline)["id"])
        self.assertEqual(run["status"], "committed")
        self.assertTrue(run["verified"]["ok"], run["verified"]["reasons"])
        worker.run_once(pipeline, batch=100)
        after = pipeline.read_contributions()
        keys = [store.contribution_key(c) for c in after]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(after[: len(existing)], existing)
        new = after[len(existing):]
        self.assertTrue(new)
        self.assertTrue(all(c["rule"] == "v2" for c in new))
        self.assertTrue(all(c["offset"] > info["legacy_offset"] for c in new))
        # Every pending offset the worker claims to have processed is in the log.
        self.assertEqual(pipeline.load_checkpoint()["offset"], len(pipeline.read_events()))

    def test_upgrade_rules_are_recorded_with_offsets(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        run = upgrade.commit(pipeline, upgrade.preview(pipeline)["id"])
        self.assertEqual(run["source_rule"], "v1")
        self.assertEqual(run["source_offset"], info["legacy_offset"])
        self.assertEqual(run["target_rule"], "v2")
        self.assertEqual(run["target_offset"], info["legacy_offset"])
        self.assertEqual(run["diff"]["rule_version_offsets"]["new_contributions_start_at"],
                         info["legacy_offset"] + 1)
        worker.run_once(pipeline, batch=100)
        verify = upgrade.verify(pipeline, pipeline.load_run(run["id"]))
        self.assertTrue(verify["ok"], verify["reasons"])

    def test_replay_mode_from_log(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        run = upgrade.preview(pipeline, mode="replay", replay_offset=14)
        self.assertEqual(run["status"], "previewed")
        self.assertEqual(run["target_offset"], 14)
        self.assertEqual(run["source_rule"], "v1")
        run = upgrade.commit(pipeline, run["id"])
        self.assertTrue(run["verified"]["ok"], run["verified"]["reasons"])
        result = worker.run_once(pipeline, batch=100)
        self.assertEqual(result["from_offset"], 14)

    def test_replay_refuses_offset_behind_old_checkpoint(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        run = upgrade.preview(pipeline, mode="replay", replay_offset=5)
        self.assertEqual(run["status"], "refused")
        self.assertIn("behind", " ".join(run["reasons"]))

    def test_refused_preview_cannot_commit(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        legacy = pipeline.legacy_checkpoint_raw()
        legacy["windows"]["beta"] = [9, 9, 9]
        with open(pipeline.ckpt_path, "w") as fh:
            json.dump(legacy, fh)
        run = upgrade.preview(pipeline)
        self.assertEqual(run["status"], "refused")
        with self.assertRaises(upgrade.UpgradeError):
            upgrade.commit(pipeline, run["id"])

    def test_conflicting_live_and_archived_v1_refused(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        live = pipeline.legacy_checkpoint_raw()
        live["windows"]["alpha"] = [9, 9, 9]
        with open(pipeline.ckpt_path, "w") as fh:
            json.dump(live, fh)
        run = upgrade.preview(pipeline)
        self.assertEqual(run["status"], "refused")
        self.assertIn("disagree", " ".join(run["reasons"]))

    def test_reserved_event_fields_rejected(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        with self.assertRaises(ValueError):
            pipeline.ingest({"id": "x", "account": "a", "amount": 1, "offset": 99})

    def test_second_upgrade_refused(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        upgrade.commit(pipeline, upgrade.preview(pipeline)["id"])
        run = upgrade.preview(pipeline)
        self.assertEqual(run["status"], "refused")
        self.assertIn("already the new format", " ".join(run["reasons"]))

    def test_digest_mismatch_blocks_commit(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        run = upgrade.preview(pipeline)
        with self.assertRaises(upgrade.UpgradeError):
            upgrade.commit(pipeline, run["id"], expected_digest="0" * 64)


class CrashTests(unittest.TestCase):
    def _run_crash(self, crash_at):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        preview = upgrade.preview(pipeline)
        run = upgrade.commit(pipeline, preview["id"], crash_at=crash_at)
        self.assertEqual(run["status"], "crashed")
        # Before the swap lands, verification must report failure rather than
        # pretend the upgrade is in place. After it lands (after_replace), the
        # contract really does hold and verification says so.
        expected_ok = crash_at == "after_replace"
        check = upgrade.verify(pipeline, run)
        self.assertEqual(check["ok"], expected_ok, check["reasons"])
        recovered = upgrade.recover(pipeline, run["id"])
        self.assertEqual(recovered["status"], "recovered")
        self.assertTrue(recovered["verified"]["ok"], recovered["verified"]["reasons"])
        worker.run_once(pipeline, batch=100)
        final = upgrade.verify(pipeline, recovered)
        self.assertTrue(final["ok"], final["reasons"])
        keys = [store.contribution_key(c) for c in pipeline.read_contributions()]
        self.assertEqual(len(keys), len(set(keys)))
        return recovered

    def test_crash_before_write(self):
        self._run_crash("before_write")

    def test_crash_after_write(self):
        self._run_crash("after_write")

    def test_crash_before_replace(self):
        self._run_crash("before_replace")

    def test_crash_after_replace(self):
        run = self._run_crash("after_replace")
        self.assertEqual(run["recovery"]["action"], "already_committed")

    def test_recover_is_idempotent(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        run = upgrade.commit(pipeline, upgrade.preview(pipeline)["id"], crash_at="before_replace")
        first = upgrade.recover(pipeline, run["id"])
        second = upgrade.recover(pipeline, run["id"])
        self.assertTrue(first["verified"]["ok"])
        self.assertEqual(second["recovery"]["action"], "already_committed")

    def test_recover_after_worker_progress_does_not_rewind(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        run = upgrade.commit(pipeline, upgrade.preview(pipeline)["id"], crash_at="after_replace")
        self.assertEqual(run["status"], "crashed")
        worker.run_once(pipeline, batch=100)
        advanced = pipeline.load_checkpoint()["offset"]
        recovered = upgrade.recover(pipeline, run["id"])
        self.assertEqual(recovered["recovery"]["action"], "superseded")
        self.assertEqual(pipeline.load_checkpoint()["offset"], advanced)
        self.assertTrue(recovered["verified"]["ok"], recovered["verified"]["reasons"])

    def test_worker_crash_between_results_and_checkpoint(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        upgrade.commit(pipeline, upgrade.preview(pipeline)["id"])
        crashed = worker.run_once(pipeline, batch=100, crash_at="before_replace")
        self.assertEqual(crashed["crashed"], "before_replace")
        first = pipeline.read_contributions()
        self.assertTrue(first)
        resumed = worker.run_once(pipeline, batch=100)
        second = pipeline.read_contributions()
        self.assertEqual(first, second, "replayed batch must dedupe, not append again")
        self.assertGreater(resumed["processed"], 0)

    def test_worker_refuses_truncated_log(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        upgrade.commit(pipeline, upgrade.preview(pipeline)["id"])
        worker.run_once(pipeline, batch=100)
        # Lose the tail of the input log.
        lines = _read_bytes(pipeline.input_path).splitlines(keepends=True)
        with open(pipeline.input_path, "wb") as fh:
            fh.write(b"".join(lines[:5]))
        with self.assertRaises(worker.LogTruncated):
            worker.run_once(pipeline, batch=100)

    def test_ingester_crash_writes_nothing(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        before = pipeline.read_events()
        with self.assertRaises(store.CrashInjected):
            pipeline.ingest({"id": "late", "account": "a", "amount": 1}, crash_at="before_write")
        self.assertEqual(pipeline.read_events(), before)

    def test_live_checkpoint_ahead_blocks_commit(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        preview = upgrade.preview(pipeline)
        upgrade.commit(pipeline, preview["id"])
        worker.run_once(pipeline, batch=100)
        # A stale preview can no longer be committed on top of newer progress.
        with self.assertRaises(upgrade.UpgradeError):
            upgrade.commit(pipeline, preview["id"])


class PublisherTests(unittest.TestCase):
    def test_no_double_count_on_redo(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        upgrade.commit(pipeline, upgrade.preview(pipeline)["id"])
        worker.run_once(pipeline, batch=100)
        pub = Publisher(pipeline)
        first = pub.publish()
        snapshot = pub.snapshot()
        # Simulate a crash after applying but before ack: ledger is not saved.
        pub.publish(crash_before_ack=True)
        second = pub.publish()
        self.assertEqual(second["new"], 0)
        self.assertEqual(pub.snapshot()["accounts"], snapshot["accounts"])
        self.assertGreaterEqual(first["new"], 1)

    def test_retraction_reduces_ledger(self):
        root, pipeline, info = seed()
        self.addCleanup(shutil.rmtree, root, True)
        upgrade.commit(pipeline, upgrade.preview(pipeline)["id"])
        worker.run_once(pipeline, batch=100)
        pub = Publisher(pipeline)
        result = pub.publish()
        self.assertEqual(result["retractions"], 1)
        self.assertGreater(result["new"], 0)
        alpha_flags = sum(
            1 for c in pipeline.read_contributions()
            if c["account"] == "alpha" and c["kind"] == "flag"
        )
        # ledger flag count for alpha = emitted flags minus the retracted one
        self.assertEqual(pub.snapshot()["accounts"]["alpha"]["flag"], alpha_flags - 1)


class WebTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipeline.web import make_server

        cls.root = tempfile.mkdtemp(prefix="pipeline-web-")
        cls.server = make_server(cls.root)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.server.server_address[:2]
        cls.base = f"http://{host}:{port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        shutil.rmtree(cls.root, ignore_errors=True)

    def call(self, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(
            self.base + path, data=data, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def setUp(self):
        status, _ = self.call("/api/reset", {"kind": "basic"})
        self.assertEqual(status, 200)

    def test_ui_served(self):
        with urllib.request.urlopen(self.base + "/") as response:
            html = response.read().decode()
        self.assertIn("upgrade console", html)
        self.assertIn("/app.js", html)

    def test_refused_preview_is_reported_not_faked(self):
        status, body = self.call("/api/upgrade/preview", {"mode": "replay", "replay_offset": 1})
        self.assertEqual(status, 200)
        # offset 1 is behind the v1 checkpoint -> explicit refusal
        self.assertEqual(body["status"], "refused")
        self.assertTrue(body["reasons"])
        status, err = self.call("/api/upgrade/commit", {"run_id": body["id"]})
        self.assertEqual(status, 400)

    def test_upgrade_then_crash_then_recover(self):
        status, preview = self.call("/api/upgrade/preview", {"mode": "auto"})
        self.assertEqual(preview["status"], "previewed")
        status, crashed = self.call("/api/upgrade/commit", {
            "run_id": preview["id"], "crash_at": "after_write",
            "expected_digest": preview["converted_digest"],
        })
        self.assertEqual(crashed["status"], "crashed")
        status, state = self.call("/api/state")
        self.assertFalse(state["worker_ready"]["ok"])
        status, recovered = self.call("/api/upgrade/recover", {"run_id": preview["id"]})
        self.assertEqual(recovered["status"], "recovered")
        self.assertTrue(recovered["verified"]["ok"])
        status, step = self.call("/api/worker/step", {"batch": 100})
        self.assertGreater(step["contributions"], 0)
        status, published = self.call("/api/publisher/step", {})
        self.assertFalse(published["crashed"])
        status, verify = self.call("/api/upgrade/verify", {"run_id": preview["id"]})
        self.assertTrue(verify["ok"], verify["reasons"])

    def test_ingest_while_upgrade_pending(self):
        status, preview = self.call("/api/upgrade/preview", {"mode": "auto"})
        status, event = self.call("/api/ingest", {"id": "live-1", "account": "delta", "amount": 999})
        self.assertEqual(event["offset"], 25)
        status, run = self.call("/api/upgrade/commit", {"run_id": preview["id"]})
        self.assertTrue(run["verified"]["ok"])
        status, step = self.call("/api/worker/step", {"batch": 100})
        self.assertEqual(step["to_offset"], 25)
        status, state = self.call("/api/state")
        self.assertGreaterEqual(state["metrics"]["events"], 25)

    def test_run_id_validation(self):
        status, err = self.call("/api/upgrade/commit", {"run_id": "../etc/passwd"})
        self.assertEqual(status, 400)


if __name__ == "__main__":
    unittest.main()
