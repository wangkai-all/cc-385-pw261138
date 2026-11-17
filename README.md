# Continuously-running event rules: v2 state, upgrade with crash-safe conversion

Three processes share one directory and nothing else:

| process | module | writes | reads |
| --- | --- | --- | --- |
| ingester | `pipeline.ingester` | `input/events.jsonl` | — |
| rule worker | `pipeline.worker_process` | `results/contributions.jsonl`, `checkpoints/rule.json` | input log |
| publisher | `pipeline.publisher_process` | `results/ledger.json`, `results/acks.jsonl` | contributions |

The interesting part is that v2's rule state is not v1's. v1 kept only the last
three amounts per account. v2 adds a latch, a cooldown, cumulative totals and an
event ledger for reversals, so a v1 checkpoint **cannot** be loaded as v2 — the
worker refuses to start on it. The upgrade path converts it, or rebuilds it from
the log, and is previewable, crashable and verifiable from the web UI.

## Run it

```bash
python3 -m pipeline.web --data ./data --seed --port 8765
#   http://127.0.0.1:8765/
```

No dependencies beyond Python 3.11. Or run the processes separately:

```bash
python3 -m pipeline.web --data ./data --seed --port 8765     # one-time seed
python3 -m pipeline.ingester --data ./data --interval 0.4    # keeps adding events
python3 -m pipeline.worker_process --data ./data             # refuses until upgrade
python3 -m pipeline.publisher_process --data ./data --once
```

Tests: `python3 -m unittest discover -s tests` (44 tests, no external services).

## What "cannot read the old checkpoint" means here

* `pipeline.checkpoint.validate` accepts only `checkpoint/v2` with a matching
  rule version, an integer offset, `emit_from`, and a state whose `totals.events`
  equals the offset. Anything else raises `IncompatibleCheckpoint` with reasons.
* `worker.run_once` turns that into `UpgradeRequired` and does no work.
* The API/UI expose the refusal; nothing is auto-migrated and nothing pretends
  a conversion succeeded.

## The (rule, offset) contract

A checkpoint says: *"rule R has processed exactly offsets 1..O, and the first
offset it may still contribute for is E (`emit_from`)."* Normally `E = O + 1`.
`E > O + 1` is used when the old rule already published for offsets beyond its
checkpoint (it crashed between publishing results and writing its checkpoint) —
those offsets then advance the new state but are never contributed again.

Verification (`upgrade.verify`) recomputes the log prefix independently and
checks that it reproduces the previewed v2 state, that the live checkpoint is a
consistent continuation of it, that contribution keys are unique, and that no
v2 contribution sits at or below an offset the old rule settled.

## Conversion vs replay

* **convert** (`mode=auto`): reads the v1 checkpoint and *proves* it against the
  log — each retained window must equal the window a clean v2 replay produces,
  and no account may be missing. If exact, v2 state (latch, counters, ledger) is
  reconstructed by replaying the same prefix, so the result is identical to what
  v2 would have held at that offset. If not exact, the preview is `refused` with
  reasons and cannot be committed.
* **replay** (`mode=replay`, `replay_offset`): rebuilds from the log up to an
  offset at or beyond the old checkpoint, for when the checkpoint itself is
  untrusted or lost. If the live v1 checkpoint is gone, pass `source_rule: "v1"`
  so the rule/offset mapping is still explicit. It must not go *behind* the old
  checkpoint (that would re-settle offsets), and the results log must not
  already tag contributions inside the replayed prefix with an unexpected rule.
  Offsets between the old checkpoint and the replay point that the old rule
  never published are listed in `report.uncontributed_offsets` (and shown as a
  warning in the UI) instead of being handled silently.

When both the live old checkpoint and the archived v1 copy exist, conversion
refuses if they disagree — it never picks one silently.

## Reversals and cumulative logic

Events may be `{"id", "account", "amount"}` or `{"id", "reversal_of": id}`. A
reversal is applied through the state's event ledger: it reduces the account
and global cumulative amount, un-latches a flag the reversed event caused (when
that flag is still the latest), decrements counters, and emits a `retract`
contribution naming its target. Reversing an unknown, future, or already
reversed event is an error, so a replay can never retract twice. The publisher
applies retractions to the cumulative ledger by target, which makes re-delivery
idempotent.

## Crash points you can inject

`before_write` · `after_write` · `before_replace` · `after_replace` · `none`,
both for the upgrade commit and for a worker batch. `recover` is idempotent and
reports what it found: `reapplied`, `already_committed`, or an error when the
live checkpoint is *newer* than the preview. A worker crash between "append
contributions" and "checkpoint swap" replays the batch; contribution keys
`(rule, offset, index)` make the replay a no-op, so nothing is counted twice.

## Layout

```
pipeline/engine.py       v2 rule + reversals + deterministic replay
pipeline/checkpoint.py   v2 format/validation + proven v1 conversion
pipeline/upgrade.py      preview / commit / recover / verify
pipeline/worker.py       batch loop, watermark-aware, crash-injectable
pipeline/publisher.py    exactly-once accounting over at-least-once input
pipeline/store.py        durable files, tmp+fsync+rename swap, dedupe
pipeline/web.py          JSON API + static UI
reference/v1/engine.py   frozen v1 engine used to produce genuine v1 state
tests/                   40 tests incl. every crash point and the web API
```
