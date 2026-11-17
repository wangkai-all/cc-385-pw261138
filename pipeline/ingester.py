"""Ingester process: append events to the input log, forever or once.

    python -m pipeline.ingester --data ./data --account alpha --amount 450
    python -m pipeline.ingester --data ./data --reversal-of ev-0017
    python -m pipeline.ingester --data ./data --interval 0.5   # synthetic stream

IDs are unique per process start plus a counter. Crashes leave the log either
with or without the line, never half a line (single fsynced write).
"""
from __future__ import annotations

import argparse
import random
import sys
import time

from .store import Pipeline
from .util import new_id


def gen_event(account_pool: list[str], i: int) -> dict:
    return {
        "id": f"live-{int(time.time())}-{i}",
        "account": random.choice(account_pool),
        "amount": random.choice([40, 50, 60, 90, 120, 260, 300, 480]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ingest events into the pipeline")
    parser.add_argument("--data", default="./data")
    parser.add_argument("--account")
    parser.add_argument("--amount", type=int)
    parser.add_argument("--reversal-of")
    parser.add_argument("--id")
    parser.add_argument("--interval", type=float, default=0.0,
                        help="if set, keep emitting synthetic events")
    parser.add_argument("--accounts", default="alpha,beta,gamma,delta")
    parser.add_argument("--limit", type=int, default=0, help="0 = unlimited")
    args = parser.parse_args(argv)

    pipeline = Pipeline(args.data)
    single = args.account or args.reversal_of
    if single:
        event = {"id": args.id or new_id("ev")}
        if args.reversal_of:
            event["reversal_of"] = args.reversal_of
        else:
            if args.amount is None:
                parser.error("--amount is required with --account")
            event["account"] = args.account
            event["amount"] = args.amount
        stored = pipeline.ingest(event)
        print(f"ingested {stored['id']} at offset {stored['offset']}")
        return 0

    if args.interval <= 0:
        parser.error("give --account/--reversal-of for a single event, or --interval for a stream")

    pool = [a for a in args.accounts.split(",") if a]
    i = 0
    last_known = {e["id"] for e in pipeline.read_events()}
    try:
        while True:
            i += 1
            event = gen_event(pool, i)
            try:
                stored = pipeline.ingest(event)
                last_known.add(stored["id"])
                print(f"{stored['offset']}: {stored['id']} {stored['account']} {stored['amount']}", flush=True)
            except ValueError as exc:
                print(f"ingest rejected: {exc}", file=sys.stderr)
            if args.limit and i >= args.limit:
                break
            if i % 15 == 0 and last_known:
                target = sorted(last_known)[-1]
                reversal = {"id": f"rev-{int(time.time())}-{i}", "reversal_of": target}
                try:
                    stored = pipeline.ingest(reversal)
                    print(f"{stored['offset']}: reversal of {target}", flush=True)
                except ValueError as exc:
                    print(f"reversal rejected: {exc}", file=sys.stderr)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("stopping ingester")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
