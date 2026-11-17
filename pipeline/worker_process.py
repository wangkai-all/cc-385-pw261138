"""Worker process: consume new offsets, publish contributions, checkpoint.

Exits non-zero with a clear message if the live checkpoint needs an upgrade.
"""
from __future__ import annotations

import argparse
import json
import sys

from . import worker
from .store import Pipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="rule worker loop")
    parser.add_argument("--data", default="./data")
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--batch", type=int, default=25)
    parser.add_argument("--once", action="store_true", help="run a single batch")
    parser.add_argument("--crash-at", default="none")
    args = parser.parse_args(argv)
    pipeline = Pipeline(args.data)
    if args.once:
        try:
            result = worker.run_once(pipeline, batch=args.batch, crash_at=args.crash_at)
        except worker.UpgradeRequired as exc:
            print(f"upgrade required: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(result, indent=2))
        return 0 if result["crashed"] is None else 3
    try:
        while True:
            result = worker.run_once(pipeline, batch=args.batch)
            if result["processed"]:
                print(json.dumps(result), flush=True)
            if not result["processed"]:
                import time

                time.sleep(args.interval)
    except worker.UpgradeRequired as exc:
        print(f"upgrade required: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("stopping worker")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
