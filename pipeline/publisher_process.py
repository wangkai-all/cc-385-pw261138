"""Publisher process: apply contributions to the cumulative ledger, exactly once."""
from __future__ import annotations

import argparse
import json
import time

from .publisher import Publisher
from .store import Pipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="results publisher loop")
    parser.add_argument("--data", default="./data")
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    publisher = Publisher(Pipeline(args.data))
    try:
        while True:
            result = publisher.publish(limit=args.limit)
            print(json.dumps(result), flush=True)
            if args.once:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("stopping publisher")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
