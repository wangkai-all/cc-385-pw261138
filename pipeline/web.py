"""JSON API + static web UI.

Endpoints (all JSON unless noted):

  GET  /                    -> web UI (static/index.html)
  GET  /api/state           -> metrics, checkpoint, runs, ledger snapshot
  GET  /api/events?after=N  -> input events with offset > N
  POST /api/ingest          -> append one event (normal or {"reversal_of": id})
  POST /api/worker/step     -> {"batch": n, "crash_at": point}
  POST /api/worker/crash    -> simulated crash of a worker step
  POST /api/publisher/step  -> {"limit": n, "crash_before_ack": bool}
  POST /api/publisher/reset-ledger-crash -> re-deliver without ack
  GET  /api/runs            -> upgrade runs
  GET  /api/runs/<id>       -> one run
  POST /api/upgrade/preview -> {"mode": "auto"|"replay", "replay_offset": n,
                                "source_rule": "v1" (optional, replay only)}
  POST /api/upgrade/commit  -> {"run_id", "crash_at", "expected_digest"}
  POST /api/upgrade/recover -> {"run_id"}
  POST /api/upgrade/verify  -> {"run_id"}
  POST /api/reset           -> reseed the demo data directory
"""
from __future__ import annotations

import json
import os
import re
import shutil
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from . import scenario, store, upgrade, worker
from .checkpoint import IncompatibleCheckpoint, LogTruncated
from .publisher import Publisher

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
_RUN_ID_RE = re.compile(r"^run_[0-9a-f]{16}$")


class App:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.lock = threading.RLock()
        self.pipeline = store.Pipeline(self.root)
        self.publisher = Publisher(self.pipeline)

    LAYOUT = ("input", "checkpoints", "results", "runs", "legacy")

    def reset(self, *, kind: str = "basic", legacy_offset: int = 12,
              total: int | None = None) -> dict:
        """Clear only this pipeline's own layout, never unrelated files."""
        with self.lock:
            for name in self.LAYOUT:
                path = os.path.join(self.root, name)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                elif os.path.exists(path):
                    os.remove(path)
            self.pipeline = store.Pipeline(self.root)
            self.publisher = Publisher(self.pipeline)
            info = scenario.seed_pipeline(
                self.root, kind=kind, legacy_offset=legacy_offset, total=total
            )
        return info

    def state(self) -> dict:
        with self.lock:
            metrics = self.pipeline.metrics()
            return {
                "root": self.root,
                "metrics": metrics,
                "runs": self.pipeline.list_runs(),
                "publisher": self.publisher.snapshot(),
                "worker_ready": _worker_ready(self.pipeline),
                "options": {
                    "crash_points": list(store.CRASH_POINTS),
                },
            }


def _worker_ready(pipeline: store.Pipeline) -> dict:
    try:
        pipeline.load_checkpoint()
        return {"ok": True, "reasons": []}
    except IncompatibleCheckpoint as exc:
        return {
            "ok": False,
            "reasons": exc.reasons,
            "kind": "log_truncated" if isinstance(exc, LogTruncated) else "incompatible",
        }
    except FileNotFoundError:
        return {"ok": True, "reasons": []}


class Handler(BaseHTTPRequestHandler):
    app: App
    server_version = "RulePipeline/1.0"

    # -------------------------------------------------------------- plumbing
    def log_message(self, fmt: str, *args: Any) -> None:  # quieter tests
        if os.environ.get("PIPELINE_HTTP_LOG"):
            super().log_message(fmt, *args)

    def _send(self, status: int, body: bytes, content_type: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, value: Any) -> None:
        payload = json.dumps(value, indent=2, sort_keys=True, default=str).encode()
        self._send(status, payload)

    def _error(self, status: int, exc: Exception, **extra: Any) -> None:
        body = {"error": str(exc), "type": type(exc).__name__}
        body.update(extra)
        self._json(status, body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"request body is not JSON: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError("request body must be a JSON object")
        return value

    # ----------------------------------------------------------------- verbs
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            with self.app.lock:
                if path in ("/", "/index.html"):
                    return self._static("index.html")
                if path == "/app.js":
                    return self._static("app.js")
                if path == "/app.css":
                    return self._static("app.css")
                if path == "/api/state":
                    return self._json(200, self.app.state())
                if path == "/api/events":
                    after = int(parse_qs(parsed.query).get("after", ["0"])[0] or 0)
                    events = self.app.pipeline.read_events()
                    return self._json(200, {"events": events[after:], "total": len(events)})
                if path == "/api/runs":
                    return self._json(200, {"runs": self.app.pipeline.list_runs()})
                match = re.fullmatch(r"/api/runs/([^/]+)", path)
                if match:
                    run = self.app.pipeline.load_run(match.group(1))
                    if run is None:
                        return self._json(404, {"error": "unknown run"})
                    return self._json(200, run)
                if path == "/api/health":
                    return self._json(200, {"ok": True})
                return self._json(404, {"error": "not found"})
        except FileNotFoundError as exc:
            return self._error(404, exc)
        except (ValueError, IncompatibleCheckpoint) as exc:
            return self._error(400, exc)
        except Exception as exc:  # pragma: no cover - surfaced to the UI
            return self._error(500, exc)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            body = self._body()
            with self.app.lock:
                if path == "/api/ingest":
                    return self._json(200, self.app.pipeline.ingest(body))
                if path == "/api/worker/step":
                    return self._json(200, worker.run_once(
                        self.app.pipeline,
                        batch=int(body.get("batch", 25)),
                        crash_at=str(body.get("crash_at", "none")),
                    ))
                if path == "/api/publisher/step":
                    limit = body.get("limit")
                    return self._json(200, self.app.publisher.publish(
                        limit=None if limit is None else int(limit),
                        crash_before_ack=bool(body.get("crash_before_ack", False)),
                    ))
                if path == "/api/upgrade/preview":
                    mode = str(body.get("mode", "auto"))
                    replay_offset = body.get("replay_offset")
                    run = upgrade.preview(
                        self.app.pipeline,
                        mode=mode,
                        replay_offset=None if replay_offset is None else int(replay_offset),
                        source_rule_override=body.get("source_rule"),
                    )
                    return self._json(200, run)
                if path == "/api/upgrade/commit":
                    run_id = _run_id(body)
                    run = upgrade.commit(
                        self.app.pipeline,
                        run_id,
                        crash_at=str(body.get("crash_at", "none")),
                        expected_digest=body.get("expected_digest"),
                    )
                    return self._json(200, run)
                if path == "/api/upgrade/recover":
                    return self._json(200, upgrade.recover(self.app.pipeline, _run_id(body)))
                if path == "/api/upgrade/verify":
                    run = self.app.pipeline.load_run(_run_id(body))
                    if run is None or not run.get("converted_checkpoint"):
                        return self._json(404, {"error": "unknown or refused run"})
                    return self._json(200, upgrade.verify(self.app.pipeline, run))
                if path == "/api/reset":
                    info = self.app.reset(
                        kind=str(body.get("kind", "basic")),
                        legacy_offset=int(body.get("legacy_offset", 12)),
                        total=None if body.get("total") is None else int(body["total"]),
                    )
                    return self._json(200, {"reset": True, "info": info})
                return self._json(404, {"error": "not found"})
        except (worker.UpgradeRequired, worker.LogTruncated) as exc:
            # Not a server fault: the worker is refusing an incompatible
            # checkpoint, which is the designed behaviour.
            return self._error(409, exc, reasons=exc.reasons)
        except FileNotFoundError as exc:
            return self._error(404, exc)
        except (ValueError, IncompatibleCheckpoint) as exc:
            return self._error(400, exc)
        except Exception as exc:  # pragma: no cover
            return self._error(500, exc)

    def _static(self, name: str) -> None:
        path = os.path.join(STATIC_DIR, name)
        if not os.path.isfile(path):
            return self._json(404, {"error": f"missing static file {name}"})
        with open(path, "rb") as fh:
            body = fh.read()
        types = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
        }
        return self._send(200, body, types.get(os.path.splitext(name)[1], "text/plain"))


def _run_id(body: dict) -> str:
    run_id = body.get("run_id")
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        raise ValueError("run_id must look like run_<16 hex chars>")
    return run_id


def make_server(root: str, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
    app = App(root)
    handler = type("BoundHandler", (Handler,), {"app": app})
    server = ThreadingHTTPServer((host, port), handler)
    server.app = app  # type: ignore[attr-defined]
    return server


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="continuous-event rule pipeline demo")
    parser.add_argument("--data", default="./data", help="data directory")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--seed", action="store_true", help="(re)seed the data directory")
    parser.add_argument("--kind", default="basic", choices=["basic", "mixed", "quiet"])
    parser.add_argument("--legacy-offset", type=int, default=12)
    args = parser.parse_args(argv)

    app_pipeline = store.Pipeline(args.data)
    if args.seed or not os.path.exists(app_pipeline.input_path):
        App(args.data).reset(kind=args.kind, legacy_offset=args.legacy_offset)
        print(f"seeded {args.data}")
    server = make_server(args.data, args.host, args.port)
    host, port = server.server_address[:2]
    print(f"listening on http://{host}:{port}/  (data: {os.path.abspath(args.data)})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
