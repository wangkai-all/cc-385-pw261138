"""Small shared helpers: canonical JSON, time, atomic writes, ids."""
from __future__ import annotations

import contextlib
import datetime as _dt
import hashlib
import json
import os
import threading
import time
from typing import Any, Iterator


def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso(ms: int | None = None) -> str:
    value = now_ms() if ms is None else ms
    return _dt.datetime.fromtimestamp(value / 1000, tz=_dt.timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def parse_iso(value: str) -> int:
    return int(_dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def digest(value: Any) -> str:
    return sha256_hex(canonical_json(value))


def new_id(prefix: str) -> str:
    return f"{prefix}_{os.urandom(8).hex()}"


def atomic_write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(os.path.dirname(os.path.abspath(path)) or ".")


def atomic_write_json(path: str, value: Any) -> None:
    atomic_write_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


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


@contextlib.contextmanager
def file_lock(path: str, timeout: float = 10.0) -> Iterator[None]:
    """Exclusive advisory lock; released automatically on process death."""
    import fcntl

    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    fh = open(path, "a+")
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"could not lock {path} within {timeout}s")
                time.sleep(0.02)
        yield
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        fh.close()
