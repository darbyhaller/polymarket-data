# writer.py
import os
import json
import time
import gzip
import threading
import subprocess
from datetime import datetime, timezone
from typing import Optional, IO, Union

# Defaults (safe for user dirs; no /var/data side effects)
DEFAULT_ROOT = os.environ.get("DATA_ROOT", os.path.expanduser("~/polymarket-data"))
ROTATE_MB = int(os.environ.get("ROTATE_MB", "512"))

# Optional pigz support (parallel gzip). Set USE_PIGZ=1 to enable.
USE_PIGZ = os.environ.get("USE_PIGZ", "0") in ("1", "true", "TRUE", "yes", "YES")
PIGZ_BIN = os.environ.get("PIGZ_BIN", "pigz")  # override path if needed
PIGZ_LEVEL = int(os.environ.get("PIGZ_LEVEL", "6"))
PIGZ_PROCS = int(os.environ.get("PIGZ_PROCS", "0"))  # 0 = auto (pigz picks)

class RotatingGzipWriter:
    """
    Hour-partitioned, size-rotating gzip writer.
    - Safe to import (no side effects).
    - Thread-safe write().
    - Optional pigz-backed compression for multi-core speed.
    """
    def __init__(self,
                 root: str = DEFAULT_ROOT,
                 rotate_mb: int = ROTATE_MB,
                 use_pigz: bool = USE_PIGZ,
                 pigz_bin: str = PIGZ_BIN,
                 pigz_level: int = PIGZ_LEVEL,
                 pigz_procs: int = PIGZ_PROCS):
        self.root = root
        self.rotate_bytes = rotate_mb * 1024 * 1024
        self.use_pigz = use_pigz
        self.pigz_bin = pigz_bin
        self.pigz_level = pigz_level
        self.pigz_procs = pigz_procs

        self.lock = threading.Lock()
        self.fp: Optional[Union[gzip.GzipFile, IO[bytes]]] = None  # gzip file or pigz stdin
        self.p: Optional[subprocess.Popen] = None  # pigz process when used
        self.bytes = 0
        self.cur_hour: Optional[datetime] = None
        self.seq = 0

        os.makedirs(root, exist_ok=True)

    # ---------- internals ----------
    def _dir_and_filename(self):
        now = datetime.now(timezone.utc)
        y, m, d, h = now.strftime("%Y %m %d %H").split()
        dirname = os.path.join(self.root, f"year={y}", f"month={m}", f"day={d}", f"hour={h}")
        os.makedirs(dirname, exist_ok=True)
        fname = f"events-{self.seq:03d}.jsonl.gz"
        return os.path.join(dirname, fname), now.replace(minute=0, second=0, microsecond=0)

    def _open_gzip(self, path: str):
        # Standard (single-threaded) gzip
        return gzip.open(path, "ab", compresslevel=6)

    def _open_pigz(self, path: str):
        # Start pigz as a child process, piping our NDJSON into it
        # pigz -p <procs> -<level> -c > path
        args = [self.pigz_bin, f"-{self.pigz_level}", "-c"]
        if self.pigz_procs and self.pigz_procs > 0:
            args.extend(["-p", str(self.pigz_procs)])
        # We direct stdout to the target file; we write to stdin
        out_fh = open(path, "ab")
        p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=out_fh)
        # We'll keep `out_fh` managed by the process; when we close p.stdin and wait, pigz closes stdout
        return p, p.stdin  # process, writeable fp

    def _close_current(self):
        # Close current file/process if open
        try:
            if self.fp:
                # If pigz: fp is p.stdin
                if self.p is not None:
                    try:
                        self.fp.flush()
                    except Exception:
                        pass
                    try:
                        self.fp.close()
                    except Exception:
                        pass
                    # Wait for pigz to finish writing and close its stdout
                    self.p.wait()
                else:
                    # gzip
                    try:
                        self.fp.flush()
                    except Exception:
                        pass
                    try:
                        self.fp.close()
                    except Exception:
                        pass
        finally:
            self.fp = None
            self.p = None

    def _open_new(self):
        self._close_current()
        path, hour = self._dir_and_filename()
        if self.use_pigz:
            self.p, self.fp = self._open_pigz(path)
        else:
            self.fp = self._open_gzip(path)
        self.bytes = 0
        self.cur_hour = hour

    # ---------- public API ----------
    def write(self, obj):
        """
        Thread-safe write of one JSON object per line (UTF-8).
        Rotates on hour boundary or when size exceeds rotate_mb.
        """
        line = (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")
        with self.lock:
            now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            need_rotate = (
                self.fp is None or
                now_hour != self.cur_hour or
                self.bytes > self.rotate_bytes
            )
            if need_rotate:
                # If same hour and we had a file open, bump seq; else reset for new hour.
                if self.cur_hour == now_hour and self.fp:
                    self.seq += 1
                else:
                    self.seq = 0
                self._open_new()

            assert self.fp is not None
            self.fp.write(line)  # both gzip.GzipFile and pigz stdin are binary file-like
            self.bytes += len(line)

    def flush(self):
        with self.lock:
            if self.fp and self.p is None:
                # gzip path supports flush; pigz stdin is buffered minimally, flush is NOP
                try:
                    self.fp.flush()
                except Exception:
                    pass

    def close(self):
        with self.lock:
            self._close_current()

    # Context-manager sugar
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

# Convenience factory (optional; no side effects on import)
def make_writer(root: Optional[str] = None,
                rotate_mb: Optional[int] = None,
                use_pigz: Optional[bool] = None,
                pigz_bin: Optional[str] = None,
                pigz_level: Optional[int] = None,
                pigz_procs: Optional[int] = None) -> RotatingGzipWriter:
    return RotatingGzipWriter(
        root=root or DEFAULT_ROOT,
        rotate_mb=rotate_mb if rotate_mb is not None else ROTATE_MB,
        use_pigz=USE_PIGZ if use_pigz is None else use_pigz,
        pigz_bin=pigz_bin or PIGZ_BIN,
        pigz_level=pigz_level if pigz_level is not None else PIGZ_LEVEL,
        pigz_procs=pigz_procs if pigz_procs is not None else PIGZ_PROCS,
    )

if __name__ == "__main__":
    # Optional smoke test (writes a couple lines wherever DEFAULT_ROOT points)
    w = make_writer()
    for i in range(2):
        w.write({"ts": int(time.time()*1000), "message": f"smoke{i}"})
    w.close()
    print(f"Wrote 2 lines under: {DEFAULT_ROOT}")
