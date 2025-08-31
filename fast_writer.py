import os
import io
import json
import gzip
import threading
import subprocess
from datetime import datetime, timezone

try:
    import orjson as _orjson
except Exception:
    _orjson = None


class RotatingGzipWriter:
    """
    Rotates by hour and/or size. Writes newline-delimited JSON.
    - Configurable gzip compresslevel (1 = fastest, 9 = smallest)
    - Optional pigz for multi-core compression
    - Uses orjson if available for faster dumps
    """
    def __init__(
        self,
        root: str,
        rotate_mb: int = 512,
        compresslevel: int = 1,
        use_pigz: bool = False,
        pigz_level: int = 1,
        pigz_procs: int | None = None,
        deterministic_mtime: bool = False,
    ):
        self.root = root
        self.rotate_bytes = int(rotate_mb) * 1024 * 1024
        self.compresslevel = int(compresslevel)
        self.use_pigz = bool(use_pigz)
        self.pigz_level = int(pigz_level)
        self.pigz_procs = pigz_procs or os.cpu_count() or 4
        self.deterministic_mtime = bool(deterministic_mtime)

        self.lock = threading.Lock()
        self.fp = None          # file-like we write() to (gzip or pigz stdin)
        self.out_fh = None      # only used when use_pigz=True (the real file on disk)
        self.pigz_proc = None   # subprocess.Popen when use_pigz=True
        self.bytes = 0
        self.cur_hour = None
        self.seq = 0

        os.makedirs(root, exist_ok=True)

    def _path(self):
        now = datetime.now(timezone.utc)
        y, m, d, h = now.strftime("%Y %m %d %H").split()
        dirname = os.path.join(
            self.root, f"year={y}", f"month={m}", f"day={d}", f"hour={h}"
        )
        os.makedirs(dirname, exist_ok=True)
        fname = f"events-{self.seq:03d}.jsonl.gz"
        return os.path.join(dirname, fname), now.replace(minute=0, second=0, microsecond=0)

    def _close_current(self):
        # Close gzip file or pigz pipeline
        try:
            if self.fp:
                try:
                    self.fp.flush()
                except Exception:
                    pass
            if self.use_pigz and self.pigz_proc:
                try:
                    if self.fp:
                        self.fp.close()  # close pigz stdin
                finally:
                    self.fp = None
                try:
                    self.pigz_proc.wait(timeout=60)
                except Exception:
                    # best-effort terminate if hung
                    self.pigz_proc.terminate()
                    try:
                        self.pigz_proc.wait(timeout=5)
                    except Exception:
                        self.pigz_proc.kill()
                self.pigz_proc = None
                if self.out_fh:
                    try:
                        self.out_fh.flush()
                        os.fsync(self.out_fh.fileno())
                    except Exception:
                        pass
                    try:
                        self.out_fh.close()
                    except Exception:
                        pass
                self.out_fh = None
            else:
                if self.fp:
                    try:
                        # gzip.GzipFile
                        self.fp.close()
                    except Exception:
                        pass
                self.fp = None
        finally:
            self.bytes = 0

    def _open_new(self):
        self._close_current()
        path, hour = self._path()
        self.cur_hour = hour

        if self.use_pigz:
            # Open final file and spawn pigz to write into it
            self.out_fh = open(path, "wb")
            args = ["pigz", f"-{self.pigz_level}", "-p", str(self.pigz_procs), "-c"]
            # Note: we can't set mtime with pigz; it uses current time.
            self.pigz_proc = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=self.out_fh,
                stderr=subprocess.DEVNULL,
                bufsize=1024 * 1024,
            )
            # We write to pigz's stdin
            self.fp = self.pigz_proc.stdin
        else:
            # Pure Python gzip (single-threaded)
            # Use mtime=0 if deterministic output is desired
            mtime = 0 if self.deterministic_mtime else None
            # Text vs binary: binary is faster; we encode ourselves.
            self.fp = gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=open(path, "wb"),
                compresslevel=self.compresslevel,
                mtime=mtime,
            )

        self.bytes = 0

    @staticmethod
    def _dump_json_line(obj) -> bytes:
        if _orjson is not None:
            return _orjson.dumps(obj) + b"\n"
        # stdlib json with compact separators
        return (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")

    def write(self, obj):
        line = self._dump_json_line(obj)
        with self.lock:
            now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            if (self.fp is None) or (now_hour != self.cur_hour) or (self.bytes > self.rotate_bytes):
                # same hour & rollover -> bump sequence; otherwise reset to 0
                if self.cur_hour == now_hour and self.fp is not None:
                    self.seq += 1
                else:
                    self.seq = 0
                self._open_new()
            # write bytes
            self.fp.write(line)
            self.bytes += len(line)

    def close(self):
        with self.lock:
            self._close_current()


# ---- usage examples ----
# Downstream (fast): level 1 gzip, or pigz level 1
# writer = RotatingGzipWriter(DATA_ROOT, rotate_mb=512, compresslevel=1, use_pigz=False)

# Upstream (high compression): level 9 (or pigz -9 on all cores)
# writer = RotatingGzipWriter(DATA_ROOT, rotate_mb=512, compresslevel=9, use_pigz=False)
# writer = RotatingGzipWriter(DATA_ROOT, rotate_mb=512, use_pigz=True, pigz_level=9, pigz_procs=os.cpu_count())

def write_event(obj):
    global writer  # ensure writer is created somewhere in your module
    writer.write(obj)
