import os, json, gzip, time, threading
from datetime import datetime, timezone

DATA_ROOT = os.environ.get("DATA_ROOT", "/var/data/polymarket")
ROTATE_MB = int(os.environ.get("ROTATE_MB", "512"))

class RotatingGzipWriter:
    def __init__(self, root):
        self.root = root
        self.lock = threading.Lock()
        self.fp = None
        self.bytes = 0
        self.cur_hour = None
        self.seq = 0
        os.makedirs(root, exist_ok=True)

    def _path(self):
        now = datetime.now(timezone.utc)
        y, m, d, h = now.strftime("%Y %m %d %H").split()
        dirname = os.path.join(self.root, f"year={y}", f"month={m}", f"day={d}", f"hour={h}")
        os.makedirs(dirname, exist_ok=True)
        fname = f"events-{self.seq:03d}.jsonl.gz"
        return os.path.join(dirname, fname), now.replace(minute=0, second=0, microsecond=0)

    def _open_new(self):
        if self.fp:
            try: self.fp.close()
            except: pass
        path, hour = self._path()
        self.fp = gzip.open(path, "ab", compresslevel=6)
        self.bytes = 0
        self.cur_hour = hour

    def write(self, obj):
        line = (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")
        with self.lock:
            now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            if (self.fp is None) or (now != self.cur_hour) or (self.bytes > ROTATE_MB * 1024 * 1024):
                # new hour or size rollover -> bump sequence if same hour
                if self.cur_hour == now and self.fp:
                    self.seq += 1
                else:
                    self.seq = 0
                self._open_new()
            self.fp.write(line)
            self.bytes += len(line)

writer = RotatingGzipWriter(DATA_ROOT)

def write_event(obj):
    writer.write(obj)