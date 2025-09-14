#!/usr/bin/env python3
"""
High-performance parquet writer for Polymarket event data.
Partitioned by event_type with optimized schemas for each event type.
"""

from datetime import timedelta
import os
import threading
from datetime import datetime, timezone
from typing import Dict, List, Any
from collections import defaultdict, deque
import time
import pyarrow as pa
import pyarrow.parquet as pq

class EventTypeParquetWriter:
    def __init__(self):
        self.root = os.path.join(os.getenv('PARQUET_ROOT', '/var/data/polymarket'), 'parquets')
        self.compression = os.getenv("PM_COMPRESSION", "zstd")
        self.batch_size = int(os.getenv("PM_BATCH_SIZE", "100_000"))
        self.rows_per_file = int(os.getenv("PM_ROWS_PER_FILE", "1_000_000"))
        
        self.lock = threading.Lock()
        
        # Buffers for each event type - using deque for efficient append/popleft
        self.buffers: Dict[str, deque] = defaultdict(deque)
        self.buffer_sizes: Dict[str, int] = defaultdict(int)
        
        # Track current files and open writers
        self.current_files: Dict[str, str] = {}
        self.current_hour: Dict[str, datetime] = {}
        self.file_sequences: Dict[str, int] = defaultdict(int)
        self.writers: Dict[str, pq.ParquetWriter] = {}
        self.rows_written: Dict[str, int] = defaultdict(int)
        
        os.makedirs(self.root, exist_ok=True)
        
        base_fields = [
            pa.field("recv_ts_ms", pa.int64()),
            pa.field("timestamp", pa.int64()),
            pa.field("asset_hash", pa.int64()),
            pa.field("price", pa.float32()),
            pa.field("size", pa.float32()),
        ]

        self.schemas = {}
        for side in "BUY", "SELL":
            self.schemas["order_update_"+side] = pa.schema(base_fields)
            self.schemas["last_trade_price_"+side] = pa.schema(base_fields + [pa.field("fee_rate_bps", pa.float32())])

    
    def _open_writer(self, file_key: str, schema: pa.Schema, path: str):
        """Open a new ParquetWriter for the given file key with .inprogress pattern."""
        self._close_writer(file_key)  # just in case
        
        # Use .inprogress pattern for atomic writes
        tmp_path = path + ".inprogress"
        self.writers[file_key] = pq.ParquetWriter(
            tmp_path,
            schema=schema,
            compression=self.compression,
            use_dictionary=True,
            write_statistics=True,
        )
        self.rows_written[file_key] = 0
        # Store final path separately for atomic rename on close
        self.current_files[file_key] = path

    def _close_writer(self, file_key: str):
        """Close the ParquetWriter and atomically rename from .inprogress to final path."""
        w = self.writers.pop(file_key, None)
        if w is not None:
            try:
                w.close()
            except Exception as e:
                print(e)
            
            # Atomic rename from .inprogress to final path
            final_path = self.current_files.get(file_key)
            if final_path:
                tmp_path = final_path + ".inprogress"
                try:
                    os.replace(tmp_path, final_path)
                except Exception as e:
                    print(f"Atomic rename failed for {tmp_path} -> {final_path}: {e}")

    def close_completed_hours(self, grace_minutes=15):
        """Close writers that are older than current hour minus grace period.
        
        This prevents late events from causing file collisions by keeping writers
        open for a grace period after their hour boundary.
        """
        
        now = datetime.fromtimestamp(time.time(), timezone.utc)
        # Close writers for hours that ended more than grace_minutes ago
        cutoff_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(minutes=grace_minutes)
        
        to_close = []
        for key, hour_dt in list(self.current_hour.items()):
            if hour_dt < cutoff_time:
                to_close.append(key)
        
        if to_close:
            print(f"Closing {len(to_close)} writers older than {cutoff_time} (grace period: {grace_minutes}min)")
        
        for key in to_close:
            self._close_writer(key)
            self.current_hour.pop(key, None)
            self.current_files.pop(key, None)
            self.rows_written.pop(key, None)
    
    def _partition_path(self, event_type: str, dt: datetime) -> str:
        """Generate partition path for event_type and datetime."""
        y, m, d, h = dt.strftime("%Y %m %d %H").split()
        return os.path.join(
            self.root,
            f"event_type={event_type}",
            f"year={y}",
            f"month={m}",
            f"day={d}",
            f"hour={h}"
        )
    
    def _file_path(self, event_type: str, dt: datetime) -> str:
        """Generate full file path including sequence number."""
        partition_dir = self._partition_path(event_type, dt)
        os.makedirs(partition_dir, exist_ok=True)
        
        seq = self.file_sequences[f"{event_type}_{dt}"]
        filename = f"events-{seq:03d}.parquet"
        return os.path.join(partition_dir, filename)

    
    def write(self, event_type: str, event: Dict[str, Any]):
        with self.lock:
            self.buffers[event_type].append(event)
            should_flush = len(self.buffers[event_type]) >= self.batch_size
            if should_flush:
                self._flush_event_type(event_type)
    
    def _flush_event_type(self, event_type: str):
        """Flush buffered events for a specific event type. Must hold lock."""
        if not self.buffers[event_type]:
            return
        
        # Convert buffer to list and clear
        events = list(self.buffers[event_type])
        self.buffers[event_type].clear()
        self.buffer_sizes[event_type] = 0
        
        # Group events by hour for proper partitioning
        hourly_groups = defaultdict(list)
        for event in events:
            # Use recv_ts_ms for partitioning
            recv_ts_ms = event["recv_ts_ms"]
            dt = datetime.fromtimestamp(recv_ts_ms / 1000, timezone.utc)
            hour_key = dt.replace(minute=0, second=0, microsecond=0)
            hourly_groups[hour_key].append(event)
        
        # Write each hourly group
        for hour_dt, hour_events in hourly_groups.items():
            self._write_batch(event_type, hour_dt, hour_events)
    
    def _write_batch(self, event_type: str, hour_dt: datetime, events: List[Dict[str, Any]]):
        """Write a batch of events to parquet. Must hold lock."""
        if not events:
            return
        
        try:
            file_key = f"{event_type}_{hour_dt}"
            if event_type == "other":
                return

            schema = self.schemas[event_type]

            rotate = False
            # rotate by rows or when hour changes (hour change is handled by file_key)
            if file_key in self.writers:
                if self.rows_written[file_key] >= self.rows_per_file:
                    rotate = True

            if rotate:
                self.file_sequences[file_key] += 1
                self._close_writer(file_key)

            # Ensure a writer exists for this file_key
            if file_key not in self.writers:
                path = self._file_path(event_type, hour_dt)
                self.current_hour[file_key] = hour_dt
                self._open_writer(file_key, schema, path)

            # Write this batch as a new row group with optimal row group size
            events.sort(key=lambda e: (e["asset_hash"], e["timestamp"]))
            table = pa.Table.from_pylist(events, schema=schema)
            self.writers[file_key].write_table(table)
            self.rows_written[file_key] += table.num_rows
            
        except Exception as e:
            # Log error but don't crash the writer
            print(f"Error writing parquet batch for {event_type}: {e}")
    
    def flush(self):
        """Flush all buffered events."""
        with self.lock:
            for event_type in list(self.buffers.keys()):
                self._flush_event_type(event_type)
            self.close_completed_hours()
    
    def close(self):
        """Flush all buffers and close all open Parquet writers."""
        self.flush()
        for key in list(self.writers.keys()):
            self._close_writer(key)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
