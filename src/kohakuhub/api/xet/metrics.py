"""Xet performance metrics tracker."""

import time
from typing import Optional
from kohakuhub.logger import get_logger

logger = get_logger("XET_METRICS")

class XetMetrics:
    def __init__(self):
        self.dedup_hits = 0
        self.dedup_misses = 0
        self.total_bytes_saved = 0
        self.total_bytes_uploaded = 0
        
    def record_dedup(self, hit: bool, size: int):
        if hit:
            self.dedup_hits += 1
            self.total_bytes_saved += size
        else:
            self.dedup_misses += 1
            self.total_bytes_uploaded += size
            
    def get_dedup_ratio(self) -> float:
        total = self.dedup_hits + self.dedup_misses
        if total == 0:
            return 0.0
        return self.dedup_hits / total

    def log_stats(self):
        ratio = self.get_dedup_ratio() * 100
        saved_mb = self.total_bytes_saved / (1024 * 1024)
        logger.info(f"XET Stats: Dedup Ratio: {ratio:.2f}%, Saved: {saved_mb:.2f} MB")

# Singleton metrics instance
metrics = XetMetrics()
