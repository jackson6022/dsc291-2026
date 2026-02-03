"""
Partition Optimization for NYC Taxi Parquet Processing.

Provides utilities to parse size strings and find optimal partition (batch) sizes
for PyArrow Parquet reading, balancing memory usage and throughput.
Supports both local paths and S3.
"""

from __future__ import annotations

import logging
import re
import resource
import time
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Multipliers for size parsing (bytes)
_SIZE_UNITS: dict[str, int] = {
    "b": 1,
    "kb": 1024,
    "mb": 1024**2,
    "gb": 1024**3,
    "tb": 1024**4,
}

# Default candidate partition sizes: 50MB to 1GB as specified in assignment
_DEFAULT_MIN_BYTES = 50 * 1024 * 1024   # 50 MB
_DEFAULT_MAX_BYTES = 1 * 1024 * 1024 * 1024   # 1 GB
_DEFAULT_CANDIDATES = 5  # number of candidate sizes to test


def parse_size(size_str: str) -> int:
    """
    Parse a human-readable size string into bytes.

    Supports formats like "200MB", "1.5GB", "512kb", "2TB".
    Case-insensitive. Accepts optional spaces between number and unit.

    Args:
        size_str: Size string (e.g., "200MB", "1.5GB", "512 KB").

    Returns:
        Size in bytes.

    Raises:
        ValueError: If the string cannot be parsed.

    Examples:
        >>> parse_size("200MB")
        209715200
        >>> parse_size("1.5GB")
        1610612736
        >>> parse_size("512 KB")
        524288
    """
    if not size_str or not isinstance(size_str, str):
        raise ValueError("size_str must be a non-empty string")

    size_str = size_str.strip()
    if not size_str:
        raise ValueError("size_str cannot be empty")

    # Match optional minus, digits (with optional decimal), optional whitespace, unit
    pattern = re.compile(
        r"^(-?\d+(?:\.\d+)?)\s*([a-zA-Z]{1,3})\s*$",
        re.IGNORECASE,
    )
    match = pattern.match(size_str)
    if not match:
        raise ValueError(f"Cannot parse size string: '{size_str}'")

    num_str, unit_str = match.groups()
    num = float(num_str)
    unit = unit_str.lower()

    if unit not in _SIZE_UNITS:
        valid = ", ".join(_SIZE_UNITS.keys())
        raise ValueError(
            f"Unknown unit '{unit_str}'. Valid units: {valid}"
        )

    if num < 0:
        raise ValueError("Size cannot be negative")

    return int(num * _SIZE_UNITS[unit])


def _get_current_rss_bytes() -> int:
    """Return current process RSS (resident set size) in bytes."""
    try:
        # Linux/macOS
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (AttributeError, ValueError):
        return 0


def _get_rss_mb() -> float:
    """Return current RSS in MB. On macOS ru_maxrss is in bytes."""
    rss = _get_current_rss_bytes()
    # On Linux, ru_maxrss is in KB; on macOS it's in bytes
    if rss < 2**20:  # If < 1MB, assume KB (Linux)
        return rss / 1024.0
    return rss / (1024.0 * 1024.0)


def _run_batch_test(
    parquet_path: str,
    batch_size: int,
    storage_options: Optional[dict] = None,
    num_batches_to_read: int = 3,
) -> tuple[float, float]:
    """
    Run a minimal batch read test: read a few batches, measure time and peak memory.

    Uses PyArrow ParquetFile.iter_batches for memory-efficient batched reading.

    Args:
        parquet_path: Path to Parquet file (local or s3://).
        batch_size: Number of rows per batch.
        storage_options: Optional storage options for S3 (e.g., anonymous).
        num_batches_to_read: Number of batches to consume for the test.

    Returns:
        (elapsed_seconds, peak_rss_mb).
    """
    rss_before = _get_rss_mb()
    start = time.perf_counter()
    total_rows = 0

    try:
        pf = pq.ParquetFile(
            parquet_path,
            pre_buffer=False,
            coerce_int96_timestamp_unit="ms",
            **(storage_options or {}),
        )
        for i, batch in enumerate(pf.iter_batches(batch_size=batch_size)):
            if i >= num_batches_to_read:
                break
            total_rows += batch.num_rows
            # Touch data to ensure it's actually read
            _ = batch.nbytes

    finally:
        elapsed = time.perf_counter() - start

    rss_after = _get_rss_mb()
    peak_rss = max(rss_before, rss_after)

    return elapsed, peak_rss


def find_optimal_partition_size(
    parquet_paths: str | list[str],
    max_memory_usage_mb: float = 4096.0,
    min_partition_bytes: Optional[int] = None,
    max_partition_bytes: Optional[int] = None,
    num_candidates: int = _DEFAULT_CANDIDATES,
    storage_options: Optional[dict] = None,
    num_batches_per_test: int = 3,
) -> int:
    """
    Find the optimal partition (batch) size for Parquet reading within memory constraints.

    Tests candidate partition sizes (expressed as row counts, derived from byte ranges
    50MB–1GB by default), uses PyArrow batching, measures time and memory, and picks
    the best size that stays within max_memory_usage.

    Supports both local paths and S3 (s3://...).

    Args:
        parquet_paths: Single path or list of Parquet file paths (local or S3).
        max_memory_usage_mb: Maximum allowed peak RSS in MB; candidates exceeding this
            are rejected.
        min_partition_bytes: Minimum partition size in bytes (default 50MB).
        max_partition_bytes: Maximum partition size in bytes (default 1GB).
        num_candidates: Number of candidate batch sizes to test.
        storage_options: Storage options for S3 (e.g. anon=True for anonymous).
        num_batches_per_test: Number of batches to read per candidate for timing.

    Returns:
        Optimal partition size in bytes. Falls back to min_partition_bytes if no
        candidate fits within memory.

    Raises:
        FileNotFoundError: If no valid Parquet file can be opened.
        ValueError: If parameters are invalid.
    """
    if isinstance(parquet_paths, str):
        parquet_paths = [parquet_paths]

    if not parquet_paths:
        raise ValueError("parquet_paths cannot be empty")

    min_b = min_partition_bytes if min_partition_bytes is not None else _DEFAULT_MIN_BYTES
    max_b = max_partition_bytes if max_partition_bytes is not None else _DEFAULT_MAX_BYTES

    if min_b <= 0 or max_b <= 0 or min_b > max_b:
        raise ValueError(
            f"Invalid partition byte range: min={min_b}, max={max_b}"
        )

    # Use first available file for benchmarking
    sample_path: Optional[str] = None
    for p in parquet_paths:
        if p and (p.startswith("s3://") or Path(p).exists()):
            sample_path = p
            break

    if not sample_path:
        raise FileNotFoundError(
            "No valid Parquet file found for partition optimization"
        )

    # Infer approximate rows per byte from the sample file (for converting bytes -> rows)
    try:
        pf = pq.ParquetFile(
            sample_path,
            pre_buffer=False,
            **(storage_options or {}),
        )
        meta = pf.metadata
        total_rows = meta.num_rows
        total_bytes = sum(
            rg.total_byte_size for rg in meta.row_groups
        ) if meta.num_row_groups else 0
        bytes_per_row = total_bytes / total_rows if total_rows > 0 else 1000
    except Exception as e:
        logger.warning("Could not read Parquet metadata, using default bytes_per_row: %s", e)
        bytes_per_row = 500

    # Candidate batch sizes as row counts (convert from byte range)
    min_rows = max(1000, int(min_b / bytes_per_row))
    max_rows = max(min_rows, int(max_b / bytes_per_row))

    candidates = []
    if num_candidates <= 1:
        candidates = [min_rows]
    else:
        step = (max_rows - min_rows) / (num_candidates - 1)
        for i in range(num_candidates):
            r = int(min_rows + i * step)
            if r > 0:
                candidates.append(r)

    logger.info(
        "Testing %d partition candidates (rows: %s) on %s",
        len(candidates),
        candidates[:5] if len(candidates) > 5 else candidates,
        sample_path,
    )

    best_size_bytes = min_b
    best_time = float("inf")
    best_memory = 0.0

    for batch_rows in candidates:
        try:
            elapsed, peak_rss = _run_batch_test(
                sample_path,
                batch_size=batch_rows,
                storage_options=storage_options,
                num_batches_to_read=num_batches_per_test,
            )
        except Exception as e:
            logger.warning(
                "Batch test failed for batch_rows=%d: %s",
                batch_rows,
                e,
            )
            continue

        # Reject if over memory limit
        if peak_rss > max_memory_usage_mb:
            logger.debug(
                "Rejecting batch_rows=%d: peak_rss=%.1f MB > max=%.1f MB",
                batch_rows,
                peak_rss,
                max_memory_usage_mb,
            )
            continue

        # Prefer faster (lower elapsed time)
        if elapsed < best_time:
            best_time = elapsed
            best_memory = peak_rss
            best_size_bytes = int(batch_rows * bytes_per_row)
            best_size_bytes = max(min_b, min(max_b, best_size_bytes))

    logger.info(
        "Optimal partition size: %d bytes (~%.1f MB), "
        "best_time=%.3fs, peak_rss=%.1f MB",
        best_size_bytes,
        best_size_bytes / (1024 * 1024),
        best_time,
        best_memory,
    )

    return best_size_bytes


def bytes_to_human(b: int) -> str:
    """Convert bytes to human-readable string (e.g. '256 MB')."""
    for unit, mul in [("TB", 1024**4), ("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)]:
        if b >= mul:
            return f"{b / mul:.1f} {unit}"
    return f"{b} B"
