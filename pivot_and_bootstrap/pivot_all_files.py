"""
Main pipeline: pivot all taxi Parquet files into a single wide table.

Part 4 (30 pts): Main Pipeline — pivot_all_files.py

- Discover Parquet files (local or S3), check schemas, group by month.
- Process one month at a time: read → normalize → pivot → cleanup (< min_rides) → intermediate Parquet.
- Count and report month-mismatch rows (row's month ≠ file's expected month).
- Combine intermediates into a single wide table (taxi_type, date, pickup_place) × hour_0..hour_23.
- Store final table as Parquet, upload to S3, generate report (rows in/out, bad rows, memory, run time).
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure repo root is on path when run as script (e.g. python pivot_and_bootstrap/pivot_all_files.py)
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import pandas as pd

# Local utilities
from pivot_and_bootstrap.pivot_utils import (
    cleanup_low_count_rows,
    discover_parquet_files,
    find_pickup_datetime_col,
    find_pickup_location_col,
    get_file_size,
    get_storage_options,
    infer_month_from_path,
    infer_taxi_type_from_path,
    is_s3_path,
    pivot_counts_date_taxi_type_location,
)

logger = logging.getLogger(__name__)

# Common schema column names used after normalization
CANONICAL_DATETIME_COL = "pickup_datetime"
CANONICAL_LOCATION_COL = "pickup_location"

# -----------------------------------------------------------------------------
# Default input/output paths (used when CLI args are not provided).
# Edit these to run without passing --input-dir / --output-dir (e.g. on AWS).
# -----------------------------------------------------------------------------
DEFAULT_INPUT_DIR = "s3://dsc291-ucsd/taxi"
DEFAULT_OUTPUT_DIR = "./pivot_and_bootstrap"
# S3 upload for wide table (e.g. on EC2). Set env DSC291_S3_OUTPUT or use --s3-output.
DEFAULT_S3_OUTPUT = "s3://291-s3-bucket/wide.parquet"


def _get_peak_rss_bytes() -> int:
    """Return peak resident set size in bytes (best-effort; 0 if unavailable)."""
    try:
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF)
        # Linux: ru_maxrss is in KB; macOS: in bytes (documentation varies)
        rss = getattr(usage, "ru_maxrss", 0) or 0
        if rss and rss < 2**20:  # Likely KB if small
            rss = rss * 1024
        return int(rss)
    except Exception:
        return 0


def _aws_credentials_available() -> bool:
    """Return True if AWS credentials are configured (for S3 with --no-s3-anon)."""
    try:
        import botocore.session
        session = botocore.session.get_session()
        creds = session.get_credentials()
        return creds is not None and getattr(creds, "access_key", None) is not None
    except Exception:
        return False


def _read_parquet_schema(file_path: str, anon: Optional[bool] = None) -> List[str]:
    """Read only Parquet schema (column names) without loading data. Fast for schema check."""
    import pyarrow.parquet as pq
    if is_s3_path(file_path):
        from pivot_and_bootstrap.pivot_utils import get_filesystem, parse_s3_path
        fs = get_filesystem(file_path, anon=anon)
        bucket, key = parse_s3_path(file_path)
        full_path = f"{bucket}/{key}"
        with fs.open(full_path, "rb") as f:
            meta = pq.read_metadata(f)
            return list(meta.schema.names)
    meta = pq.read_metadata(file_path)
    return list(meta.schema.names)


def _path_suggests_older_parquet(file_path: str) -> bool:
    """True if path looks like older NYC TLC data (2009-2014) which often needs fastparquet."""
    path = (file_path or "").lower()
    for y in ("2009", "2010", "2011", "2012", "2013", "2014"):
        if y in path:
            return True
    return False


def read_parquet_file(
    file_path: str,
    anon: Optional[bool] = None,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Read a single Parquet file from local path or S3.
    For older-year paths (2009-2014) tries fastparquet first; else PyArrow first (with retries), then fastparquet.

    Args:
        file_path: Local path or s3:// URI to a Parquet file.
        anon: For S3, use anonymous access if True; if False, use AWS credentials.
        columns: If provided, read only these columns (faster, less memory).

    Returns:
        DataFrame with the file contents.

    Raises:
        Exception: On read or schema errors after all attempts.
    """
    storage_options = (
        get_storage_options(file_path, anon=anon) if is_s3_path(file_path) else {}
    )
    last_error: Optional[Exception] = None
    try_fastparquet_first = _path_suggests_older_parquet(file_path)

    def _read(engine: str, cols: Optional[List[str]] = None) -> pd.DataFrame:
        return pd.read_parquet(
            file_path,
            storage_options=storage_options or None,
            columns=cols or columns,
            engine=engine,
        )

    # For older paths, try fastparquet first (handles legacy codec better)
    if try_fastparquet_first:
        try:
            import fastparquet  # noqa: F401
            return _read("fastparquet")
        except ImportError:
            pass
        except Exception as e:
            last_error = e

    # PyArrow with retries (transient S3/network)
    for attempt in range(3):
        try:
            return _read("pyarrow")
        except Exception as e:
            last_error = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
            continue

    # Fastparquet fallback (or retry for older paths)
    try:
        import fastparquet  # noqa: F401
        logger.debug("PyArrow failed (%s), trying fastparquet for %s", last_error, file_path)
        return _read("fastparquet")
    except ImportError:
        pass
    except Exception as fp_err:
        last_error = fp_err

    # Last resort: full file with fastparquet then slice
    if columns:
        try:
            import fastparquet  # noqa: F401
            full = _read("fastparquet", cols=None)
            missing = [c for c in columns if c not in full.columns]
            if not missing:
                return full[columns].copy()
        except ImportError:
            pass
        except Exception:
            pass
    if last_error is not None:
        raise last_error
    raise RuntimeError("Failed to read parquet")


def normalize_to_common_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame to common schema: pickup_datetime, pickup_location.

    Uses pivot_utils column detection and renames to canonical names so all
    downstream code sees the same column names. Drops other columns to reduce
    memory; only keeps required for pivot.

    Args:
        df: Raw trip-level DataFrame (any taxi schema).

    Returns:
        DataFrame with columns pickup_datetime, pickup_location (and no other columns).

    Raises:
        ValueError: If required columns cannot be found.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=[CANONICAL_DATETIME_COL, CANONICAL_LOCATION_COL])

    cols = list(df.columns)
    dt_col = find_pickup_datetime_col(cols)
    loc_col = find_pickup_location_col(cols)
    if dt_col is None:
        raise ValueError("Could not find pickup datetime column; schema may be inconsistent.")
    if loc_col is None:
        raise ValueError("Could not find pickup location column; schema may be inconsistent.")

    out = df[[dt_col, loc_col]].copy()
    out.columns = [CANONICAL_DATETIME_COL, CANONICAL_LOCATION_COL]
    return out


def check_schemas_consistent(
    file_paths: List[str],
    sample_size: int = 2,
    anon: Optional[bool] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Check if required columns (pickup datetime, pickup location) can be detected
    in discovered files. Reads only Parquet metadata (no data). Samples up to sample_size files.

    Args:
        file_paths: List of Parquet file paths.
        sample_size: Max number of files to sample for schema check.
        anon: For S3 paths, use anonymous access if True; False for AWS credentials.

    Returns:
        (True, None) if all sampled files have required columns;
        (False, message) otherwise.
    """
    if not file_paths:
        return False, "No files to check."

    to_check = file_paths[:sample_size]
    for path in to_check:
        try:
            cols = _read_parquet_schema(path, anon=anon)
            if find_pickup_datetime_col(cols) is None:
                return False, f"Missing pickup datetime column in: {path}"
            if find_pickup_location_col(cols) is None and _find_pickup_lon_lat_cols(cols) is None:
                return False, f"Missing pickup location (or lon/lat) column in: {path}"
        except Exception as e:
            return False, f"Error reading schema {path}: {e}"

    return True, None


def _sanitize_intermediate_filename(file_path: str) -> str:
    """Produce a safe filename for intermediate Parquet (no path, no special chars)."""
    name = Path(file_path).name
    name = re.sub(r"[^\w\-\.]", "_", name)
    if not name.lower().endswith(".parquet"):
        name = name + ".parquet"
    return name


def _find_pickup_lon_lat_cols(columns: List[str]) -> Optional[Tuple[str, str]]:
    """Find pickup longitude and latitude columns (older TLC data without PULocationID). Returns (lon_col, lat_col) or None."""
    col_map = {(c or "").strip().lower(): c for c in columns}
    lon_col = None
    for v in ("pickup_longitude", "pickup_long", "start_lon", "start_longitude"):
        if v in col_map:
            lon_col = col_map[v]
            break
    if lon_col is None:
        for norm, c in col_map.items():
            if "pickup" in norm and "long" in norm:
                lon_col = c
                break
    lat_col = None
    for v in ("pickup_latitude", "pickup_lat", "start_lat", "start_latitude"):
        if v in col_map:
            lat_col = col_map[v]
            break
    if lat_col is None:
        for norm, c in col_map.items():
            if "pickup" in norm and "lat" in norm:
                lat_col = c
                break
    return (lon_col, lat_col) if (lon_col and lat_col) else None


def process_single_file(
    file_path: str,
    intermediate_dir: Path,
    min_rides: int = 50,
    anon: Optional[bool] = None,
    partition_size_bytes: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Process one Parquet file: read → normalize → pivot → cleanup → write intermediate.

    When partition_size_bytes is set and schema has dt_col+loc_col, reads in batches
    (PyArrow iter_batches) to bound memory; otherwise reads the full file.

    Counts rows where the row's month does not match the month inferred from the file path.

    Args:
        file_path: Path to Parquet file (local or S3).
        intermediate_dir: Directory to write intermediate Parquet (e.g. intermediates/2023-01/).
        min_rides: Minimum total rides to keep a row (cleanup_low_count_rows).
        partition_size_bytes: If set, read file in batches of ~this size (bytes) for memory control.

    Returns:
        Dict with: input_rows, output_rows, month_mismatch_count, cleanup_removed,
                   parse_failures, intermediate_path, file_path, error (if failed).
    """
    result: Dict[str, Any] = {
        "file_path": file_path,
        "input_rows": 0,
        "output_rows": 0,
        "month_mismatch_count": 0,
        "cleanup_removed": 0,
        "parse_failures": 0,
        "intermediate_path": None,
        "error": None,
    }

    expected_month = infer_month_from_path(file_path)
    taxi_type = infer_taxi_type_from_path(file_path)

    df: Optional[pd.DataFrame] = None
    normalized: Optional[pd.DataFrame] = None
    try:
        try:
            cols = _read_parquet_schema(file_path, anon=anon)
        except Exception:
            # Schema read can fail (e.g. codec on metadata); still try full file read below
            cols = []
        dt_col = find_pickup_datetime_col(cols) if cols else None
        loc_col = find_pickup_location_col(cols) if cols else None
        if dt_col is not None and loc_col is not None:
            if partition_size_bytes and partition_size_bytes > 0:
                # Batched path: read in chunks to bound memory (uses partition_optimization size)
                import pyarrow.parquet as pq
                storage_opts = (
                    get_storage_options(file_path, anon=anon) if is_s3_path(file_path) else {}
                )
                pf = pq.ParquetFile(file_path, **(storage_opts or {}))
                meta = pf.metadata
                num_rows = meta.num_rows or 1
                total_bytes = (
                    sum(rg.total_byte_size for rg in meta.row_groups)
                    if meta.num_row_groups else partition_size_bytes
                )
                bytes_per_row = max(total_bytes / num_rows, 100)
                batch_rows = max(1000, int(partition_size_bytes / bytes_per_row))
                # Accumulate (date, hour, pickup_place) -> count in one dict to avoid
                # many small DataFrames and a large concat+groupby
                agg_counts: Dict[Tuple[Any, int, str], int] = defaultdict(int)
                acc_input = 0
                acc_parse_fail = 0
                acc_mismatch = 0
                for batch in pf.iter_batches(
                    batch_size=batch_rows, columns=[dt_col, loc_col]
                ):
                    df = batch.to_pandas()
                    df = df.rename(
                        columns={
                            dt_col: CANONICAL_DATETIME_COL,
                            loc_col: CANONICAL_LOCATION_COL,
                        }
                    )
                    dt_ser = pd.to_datetime(
                        df[CANONICAL_DATETIME_COL], errors="coerce"
                    )
                    valid = dt_ser.notna()
                    acc_input += len(df)
                    acc_parse_fail += int((~valid).sum())
                    df = df.loc[valid].copy()
                    df[CANONICAL_DATETIME_COL] = dt_ser.loc[valid].values
                    if expected_month is not None:
                        y, m = expected_month
                        ry = df[CANONICAL_DATETIME_COL].dt.year
                        rm = df[CANONICAL_DATETIME_COL].dt.month
                        acc_mismatch += int(((ry != y) | (rm != m)).sum())
                    batch_long = pd.DataFrame(
                        {
                            "date": df[CANONICAL_DATETIME_COL].dt.date,
                            "hour": df[CANONICAL_DATETIME_COL].dt.hour,
                            "pickup_place": df[CANONICAL_LOCATION_COL].astype(str),
                        },
                        copy=False,
                    )
                    bc = batch_long.groupby(
                        ["date", "hour", "pickup_place"], as_index=False
                    ).size()
                    for t in bc.itertuples(index=False):
                        agg_counts[(t[0], int(t[1]), t[2])] += t[3]
                if not agg_counts:
                    result["input_rows"] = 0
                    result["intermediate_path"] = None
                    return result
                result["input_rows"] = acc_input
                result["parse_failures"] = acc_parse_fail
                result["month_mismatch_count"] = acc_mismatch
                # Build long-format DataFrame from accumulated counts (single pass)
                long_agg = pd.DataFrame(
                    [
                        (d, h, p, c)
                        for (d, h, p), c in agg_counts.items()
                    ],
                    columns=["date", "hour", "pickup_place", "count"],
                )
                long_agg["taxi_type"] = taxi_type
                long_agg = long_agg[
                    ["taxi_type", "date", "pickup_place", "hour", "count"]
                ]
                pivoted = pivot_counts_date_taxi_type_location(long_agg)
                cleaned, cleanup_stats = cleanup_low_count_rows(
                    pivoted, min_rides=min_rides
                )
                result["cleanup_removed"] = cleanup_stats.get(
                    "rows_removed", 0
                )
                result["output_rows"] = len(cleaned)
                intermediate_dir.mkdir(parents=True, exist_ok=True)
                base_name = _sanitize_intermediate_filename(file_path)
                out_path = intermediate_dir / base_name
                cleaned.to_parquet(
                    out_path,
                    index=True,
                    engine="pyarrow",
                    compression="snappy",
                )
                result["intermediate_path"] = str(out_path)
                return result
            # Fast path: read only the two columns we need
            df = read_parquet_file(file_path, anon=anon, columns=[dt_col, loc_col])
            normalized = df.rename(
                columns={
                    dt_col: CANONICAL_DATETIME_COL,
                    loc_col: CANONICAL_LOCATION_COL,
                }
            )
        else:
            # Fallback for older files (e.g. 2009) with different schema or lon/lat instead of PULocationID
            df = read_parquet_file(file_path, anon=anon)
            cols = list(df.columns)
            dt_col = find_pickup_datetime_col(cols)
            loc_col = find_pickup_location_col(cols)
            if dt_col is None:
                raise ValueError("Could not find pickup datetime column; schema may be inconsistent.")
            if loc_col is not None:
                normalized = df[[dt_col, loc_col]].copy()
                normalized.columns = [CANONICAL_DATETIME_COL, CANONICAL_LOCATION_COL]
            else:
                lon_lat = _find_pickup_lon_lat_cols(cols)
                if lon_lat is None:
                    raise ValueError("Could not find pickup location (or lon/lat) column; schema may be inconsistent.")
                lon_col, lat_col = lon_lat
                normalized = df[[dt_col, lon_col, lat_col]].copy()
                normalized.columns = [CANONICAL_DATETIME_COL, "_lon", "_lat"]
                normalized[CANONICAL_LOCATION_COL] = (
                    normalized["_lon"].astype(float).round(3).astype(str)
                    + "_"
                    + normalized["_lat"].astype(float).round(3).astype(str)
                )
                normalized = normalized[[CANONICAL_DATETIME_COL, CANONICAL_LOCATION_COL]]
    except Exception as e:
        result["error"] = str(e)
        result["parse_failures"] = 1  # whole file
        # Log read errors (e.g. GZipCodec / corrupted file) without full traceback
        if isinstance(e, OSError) or "GZipCodec" in str(e) or "incorrect data check" in str(e):
            logger.warning("Skipping unreadable file %s: %s", file_path, e)
        else:
            logger.exception("Failed to read %s: %s", file_path, e)
        return result

    result["input_rows"] = len(normalized)
    if len(normalized) == 0:
        result["intermediate_path"] = None
        return result

    # Parse datetime for month-mismatch and pivot (skip if already datetime64)
    dt_ser = normalized[CANONICAL_DATETIME_COL]
    if not pd.api.types.is_datetime64_any_dtype(dt_ser):
        dt_ser = pd.to_datetime(dt_ser, errors="coerce")
    normalized[CANONICAL_DATETIME_COL] = dt_ser
    valid = normalized[CANONICAL_DATETIME_COL].notna()
    result["parse_failures"] += int((~valid).sum())
    normalized = normalized[valid].copy()

    if len(normalized) == 0:
        result["intermediate_path"] = None
        return result

    # Month mismatch: row's (year, month) != file's expected (year, month)
    if expected_month is not None:
        y, m = expected_month
        row_year = normalized[CANONICAL_DATETIME_COL].dt.year
        row_month = normalized[CANONICAL_DATETIME_COL].dt.month
        mismatch = (row_year != y) | (row_month != m)
        result["month_mismatch_count"] = int(mismatch.sum())

    # Pivot
    pivoted = pivot_counts_date_taxi_type_location(
        normalized,
        taxi_type=taxi_type,
        datetime_col=CANONICAL_DATETIME_COL,
        location_col=CANONICAL_LOCATION_COL,
    )

    # Cleanup rows with < min_rides
    cleaned, cleanup_stats = cleanup_low_count_rows(pivoted, min_rides=min_rides)
    result["cleanup_removed"] = cleanup_stats.get("rows_removed", 0)
    result["output_rows"] = len(cleaned)

    # Write intermediate
    intermediate_dir.mkdir(parents=True, exist_ok=True)
    base_name = _sanitize_intermediate_filename(file_path)
    out_path = intermediate_dir / base_name
    cleaned.to_parquet(
        out_path, index=True, engine="pyarrow", compression="snappy"
    )
    result["intermediate_path"] = str(out_path)

    return result


def group_files_by_month(file_paths: List[str]) -> Dict[Tuple[int, int], List[str]]:
    """Group file paths by (year, month). Files with un inferrable month go into (0, 0)."""
    groups: Dict[Tuple[int, int], List[str]] = defaultdict(list)
    for p in file_paths:
        key = infer_month_from_path(p) or (0, 0)
        groups[key].append(p)
    return dict(groups)


def flatten_files_by_month(
    by_month: Dict[Tuple[int, int], List[str]],
    output_dir: Path,
    month_keys: Optional[List[Tuple[int, int]]] = None,
) -> List[Tuple[str, Path]]:
    """Flatten (month_key -> file_paths) to list of (file_path, intermediate_dir) for one big pool.
    If month_keys is set, only include those months (e.g. for --max-months)."""
    keys = month_keys if month_keys is not None else list(by_month.keys())
    tasks: List[Tuple[str, Path]] = []
    for (y, m) in keys:
        paths = by_month.get((y, m), [])
        intermediate_dir = output_dir / "intermediates" / f"{y}-{m:02d}"
        for fp in paths:
            tasks.append((fp, intermediate_dir))
    return tasks


def process_month_batch(
    month_key: Tuple[int, int],
    file_paths: List[str],
    output_dir: Path,
    min_rides: int,
    workers: int = 1,
    anon: Optional[bool] = None,
    partition_size_bytes: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Process all files for one month. If workers > 1, process files in parallel (multiprocessing).
    When partition_size_bytes is set, each file may be read in batches to bound memory.
    """
    y, m = month_key
    intermediate_dir = output_dir / "intermediates" / f"{y}-{m:02d}"
    results: List[Dict[str, Any]] = []
    logger.info("Processing month %d-%02d: %d file(s)", y, m, len(file_paths))

    if workers <= 1:
        for i, fp in enumerate(file_paths, 1):
            results.append(
                process_single_file(
                    fp,
                    intermediate_dir,
                    min_rides=min_rides,
                    anon=anon,
                    partition_size_bytes=partition_size_bytes,
                )
            )
            logger.debug("Month %d-%02d: %d / %d file(s) processed", y, m, i, len(file_paths))
        logger.info("Month %d-%02d done: %d file(s) processed", y, m, len(results))
        return results

    from concurrent.futures import ProcessPoolExecutor, as_completed
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                process_single_file,
                fp,
                intermediate_dir,
                min_rides,
                anon,
                partition_size_bytes,
            ): fp
            for fp in file_paths
        }
        done = 0
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                fp = futures[fut]
                results.append({
                    "file_path": fp,
                    "input_rows": 0,
                    "output_rows": 0,
                    "month_mismatch_count": 0,
                    "cleanup_removed": 0,
                    "parse_failures": 0,
                    "intermediate_path": None,
                    "error": str(e),
                })
            done += 1
            logger.debug("Month %d-%02d: %d / %d file(s) processed", y, m, done, len(file_paths))

    logger.info("Month %d-%02d done: %d file(s) processed", y, m, len(results))
    return results


def _read_one_intermediate(path: str) -> Optional[pd.DataFrame]:
    """Read a single intermediate Parquet file. Used for parallel combine."""
    try:
        storage_options = get_storage_options(path) if is_s3_path(path) else {}
        return pd.read_parquet(
            path,
            storage_options=storage_options or None,
            engine="pyarrow",
        )
    except Exception as e:
        logger.warning("Could not read intermediate %s: %s", path, e)
        return None


def _read_one_intermediate_arrow(path: str):
    """Read a single intermediate Parquet file as PyArrow Table. Returns None on failure."""
    try:
        import pyarrow.parquet as pq
        storage_options = get_storage_options(path) if is_s3_path(path) else {}
        return pq.read_table(path, **(storage_options or {}))
    except Exception as e:
        logger.warning("Could not read intermediate %s: %s", path, e)
        return None


def combine_into_wide_table(intermediate_paths: List[str]) -> pd.DataFrame:
    """
    Read all intermediate Parquet files, aggregate by (taxi_type, date, pickup_place),
    sum hour_0..hour_23 to produce a single wide table. Reads files in parallel.

    Args:
        intermediate_paths: List of paths to intermediate Parquet files (from process_single_file).

    Returns:
        Single DataFrame indexed by (taxi_type, date, pickup_place) with columns hour_0..hour_23.
    """
    if not intermediate_paths:
        return pd.DataFrame(
            columns=[f"hour_{i}" for i in range(24)],
            index=pd.MultiIndex.from_tuples([], names=["taxi_type", "date", "pickup_place"]),
        )

    logger.info("Combining %d intermediate Parquet file(s) into wide table", len(intermediate_paths))
    logger.info("Reading and concatenating intermediates...")
    max_workers = min(16, len(intermediate_paths), max((os.cpu_count() or 4) * 2, 4))
    # Use PyArrow concat for lower peak memory when combining many intermediates
    try:
        import pyarrow as pa
        tables: List[pa.Table] = []
        if max_workers <= 1:
            for path in intermediate_paths:
                t = _read_one_intermediate_arrow(path)
                if t is not None:
                    tables.append(t)
        else:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(_read_one_intermediate_arrow, p): p for p in intermediate_paths}
                for fut in as_completed(futures):
                    t = fut.result()
                    if t is not None:
                        tables.append(t)
        if tables:
            combined_table = pa.concat_tables(tables)
            combined = combined_table.to_pandas()
            del tables
            del combined_table
        else:
            combined = None
    except ImportError:
        combined = None
    if combined is None:
        # Fallback: pandas read + concat
        frames: List[pd.DataFrame] = []
        if max_workers <= 1:
            for path in intermediate_paths:
                df = _read_one_intermediate(path)
                if df is not None:
                    frames.append(df)
        else:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(_read_one_intermediate, p): p for p in intermediate_paths}
                for fut in as_completed(futures):
                    df = fut.result()
                    if df is not None:
                        frames.append(df)
        if not frames:
            return pd.DataFrame(
                columns=[f"hour_{i}" for i in range(24)],
                index=pd.MultiIndex.from_tuples([], names=["taxi_type", "date", "pickup_place"]),
            )
        combined = pd.concat(frames, axis=0)

    if combined is None or len(combined) == 0:
        return pd.DataFrame(
            columns=[f"hour_{i}" for i in range(24)],
            index=pd.MultiIndex.from_tuples([], names=["taxi_type", "date", "pickup_place"]),
        )

    hour_cols = [c for c in combined.columns if c.startswith("hour_")]
    # Parquet written with index=True: PyArrow read gives flat columns; pandas read may keep index
    if isinstance(combined.index, pd.MultiIndex) and combined.index.names == ["taxi_type", "date", "pickup_place"]:
        combined = combined[hour_cols]
        wide = combined.groupby(
            level=["taxi_type", "date", "pickup_place"], sort=False
        ).sum()
    else:
        key_cols = [c for c in combined.columns if c in ("taxi_type", "date", "pickup_place")]
        combined = combined[key_cols + hour_cols]
        wide = combined.groupby(
            by=key_cols, sort=False
        ).sum()
    wide = wide.astype(int)

    logger.info("Aggregating by (taxi_type, date, pickup_place)...")
    logger.info("Combined %d intermediate(s) into wide table with %d rows", len(intermediate_paths), len(wide))
    return wide


def upload_to_s3(local_path: str, s3_path: str) -> None:
    """Upload a local file to S3. Uses s3fs via pivot_utils.get_filesystem. Streams in chunks to avoid loading full file into memory. Requires AWS credentials (env vars, IAM role, or ~/.aws/credentials)."""
    import shutil
    from pivot_and_bootstrap.pivot_utils import get_filesystem, parse_s3_path
    # Use authenticated access; anonymous S3 cannot perform multipart uploads.
    fs = get_filesystem(s3_path, anon=False)
    bucket, key = parse_s3_path(s3_path)
    full_s3 = f"{bucket}/{key}"
    with open(local_path, "rb") as f:
        with fs.open(full_s3, "wb") as out:
            shutil.copyfileobj(f, out, length=2**20)  # 1 MB chunks
    logger.info("Uploaded %s -> s3://%s", local_path, full_s3)


def generate_report(
    input_row_count: int,
    output_row_count: int,
    bad_rows: int,
    month_mismatch_total: int,
    cleanup_removed_total: int,
    parse_failures_total: int,
    peak_rss_bytes: int,
    run_time_seconds: float,
    report_path: str,
    json_path: Optional[str] = None,
    files_failed: int = 0,
    failed_files: Optional[List[Dict[str, str]]] = None,
    time_discovery_seconds: float = 0.0,
    time_processing_seconds: float = 0.0,
    time_combine_output_seconds: float = 0.0,
    workers_used: Optional[int] = None,
    cpu_count: Optional[int] = None,
    input_bytes: int = 0,
    output_bytes: int = 0,
    s3_output_uri: str = "",
) -> None:
    """
    Write pipeline report to a small .tex file (required). Optionally write JSON.
    Includes resource utilization and run-time breakdown per assignment.
    """
    peak_rss_mb = round(peak_rss_bytes / (1024 * 1024), 2)
    run_time_minutes = round(run_time_seconds / 60, 2)
    # Throughput (MB/s): input during processing, output during combine
    input_throughput_mbs = (
        (input_bytes / (1024 * 1024)) / time_processing_seconds
        if time_processing_seconds > 0 and input_bytes > 0
        else None
    )
    output_throughput_mbs = (
        (output_bytes / (1024 * 1024)) / time_combine_output_seconds
        if time_combine_output_seconds > 0 and output_bytes > 0
        else None
    )

    report = {
        "input_row_count": input_row_count,
        "output_row_count": output_row_count,
        "bad_rows_ignored": bad_rows,
        "month_mismatch_total": month_mismatch_total,
        "cleanup_removed_total": cleanup_removed_total,
        "parse_failures_total": parse_failures_total,
        "files_failed": files_failed,
        "failed_files": failed_files if failed_files is not None else [],
        "peak_rss_bytes": peak_rss_bytes,
        "peak_rss_mb": peak_rss_mb,
        "run_time_seconds": run_time_seconds,
        "run_time_minutes": run_time_minutes,
        "time_discovery_seconds": round(time_discovery_seconds, 2),
        "time_processing_seconds": round(time_processing_seconds, 2),
        "time_combine_output_seconds": round(time_combine_output_seconds, 2),
        "workers_used": workers_used,
        "cpu_count": cpu_count,
        "input_bytes": input_bytes,
        "output_bytes": output_bytes,
        "input_throughput_mb_s": round(input_throughput_mbs, 2) if input_throughput_mbs is not None else None,
        "output_throughput_mb_s": round(output_throughput_mbs, 2) if output_throughput_mbs is not None else None,
        "s3_output_uri": s3_output_uri or None,
    }

    # Required: output the report to a small tex file (with resource utilization & run-time breakdown)
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as f:
        f.write("% Pipeline report (auto-generated)\n")
        f.write("% Includes: row counts, bad rows, memory, run time; resource utilization; run-time breakdown.\n\n")
        f.write("\\textbf{Row counts \\& bad rows}\\\\\n")
        f.write("\\begin{tabular}{ll}\n")
        f.write("  Input row count & {}\\\\\n".format(input_row_count))
        f.write("  Output row count & {}\\\\\n".format(output_row_count))
        f.write("  Bad rows ignored & {}\\\\\n".format(bad_rows))
        f.write("  Month mismatch total & {}\\\\\n".format(month_mismatch_total))
        f.write("  Cleanup removed (< min rides) & {}\\\\\n".format(cleanup_removed_total))
        f.write("  Parse failures & {}\\\\\n".format(parse_failures_total))
        f.write("\\end{tabular}\\\\\n\n")
        f.write("\\textbf{Resource utilization}\\\\\n")
        f.write("\\begin{tabular}{ll}\n")
        f.write("  Peak RSS (MB) & {:.2f}\\\\\n".format(peak_rss_mb))
        f.write("  Workers used & {}\\\\\n".format(workers_used if workers_used is not None else "---"))
        f.write("  CPU count & {}\\\\\n".format(cpu_count if cpu_count is not None else "---"))
        if input_throughput_mbs is not None:
            f.write("  Input throughput (MB/s) & {:.2f}\\\\\n".format(input_throughput_mbs))
        if output_throughput_mbs is not None:
            f.write("  Output throughput (MB/s) & {:.2f}\\\\\n".format(output_throughput_mbs))
        if s3_output_uri:
            f.write("  S3 output & {}\\\\\n".format(s3_output_uri.replace("_", "\\_")))
        f.write("\\end{tabular}\\\\\n\n")
        f.write("\\textbf{Run-time analysis (seconds)}\\\\\n")
        f.write("\\begin{tabular}{ll}\n")
        f.write("  Wall time (total) & {:.2f}\\\\\n".format(run_time_seconds))
        f.write("  File discovery & {:.2f}\\\\\n".format(time_discovery_seconds))
        f.write("  Per-month file processing & {:.2f}\\\\\n".format(time_processing_seconds))
        f.write("  Combine \\& output & {:.2f}\\\\\n".format(time_combine_output_seconds))
        f.write("\\end{tabular}\n")
    logger.info("Report written to %s", report_path)

    if json_path:
        Path(json_path).parent.mkdir(parents=True, exist_ok=True)
        with open(json_path, "w") as f:
            json.dump(report, f, indent=2)
        logger.info("JSON report written to %s", json_path)


def run_partition_optimization(
    file_paths: List[str],
    max_memory_usage: Optional[str],
    storage_options: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    """
    If partition_optimization module exists and max_memory_usage is set,
    run find_optimal_partition_size on a sample of file_paths and return
    optimal partition size in bytes (for batched reading). Returns None if
    disabled or module unavailable.
    """
    try:
        from pivot_and_bootstrap.partition_optimization import (  # type: ignore[import-untyped]
            find_optimal_partition_size,
            parse_size,
        )
    except ImportError:
        logger.debug("partition_optimization not available; skipping partition optimization")
        return None

    if not max_memory_usage or not file_paths:
        return None

    try:
        max_bytes = parse_size(max_memory_usage)
        max_mb = max_bytes / (1024 * 1024)
        # Use first few files for benchmarking (same schema typically)
        sample = file_paths[: min(5, len(file_paths))]
        optimal_bytes = find_optimal_partition_size(
            sample,
            max_memory_usage_mb=max_mb,
            storage_options=storage_options or {},
        )
        logger.info(
            "Partition optimization: using batch size %d bytes (~%.1f MB)",
            optimal_bytes,
            optimal_bytes / (1024 * 1024),
        )
        return optimal_bytes
    except Exception as e:
        logger.warning("Partition optimization failed: %s", e)
        return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pivot taxi Parquet files into a single wide table (taxi_type, date, pickup_place) x hour_0..23",
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Local dir or s3:// path (default from code: {DEFAULT_INPUT_DIR!r})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Local directory for intermediates and final Parquet (default from code: {DEFAULT_OUTPUT_DIR!r})",
    )
    parser.add_argument(
        "--min-rides",
        type=int,
        default=50,
        help="Discard rows with fewer than this many total rides (default: 50)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers per month (default: min(4, CPU count))",
    )
    parser.add_argument(
        "--parallel-files",
        type=int,
        default=None,
        metavar="N",
        help="Process all files in one pool with N workers (faster). Overrides --workers for file processing.",
    )
    parser.add_argument(
        "--max-months",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N months (by date). Use e.g. 12 to run in ~30 min on S3 instead of full history.",
    )
    parser.add_argument(
        "--partition-size",
        type=str,
        default=None,
        help="Optional partition size (e.g. 200MB) for optimization; requires partition_optimization",
    )
    parser.add_argument(
        "--skip-partition-optimization",
        action="store_true",
        help="Do not run partition optimization even if module available",
    )
    parser.add_argument(
        "--keep-intermediate",
        action="store_true",
        help="Keep intermediate Parquet files after combining",
    )
    parser.add_argument(
        "--s3-output",
        type=str,
        default=None,
        help="S3 URI for final wide table (e.g. s3://291-s3-bucket/wide.parquet). On EC2, set env DSC291_S3_OUTPUT to upload without passing this.",
    )
    parser.add_argument(
        "--report-output",
        type=str,
        default=None,
        help="Path for report output: .tex (required) or .json (default: <output-dir>/report.tex)",
    )
    parser.add_argument(
        "--report-json",
        type=str,
        default=None,
        help="Optional path for JSON report (default: none); use for machine-readable stats",
    )
    parser.add_argument(
        "--max-memory-usage",
        type=str,
        default=None,
        help="Max memory for partition optimization (e.g. 4GB)",
    )
    parser.add_argument(
        "--no-s3-anon",
        action="store_true",
        help="Use AWS credentials for S3 (run aws configure or set AWS_ACCESS_KEY_ID). If credentials are missing, falls back to anonymous access.",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    args = parser.parse_args()

    if args.workers is None:
        args.workers = min(4, os.cpu_count() or 4)
        if args.workers < 1:
            args.workers = 1

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    # Default report: assignment requires "output the report to a small tex file"
    if args.report_output is None:
        args.report_output = str(output_dir / "report.tex")
    # S3 upload: on EC2 set env DSC291_S3_OUTPUT=s3://291-s3-bucket/wide.parquet or pass --s3-output
    if args.s3_output is None:
        args.s3_output = os.environ.get("DSC291_S3_OUTPUT", DEFAULT_S3_OUTPUT)

    # S3: anonymous by default; use credentials if --no-s3-anon (required for nyc-tlc)
    s3_anon = not args.no_s3_anon

    # When reading from S3 with credentials, require AWS to be configured up front
    if is_s3_path(args.input_dir) and not s3_anon:
        if not _aws_credentials_available():
            logger.error(
                "S3 input requires AWS credentials. Configure them and run with --no-s3-anon.\n"
                "  1. Install AWS CLI: pip install awscli (or brew install awscli)\n"
                "  2. Run: aws configure\n"
                "  3. Enter your Access Key ID and Secret Access Key (from AWS IAM → Users → Security credentials → Create access key)\n"
                "  4. Run this script again with: --input-dir \"s3://nyc-tlc/trip data/\" --output-dir ... --no-s3-anon"
            )
            return 1

    wall_start = time.perf_counter()
    rss_start = _get_peak_rss_bytes()

    logger.info(
        "Pipeline started. input_dir=%s output_dir=%s workers=%s parallel_files=%s",
        args.input_dir,
        args.output_dir,
        args.workers,
        getattr(args, "parallel_files", None),
    )

    # 1. Discover
    logger.info("Step 1/7: Discovering Parquet files...")
    try:
        files = discover_parquet_files(args.input_dir, anon=s3_anon)
    except Exception as e:
        if not s3_anon and (
            "Unable to locate credentials" in str(e)
            or "NoCredentialsError" in type(e).__name__
        ):
            logger.warning(
                "AWS credentials not found (run 'aws configure' or set AWS_ACCESS_KEY_ID). "
                "Trying anonymous S3 access."
            )
            s3_anon = True
            try:
                files = discover_parquet_files(args.input_dir, anon=s3_anon)
            except Exception as e2:
                err_msg = str(e2).lower()
                if "403" in err_msg or "forbidden" in err_msg:
                    logger.error(
                        "S3 returned 403 Forbidden. The nyc-tlc bucket does not allow anonymous access. "
                        "Configure AWS credentials (aws configure) and run with --no-s3-anon."
                    )
                logger.exception("Discovery failed: %s", e2)
                return 1
        else:
            logger.exception("Discovery failed: %s", e)
            return 1

    if not files:
        logger.warning("No Parquet files found under %s", args.input_dir)
        return 0

    logger.info("Discovered %d Parquet file(s)", len(files))
    time_discovery_seconds = time.perf_counter() - wall_start

    # 2. Check schemas
    logger.info("Step 2/7: Checking schemas (sampling files)...")
    ok, msg = check_schemas_consistent(files, anon=s3_anon)
    if not ok:
        logger.warning("Schema check: %s. Proceeding with common-schema normalization.", msg)
    else:
        logger.info("Schema check: required columns present in sampled files.")

    # 3. Optional partition optimization: get batch size (bytes) for memory-bounded reads
    logger.info("Step 3/7: Partition / memory settings...")
    partition_size_bytes: Optional[int] = None
    if args.partition_size:
        try:
            from pivot_and_bootstrap.partition_optimization import parse_size  # type: ignore[import-untyped]
            partition_size_bytes = parse_size(args.partition_size)
        except Exception as e:
            logger.warning("Could not parse --partition-size %r: %s", args.partition_size, e)
    elif (
        not args.skip_partition_optimization
        and args.max_memory_usage
        and files
    ):
        storage_opts = (
            get_storage_options(args.input_dir, anon=s3_anon)
            if is_s3_path(args.input_dir)
            else None
        )
        partition_size_bytes = run_partition_optimization(
            files, args.max_memory_usage, storage_opts
        )

    if partition_size_bytes is not None:
        logger.info("Using partition size %.1f MB for batched reads", partition_size_bytes / (1024 * 1024))
    else:
        logger.info("No partition size; reading full files per worker.")

    # 4. Group by month, process files
    logger.info("Step 4/7: Grouping by month and processing files...")
    by_month = group_files_by_month(files)
    # Sort months for deterministic order (skip (0,0) or put last)
    month_keys = sorted(k for k in by_month if k != (0, 0))
    if (0, 0) in by_month:
        month_keys.append((0, 0))
    if getattr(args, "max_months", None) is not None and args.max_months > 0:
        month_keys = month_keys[: args.max_months]
        n_files_subset = sum(len(by_month[k]) for k in month_keys)
        logger.info("Limiting to first %d month(s), %d file(s) (--max-months)", len(month_keys), n_files_subset)
    total_files = sum(len(by_month[k]) for k in month_keys)
    logger.info("Grouped into %d month(s), %d total file(s)", len(month_keys), total_files)

    t_processing_start = time.perf_counter()
    workers_used = args.workers  # may be overridden in parallel_files branch

    all_results: List[Dict[str, Any]] = []
    total_input_rows = 0
    total_output_rows_before_combine = 0
    total_month_mismatch = 0
    total_cleanup_removed = 0
    total_parse_failures = 0
    intermediate_paths: List[str] = []
    max_concurrent = args.parallel_files

    if max_concurrent is not None and max_concurrent > 0:
        # Process ALL files in one pool (many months in parallel)
        tasks = flatten_files_by_month(by_month, output_dir, month_keys=month_keys)
        n_workers = min(max_concurrent, len(tasks))
        workers_used = n_workers
        logger.info(
            "Processing %d files in one pool with %d workers (--parallel-files)",
            len(tasks),
            n_workers,
        )
        from concurrent.futures import ProcessPoolExecutor, as_completed
        try:
            from tqdm import tqdm
            progress = tqdm(total=len(tasks), desc="Files", unit="file")
        except ImportError:
            progress = None
        done_count = 0
        log_interval = max(1, len(tasks) // 20)  # log ~20 times over the run
        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            futures = {
                executor.submit(
                    process_single_file,
                    fp,
                    intermediate_dir,
                    args.min_rides,
                    s3_anon,
                    partition_size_bytes,
                ): fp
                for fp, intermediate_dir in tasks
            }
            for fut in as_completed(futures):
                if progress is not None:
                    progress.update(1)
                done_count += 1
                if done_count % log_interval == 0 or done_count == len(tasks):
                    logger.info("Files completed: %d / %d", done_count, len(tasks))
                try:
                    r = fut.result()
                    all_results.append(r)
                except Exception as e:
                    fp = futures[fut]
                    all_results.append({
                        "file_path": fp,
                        "input_rows": 0,
                        "output_rows": 0,
                        "month_mismatch_count": 0,
                        "cleanup_removed": 0,
                        "parse_failures": 0,
                        "intermediate_path": None,
                        "error": str(e),
                    })
        if progress is not None:
            progress.close()
        logger.info("File processing complete: %d / %d files", len(all_results), len(tasks))
        for r in all_results:
            total_input_rows += r.get("input_rows", 0)
            total_output_rows_before_combine += r.get("output_rows", 0)
            total_month_mismatch += r.get("month_mismatch_count", 0)
            total_cleanup_removed += r.get("cleanup_removed", 0)
            total_parse_failures += r.get("parse_failures", 0)
            if r.get("intermediate_path"):
                intermediate_paths.append(r["intermediate_path"])
        logger.info(
            "Aggregated: %d input rows, %d intermediate file(s) written",
            total_input_rows,
            len(intermediate_paths),
        )
        time_processing_seconds = time.perf_counter() - t_processing_start
    else:
        # Original: process one month at a time with --workers per month
        try:
            from tqdm import tqdm
            month_iter = tqdm(month_keys, desc="Months")
        except ImportError:
            month_iter = month_keys
        files_processed_so_far = 0
        for month_key in month_iter:
            month_files = by_month[month_key]
            results = process_month_batch(
                month_key,
                month_files,
                output_dir,
                min_rides=args.min_rides,
                workers=args.workers,
                anon=s3_anon,
                partition_size_bytes=partition_size_bytes,
            )
            all_results.extend(results)
            files_processed_so_far += len(results)
            logger.info(
                "Files processed: %d / %d total (month %s: %d files)",
                files_processed_so_far,
                total_files,
                f"{month_key[0]}-{month_key[1]:02d}" if month_key != (0, 0) else "unknown",
                len(results),
            )

            for r in results:
                total_input_rows += r.get("input_rows", 0)
                total_output_rows_before_combine += r.get("output_rows", 0)
                total_month_mismatch += r.get("month_mismatch_count", 0)
                total_cleanup_removed += r.get("cleanup_removed", 0)
                total_parse_failures += r.get("parse_failures", 0)
                if r.get("intermediate_path"):
                    intermediate_paths.append(r["intermediate_path"])

            # Month-mismatch report (per month)
            month_total = sum(r.get("month_mismatch_count", 0) for r in results)
            if month_total > 0:
                logger.info("Month %s: %d row(s) with month mismatch", month_key, month_total)

        time_processing_seconds = time.perf_counter() - t_processing_start

    logger.info(
        "Month-mismatch total: %d (across all files)",
        total_month_mismatch,
    )

    # 5. Combine into single wide table
    t_combine_start = time.perf_counter()
    logger.info("Step 5/7: Combining intermediates into wide table...")
    wide = combine_into_wide_table(intermediate_paths)
    final_path = output_dir / "wide_table.parquet"
    logger.info("Step 6/7: Writing final wide table to %s", final_path)
    wide.to_parquet(
        final_path, index=True, engine="pyarrow", compression="snappy"
    )
    logger.info("Wrote final wide table to %s (%d rows)", final_path, len(wide))

    # 6. Upload to S3 if requested
    if args.s3_output:
        logger.info("Uploading to S3: %s", args.s3_output)
        try:
            upload_to_s3(str(final_path), args.s3_output)
        except Exception as e:
            logger.exception("S3 upload failed: %s", e)

    time_combine_output_seconds = time.perf_counter() - t_combine_start
    output_bytes = final_path.stat().st_size if final_path.exists() else 0

    # 7. Clean intermediates unless --keep-intermediate
    if not args.keep_intermediate:
        logger.info("Step 7/7: Removing %d intermediate file(s)...", len(intermediate_paths))
        for p in intermediate_paths:
            try:
                Path(p).unlink(missing_ok=True)
            except Exception as e:
                logger.warning("Could not remove %s: %s", p, e)
        # Remove empty intermediate dirs
        for month_key in month_keys:
            d = output_dir / "intermediates" / f"{month_key[0]}-{month_key[1]:02d}"
            if d.exists() and d.is_dir() and not any(d.iterdir()):
                d.rmdir()
        logger.info("Cleaned intermediate files and empty directories.")
    else:
        logger.info("Keeping %d intermediate file(s) (--keep-intermediate).", len(intermediate_paths))

    # 8. Report
    wall_elapsed = time.perf_counter() - wall_start
    peak_rss = _get_peak_rss_bytes()
    # Best-effort total input size for throughput (S3/local)
    input_bytes = 0
    try:
        for fp in files:
            try:
                input_bytes += get_file_size(fp, anon=s3_anon)
            except Exception:
                pass
    except Exception:
        pass
    cpu_count = os.cpu_count()
    logger.info(
        "Generating report (run_time=%.1fs, peak_rss=%.1f MB)...",
        wall_elapsed,
        peak_rss / (1024 * 1024),
    )
    bad_rows = total_parse_failures + total_cleanup_removed  # month mismatch is informational, not "dropped"
    failed_results = [r for r in all_results if r.get("error")]
    files_failed = len(failed_results)
    failed_files_list = [
        {"file_path": r.get("file_path", ""), "error": r.get("error", "")}
        for r in failed_results
    ]
    if files_failed:
        logger.info("Skipped %d file(s) (unreadable or error)", files_failed)
    # Always write failed_files.json (empty list if none)
    failed_path = output_dir / "failed_files.json"
    try:
        with open(failed_path, "w") as f:
            json.dump(failed_files_list, f, indent=2)
        if files_failed:
            logger.info("Wrote failed file list to %s", failed_path)
    except Exception as e:
        logger.warning("Could not write failed_files.json: %s", e)
    generate_report(
        input_row_count=total_input_rows,
        output_row_count=len(wide),
        bad_rows=bad_rows,
        month_mismatch_total=total_month_mismatch,
        cleanup_removed_total=total_cleanup_removed,
        parse_failures_total=total_parse_failures,
        peak_rss_bytes=peak_rss,
        run_time_seconds=wall_elapsed,
        report_path=args.report_output,
        json_path=args.report_json,
        files_failed=files_failed,
        failed_files=failed_files_list,
        time_discovery_seconds=time_discovery_seconds,
        time_processing_seconds=time_processing_seconds,
        time_combine_output_seconds=time_combine_output_seconds,
        workers_used=workers_used,
        cpu_count=cpu_count,
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        s3_output_uri=args.s3_output or "",
    )

    report_path_final = args.report_output  # set above (default output_dir/report.tex)
    logger.info(
        "Pipeline complete in %.1f min. Wide table: %s | Report: %s",
        wall_elapsed / 60.0,
        final_path,
        report_path_final,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
