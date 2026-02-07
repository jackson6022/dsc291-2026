# for all NYC TLC data
#import dask.dataframe as dd
import pandas as pd
import numpy as np
from pathlib import Path
import glob
import re
import os
import logging
from typing import List, Optional, Dict, Any, Union
from urllib.parse import urlparse

"""
Core utilities for pivoting NYC TLC taxi trip data.

Handles column detection (pickup datetime, pickup location, taxi type from path),
month inference from file paths, pivoting to (taxi_type, date, pickup_place) × hour_0..hour_23,
and cleaning low-count rows.
"""



# --- Column name variants (case-insensitive) ---

PICKUP_DATETIME_VARIANTS = [
    "tpep_pickup_datetime",   # Yellow taxi
    "lpep_pickup_datetime",   # Green taxi
    "pickup_datetime",
]

PICKUP_LOCATION_VARIANTS = [
    "PULocationID",
    "pickup_location_id",
    "pu_location_id",
    "pickup_location",
    "PULocation",
]

TAXI_TYPE_PATH_PATTERNS = [
    # Order matters: check longer patterns first (fhvhv before fhv)
    # Patterns match: fhvhv_tripdata, fhvhv_2023, fhvhv-tripdata, etc.
    ("fhvhv", re.compile(r"fhvhv[\W_]", re.IGNORECASE)),
    # Patterns match: fhv_tripdata, fhv_2023, fhv-tripdata, etc.
    ("fhv", re.compile(r"fhv[\W_]", re.IGNORECASE)),
    # Patterns match: yellow_tripdata, yellow_2023, yellow-tripdata, etc.
    ("yellow", re.compile(r"yellow[\W_]", re.IGNORECASE)),
    # Patterns match: green_tripdata, green_2023, green-tripdata, etc.
    ("green", re.compile(r"green[\W_]", re.IGNORECASE)),
]

# Year-month in path: 2023-01 or year=2023/month=01 (or month=01)
MONTH_IN_PATH_PATTERN = re.compile(
    r"year[=_]?(\d{4})[\/_]?month[=_]?(\d{1,2})"  # year=2023/month=01
    r"|"
    r"(\d{4})[\-_](\d{1,2})(?=[\-_.]|\.parquet)"   # 2023-01 or 2023_01
)


def _normalize_col_candidate(name: str) -> str:
    """Normalize for case-insensitive comparison (strip, lower)."""
    return (name or "").strip().lower()


def _match_column(columns: list, variants: list[str]) -> Optional[str]:
    """Return first column that matches any variant (case-insensitive)."""
    col_set = {c: _normalize_col_candidate(c) for c in columns}
    for v in variants:
        vn = _normalize_col_candidate(v)
        for col, norm in col_set.items():
            if norm == vn:
                return col
    return None


def find_pickup_datetime_col(columns: Union[list, pd.DataFrame]) -> str:
    """
    Find the pickup datetime column from a list of column names or DataFrame.

    Handles common NYC TLC variants: tpep_pickup_datetime (yellow),
    lpep_pickup_datetime (green), pickup_datetime. Matching is case-insensitive.

    Parameters
    ----------
    columns : list or pandas.DataFrame
        List of column names (e.g. df.columns.tolist()) or a DataFrame.

    Returns
    -------
    str
        The actual column name if found.

    Raises
    ------
    ValueError
        If no matching datetime column is found.
    """
    # Handle DataFrame input
    if isinstance(columns, pd.DataFrame):
        columns = columns.columns.tolist()
    
    result = _match_column(columns, PICKUP_DATETIME_VARIANTS)
    if result is None:
        raise ValueError(
            f"Could not find pickup datetime column. "
            f"Expected one of: {PICKUP_DATETIME_VARIANTS}. "
            f"Available columns: {columns}"
        )
    return result


def find_pickup_location_col(columns: Union[list, pd.DataFrame]) -> str:
    """
    Find the pickup location column from a list of column names or DataFrame.

    Handles common variants: PULocationID, pickup_location_id, pu_location_id,
    pickup_location. Matching is case-insensitive.
    
    If pickup location column is not found, falls back to dropoff location column
    (DOLocationID, etc.) if available.

    Parameters
    ----------
    columns : list or pandas.DataFrame
        List of column names or a DataFrame.

    Returns
    -------
    str
        The actual column name if found.

    Raises
    ------
    ValueError
        If no matching location column is found.
    """
    # Handle DataFrame input
    if isinstance(columns, pd.DataFrame):
        columns = columns.columns.tolist()
    
    result = _match_column(columns, PICKUP_LOCATION_VARIANTS)
    
    # Fallback to dropoff location if pickup not found
    if result is None:
        dropoff_variants = ["DOLocationID", "dropoff_location_id", "do_location_id", 
                           "dropoff_location", "DOLocation"]
        result = _match_column(columns, dropoff_variants)
    
    if result is None:
        raise ValueError(
            f"Could not find pickup or dropoff location column. "
            f"Expected one of: {PICKUP_LOCATION_VARIANTS}. "
            f"Available columns: {columns}"
        )
    return result


def infer_taxi_type_from_path(file_path: str) -> Optional[str]:
    """
    Infer taxi type (e.g. 'yellow', 'green', 'fhv', 'fhvhv') from file path.

    Looks for patterns like yellow_tripdata, green_tripdata, fhv_tripdata, 
    fhvhv_tripdata. Case-insensitive. Checks longer patterns first (fhvhv 
    before fhv) to avoid false matches.

    Parameters
    ----------
    file_path : str
        Path or filename (e.g. .../yellow_tripdata_2023-01.parquet).

    Returns
    -------
    str or None
        'yellow', 'green', 'fhv', 'fhvhv', or None if not inferrable.
    """
    path = (file_path or "").strip()
    for taxi_type, pattern in TAXI_TYPE_PATH_PATTERNS:
        if pattern.search(path):
            return taxi_type
    return None


def infer_month_from_path(file_path: str) -> Optional[tuple[int, int]]:
    """
    Infer (year, month) from file path for grouping and month-mismatch checks.

    Supports:
    - ...2023-01... or ...2023_01... (year, month) before .parquet or next delimiter
    - ...year=2023/month=01... or year=2023_month=01

    Parameters
    ----------
    file_path : str
        Path or filename (e.g. yellow_tripdata_2023-01.parquet or .../year=2023/month=01/...).

    Returns
    -------
    tuple of (year, month) or None
        (year, month) as integers (e.g. (2023, 1)), or None when not inferrable.
    """
    path = (file_path or "").strip()
    m = MONTH_IN_PATH_PATTERN.search(path)
    if not m:
        return None
    # Group 1,2: year=.../month=...; Group 3,4: 2023-01 style
    g1, g2, g3, g4 = m.group(1), m.group(2), m.group(3), m.group(4)
    if g1 is not None and g2 is not None:
        year, month = int(g1), int(g2)
    elif g3 is not None and g4 is not None:
        year, month = int(g3), int(g4)
    else:
        return None
    if 1 <= month <= 12 and 2000 <= year <= 2100:
        return (year, month)
    return None


def pivot_counts_date_taxi_type_location(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot trip-level data to wide form: (taxi_type, date, pickup_place) index,
    columns hour_0..hour_23, missing filled with 0.

    Takes a pandas DataFrame (pdf) with raw trip data and pivots it to a wide format
    with counts per hour for each (taxi_type, date, pickup_place) combination.

    Parameters
    ----------
    pdf : pandas.DataFrame
        Raw trip data DataFrame with:
        - taxi_type: column with taxi type (yellow, green, fhv, fhvhv)
        - datetime column: pickup datetime (tpep_pickup_datetime, lpep_pickup_datetime, 
          or pickup_datetime) - automatically detected
        - location column: pickup location (PULocationID, pickup_location_id, etc.) 
          - automatically detected

    Returns
    -------
    pandas.DataFrame
        DataFrame with:
        - Index: (taxi_type, date, pickup_place) as MultiIndex
        - Columns: hour_0, hour_1, ..., hour_23 (24 columns)
        - Values: trip counts per hour (0 where missing)
    """
    # Make a copy to avoid modifying the original
    pdf = pdf.copy()
    
    # Ensure taxi_type exists
    if 'taxi_type' not in pdf.columns:
        raise ValueError("DataFrame must have 'taxi_type' column")
    
    # Detect datetime and location columns
    datetime_col = find_pickup_datetime_col(pdf)
    location_col = find_pickup_location_col(pdf)
    
    # Extract date and hour from datetime column
    pdf['date'] = pd.to_datetime(pdf[datetime_col]).dt.date
    pdf['hour'] = pd.to_datetime(pdf[datetime_col]).dt.hour
    pdf['pickup_place'] = pdf[location_col]
    
    # Aggregate: count trips by (taxi_type, date, pickup_place, hour)
    # Each row in the input is one trip, so we count rows
    aggregated = pdf.groupby(['taxi_type', 'date', 'pickup_place', 'hour']).size().reset_index(name='count')
    
    # Pivot the aggregated data to wide format
    piv = aggregated.pivot_table(
        index=['taxi_type', 'date', 'pickup_place'],
        columns='hour',
        values='count',
        aggfunc='sum',
        fill_value=0,
    )

    # Rename hour columns to hour_0..hour_23 and ensure all 0..23 present
    hour_cols = [f"hour_{h}" for h in range(24)]
    
    # Handle case where pivot_table creates MultiIndex columns (from 'hour' column)
    if isinstance(piv.columns, pd.MultiIndex):
        # Extract the hour values from the MultiIndex
        piv.columns = piv.columns.get_level_values(-1)
    
    # Ensure all hours 0-23 are present as columns
    for h in range(24):
        if h not in piv.columns:
            piv[h] = 0
    
    # Select and reorder columns to ensure 0-23 order
    piv = piv[[h for h in range(24)]].copy()
    piv.columns = hour_cols
    
    # Ensure index is MultiIndex with correct names
    # pivot_table should already create MultiIndex when using list for index
    if not isinstance(piv.index, pd.MultiIndex):
        raise ValueError("Expected MultiIndex from pivot_table, but got regular index")
    
    # Set index names explicitly
    piv.index.names = ['taxi_type', 'date', 'pickup_place']

    # Keep MultiIndex (don't reset_index) as tests expect
    return piv


def cleanup_low_count_rows(
    df: pd.DataFrame,
    min_rides: int = 50,
) -> tuple[pd.DataFrame, dict]:
    """
    Discard rows with fewer than min_rides (sum across hour_0..hour_23).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with index or columns (taxi_type, date, pickup_place) and hour_0..hour_23.
    min_rides : int
        Minimum total rides to keep a row (default 50).

    Returns
    -------
    tuple of (cleaned_df, stats)
        cleaned_df : DataFrame with low-count rows removed.
        stats : dict with keys such as 'rows_before', 'rows_after', 'rows_dropped', 'rides_dropped'.
    """
    hour_cols = [f"hour_{h}" for h in range(24)]
    missing = [c for c in hour_cols if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame must have columns {hour_cols}; missing: {missing}")

    total_rides = df[hour_cols].sum(axis=1)
    rows_before = len(df)
    mask = total_rides >= min_rides
    cleaned = df.loc[mask].copy()
    rows_after = len(cleaned)
    rows_dropped = rows_before - rows_after
    rides_dropped = int(total_rides.loc[~mask].sum())

    stats = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_dropped": rows_dropped,
        "rides_dropped": rides_dropped,
    }
    return cleaned, stats


"""
S3 & File Discovery Utilities

This module provides utilities for working with both local and S3 file systems,
including path detection, filesystem abstraction, and recursive parquet file discovery.

Part 2 of the Taxi Data Pivoting Pipeline (15 pts)
"""

import os
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from urllib.parse import urlparse

# Configure logging
logger = logging.getLogger(__name__)


def is_s3_path(path: str) -> bool:
    """
    Check if a given path is an S3 path.
    
    Args:
        path: A file or directory path string.
        
    Returns:
        True if the path is an S3 path (starts with 's3://' or 's3a://'), 
        False otherwise.
        
    Examples:
        >>> is_s3_path('s3://my-bucket/data/')
        True
        >>> is_s3_path('s3a://my-bucket/data/')
        True
        >>> is_s3_path('/local/path/to/data')
        False
        >>> is_s3_path('./relative/path')
        False
    """
    if not isinstance(path, str):
        return False
    
    path_lower = path.lower().strip()
    return path_lower.startswith('s3://') or path_lower.startswith('s3a://')


def parse_s3_path(path: str) -> tuple[str, str]:
    """
    Parse an S3 path into bucket and key components.
    
    Args:
        path: An S3 path (e.g., 's3://bucket/prefix/file.parquet')
        
    Returns:
        Tuple of (bucket_name, key/prefix)
        
    Raises:
        ValueError: If the path is not a valid S3 path.
        
    Examples:
        >>> parse_s3_path('s3://my-bucket/data/file.parquet')
        ('my-bucket', 'data/file.parquet')
        >>> parse_s3_path('s3://my-bucket/')
        ('my-bucket', '')
    """
    if not is_s3_path(path):
        raise ValueError(f"Not a valid S3 path: {path}")
    
    # Handle both s3:// and s3a:// prefixes
    parsed = urlparse(path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    
    return bucket, key


def get_storage_options(path: str, anon: Optional[bool] = None) -> Dict[str, Any]:
    """
    Get storage options for reading/writing to a given path.
    
    For S3 paths, returns appropriate options for s3fs/pyarrow.
    For local paths, returns an empty dict.
    
    Args:
        path: A file or directory path (local or S3).
        anon: If True, use anonymous access for S3. If None, defaults to True
              for public buckets (will attempt anonymous first).
              
    Returns:
        Dictionary of storage options suitable for pandas/pyarrow S3 operations.
        
    Examples:
        >>> get_storage_options('/local/path')
        {}
        >>> get_storage_options('s3://nyc-tlc/trip data/', anon=True)
        {'anon': True}
    """
    if not is_s3_path(path):
        return {}
    
    options: Dict[str, Any] = {}
    
    # Default to anonymous access for public buckets
    if anon is None:
        # Try anonymous first - common for public datasets
        options['anon'] = True
    else:
        options['anon'] = anon
    
    return options


def get_filesystem(path: str, anon: Optional[bool] = None):
    """
    Get an appropriate filesystem object for the given path.
    
    Args:
        path: A file or directory path (local or S3).
        anon: If True, use anonymous access for S3. If None, attempts
              anonymous access first.
              
    Returns:
        For S3 paths: an s3fs.S3FileSystem instance
        For local paths: a pyarrow.fs.LocalFileSystem or fsspec LocalFileSystem
        
    Raises:
        ImportError: If required packages (s3fs, fsspec) are not installed.
        
    Examples:
        >>> fs = get_filesystem('s3://nyc-tlc/trip data/')
        >>> fs = get_filesystem('/local/data/')
    """
    if is_s3_path(path):
        try:
            import s3fs
        except ImportError:
            raise ImportError(
                "s3fs is required for S3 operations. "
                "Install with: pip install s3fs"
            )
        
        # Determine anonymous access setting
        use_anon = anon if anon is not None else True
        
        logger.debug(f"Creating S3FileSystem with anon={use_anon}")
        return s3fs.S3FileSystem(anon=use_anon)
    
    else:
        # For local filesystem, use fsspec's LocalFileSystem
        try:
            import fsspec
            return fsspec.filesystem('file')
        except ImportError:
            # Fallback to basic approach if fsspec not available
            try:
                from pyarrow.fs import LocalFileSystem
                return LocalFileSystem()
            except ImportError:
                raise ImportError(
                    "Either fsspec or pyarrow is required for filesystem operations. "
                    "Install with: pip install fsspec or pip install pyarrow"
                )


def _discover_local_parquet_files(input_path: str) -> List[str]:
    """
    Recursively discover all parquet files in a local directory.
    
    Args:
        input_path: Local directory or file path.
        
    Returns:
        Sorted list of absolute paths to parquet files.
    """
    path = Path(input_path).resolve()
    parquet_files: List[str] = []
    
    if path.is_file():
        if path.suffix.lower() == '.parquet':
            parquet_files.append(str(path))
    elif path.is_dir():
        # Recursive glob for all .parquet files
        for parquet_path in path.rglob('*.parquet'):
            if parquet_path.is_file():
                parquet_files.append(str(parquet_path))
    else:
        logger.warning(f"Path does not exist: {input_path}")
    
    # Sort for consistent ordering
    parquet_files.sort()
    
    logger.info(f"Discovered {len(parquet_files)} parquet file(s) in {input_path}")
    return parquet_files


def _discover_s3_parquet_files(input_path: str, anon: Optional[bool] = None) -> List[str]:
    """
    Recursively discover all parquet files in an S3 location.
    
    Args:
        input_path: S3 path (e.g., 's3://bucket/prefix/').
        anon: If True, use anonymous access.
        
    Returns:
        Sorted list of S3 paths to parquet files (with s3:// prefix).
    """
    fs = get_filesystem(input_path, anon=anon)
    bucket, prefix = parse_s3_path(input_path)
    
    parquet_files: List[str] = []
    
    # Construct the full S3 path for listing
    s3_path = f"{bucket}/{prefix}" if prefix else bucket
    
    try:
        # Check if it's a single file
        if s3_path.lower().endswith('.parquet'):
            if fs.exists(s3_path):
                parquet_files.append(f"s3://{s3_path}")
        else:
            # List all files recursively
            all_files = fs.find(s3_path)
            
            for file_path in all_files:
                if file_path.lower().endswith('.parquet'):
                    # Ensure proper s3:// prefix
                    if not file_path.startswith('s3://'):
                        file_path = f"s3://{file_path}"
                    parquet_files.append(file_path)
                    
    except Exception as e:
        logger.error(f"Error listing S3 path {input_path}: {e}")
        raise
    
    # Sort for consistent ordering
    parquet_files.sort()
    
    logger.info(f"Discovered {len(parquet_files)} parquet file(s) in {input_path}")
    return parquet_files


def discover_parquet_files(
    input_path: str,
    anon: Optional[bool] = None
) -> List[str]:
    """
    Recursively discover all parquet files in a local directory or S3 location.
    
    This function handles both local filesystem paths and S3 paths transparently.
    For S3 paths, it uses s3fs for efficient listing.
    
    Args:
        input_path: Path to a directory or file. Can be:
            - Local path: '/path/to/data/' or './relative/path/'
            - S3 path: 's3://bucket/prefix/' or 's3a://bucket/prefix/'
        anon: For S3 paths, whether to use anonymous access. 
              Defaults to True for public buckets.
              
    Returns:
        Sorted list of paths to parquet files. For S3, paths include 
        the 's3://' prefix. For local, paths are absolute.
        
    Raises:
        ValueError: If the input path is empty or invalid.
        FileNotFoundError: If a local path doesn't exist.
        ImportError: If required packages for S3 are not installed.
        
    Examples:
        >>> # Local discovery
        >>> files = discover_parquet_files('/data/taxi/')
        >>> print(files)
        ['/data/taxi/yellow_2023-01.parquet', '/data/taxi/yellow_2023-02.parquet']
        
        >>> # S3 discovery (public bucket)
        >>> files = discover_parquet_files('s3://nyc-tlc/trip data/')
        >>> print(files[:2])
        ['s3://nyc-tlc/trip data/yellow_tripdata_2023-01.parquet', ...]
    """
    if not input_path or not isinstance(input_path, str):
        raise ValueError("input_path must be a non-empty string")
    
    input_path = input_path.strip()
    
    # Check again after stripping whitespace
    if not input_path:
        raise ValueError("input_path must be a non-empty string")
    
    logger.debug(f"Discovering parquet files in: {input_path}")
    
    if is_s3_path(input_path):
        return _discover_s3_parquet_files(input_path, anon=anon)
    else:
        # Local path
        resolved_path = Path(input_path).resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(f"Local path does not exist: {input_path}")
        return _discover_local_parquet_files(input_path)


def validate_parquet_file(path: str, anon: Optional[bool] = None) -> bool:
    """
    Validate that a file exists and is a valid parquet file.
    
    Args:
        path: Path to the parquet file (local or S3).
        anon: For S3 paths, whether to use anonymous access.
        
    Returns:
        True if the file exists and appears to be a valid parquet file,
        False otherwise.
    """
    try:
        import pyarrow.parquet as pq
        
        if is_s3_path(path):
            fs = get_filesystem(path, anon=anon)
            bucket, key = parse_s3_path(path)
            full_path = f"{bucket}/{key}"
            
            # Try to read metadata only (fast validation)
            with fs.open(full_path, 'rb') as f:
                pq.read_metadata(f)
        else:
            # Local file
            if not Path(path).exists():
                return False
            pq.read_metadata(path)
        
        return True
        
    except Exception as e:
        logger.warning(f"Parquet validation failed for {path}: {e}")
        return False


# Convenience function for getting file size
def get_file_size(path: str, anon: Optional[bool] = None) -> int:
    """
    Get the size of a file in bytes.
    
    Args:
        path: Path to the file (local or S3).
        anon: For S3 paths, whether to use anonymous access.
        
    Returns:
        File size in bytes.
        
    Raises:
        FileNotFoundError: If the file doesn't exist.
    """
    if is_s3_path(path):
        fs = get_filesystem(path, anon=anon)
        bucket, key = parse_s3_path(path)
        full_path = f"{bucket}/{key}"
        info = fs.info(full_path)
        return info.get('size', info.get('Size', 0))
    else:
        return Path(path).stat().st_size


if __name__ == "__main__":
    # Simple test/demo when run directly
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) > 1:
        test_path = sys.argv[1]
    else:
        test_path = "."
    
    print(f"\nTesting path: {test_path}")
    print(f"Is S3 path: {is_s3_path(test_path)}")
    
    try:
        files = discover_parquet_files(test_path)
        print(f"\nDiscovered {len(files)} parquet file(s):")
        for f in files[:10]:  # Show first 10
            print(f"  - {f}")
        if len(files) > 10:
            print(f"  ... and {len(files) - 10} more")
    except Exception as e:
        print(f"Error: {e}")