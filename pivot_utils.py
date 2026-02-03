# for all NYC TLC data
import dask.dataframe as dd
import pandas as pd
import numpy as np
from pathlib import Path
import glob
import re
from typing import Optional

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
    ("yellow", re.compile(r"yellow[\W_]trip", re.IGNORECASE)),
    ("green", re.compile(r"green[\W_]trip", re.IGNORECASE)),
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


def find_pickup_datetime_col(columns: list) -> Optional[str]:
    """
    Find the pickup datetime column from a list of column names.

    Handles common NYC TLC variants: tpep_pickup_datetime (yellow),
    lpep_pickup_datetime (green), pickup_datetime. Matching is case-insensitive.

    Parameters
    ----------
    columns : list
        List of column names (e.g. df.columns.tolist()).

    Returns
    -------
    str or None
        The actual column name if found, else None.
    """
    return _match_column(columns, PICKUP_DATETIME_VARIANTS)


def find_pickup_location_col(columns: list) -> Optional[str]:
    """
    Find the pickup location column from a list of column names.

    Handles common variants: PULocationID, pickup_location_id, pu_location_id,
    pickup_location. Matching is case-insensitive.

    Parameters
    ----------
    columns : list
        List of column names.

    Returns
    -------
    str or None
        The actual column name if found, else None.
    """
    return _match_column(columns, PICKUP_LOCATION_VARIANTS)


def infer_taxi_type_from_path(file_path: str) -> Optional[str]:
    """
    Infer taxi type (e.g. 'yellow', 'green') from file path.

    Looks for patterns like yellow_tripdata, green_tripdata. Case-insensitive.

    Parameters
    ----------
    file_path : str
        Path or filename (e.g. .../yellow_tripdata_2023-01.parquet).

    Returns
    -------
    str or None
        'yellow', 'green', or None if not inferrable.
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
    Pivot aggregated counts to wide form: (taxi_type, date, pickup_place) index,
    columns hour_0..hour_23, missing filled with 0.

    Expects pdf to have columns: taxi_type, date, pickup_place, hour, and a count
    column (e.g. 'count' or 'rides'). If multiple count columns exist, one named
    'count' or 'rides' is used; otherwise the last numeric column is used.

    Parameters
    ----------
    pdf : pandas.DataFrame
        Long-format dataframe with at least: taxi_type, date, pickup_place, hour, and counts.

    Returns
    -------
    pandas.DataFrame
        Index: (taxi_type, date, pickup_place). Columns: hour_0..hour_23. Values: counts (0 where missing).
    """
    required = {"taxi_type", "date", "pickup_place", "hour"}
    cols = set(pdf.columns)
    if not required.issubset(cols):
        missing = required - cols
        raise ValueError(f"DataFrame must have columns {required}; missing: {missing}")

    # Identify count column
    count_col = None
    for c in ("count", "rides", "n"):
        if c in pdf.columns and pd.api.types.is_numeric_dtype(pdf[c]):
            count_col = c
            break
    if count_col is None:
        numeric = pdf.select_dtypes(include=["number"]).columns.tolist()
        if numeric:
            count_col = numeric[-1]
        else:
            raise ValueError("No numeric count column found (looked for 'count', 'rides', 'n')")

    # Pivot: rows = (taxi_type, date, pickup_place), columns = hour, values = count_col
    piv = pdf.pivot_table(
        index=["taxi_type", "date", "pickup_place"],
        columns="hour",
        values=count_col,
        aggfunc="sum",
        fill_value=0,
    )

    # Rename hour columns to hour_0..hour_23 and ensure all 0..23 present
    hour_cols = [f"hour_{h}" for h in range(24)]
    for h in range(24):
        if h not in piv.columns:
            piv[h] = 0
    piv = piv[[h for h in range(24)]].copy()
    piv.columns = hour_cols

    return piv.reset_index()


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
