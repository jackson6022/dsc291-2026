
## Part 1: Core Utilities

The module notes that Part 1 focuses on the core utilities (column detection, path inference, pivoting, and cleanup), which form the basis of the NYC TLC taxi data pivoting pipeline.

`pivot_utils.py` is a utility module for pivoting **NYC TLC (Taxi and Limousine Commission) taxi data**. It supports data discovery, column detection, and reshaping of trip-level records into a time-series format suitable for analysis.

---

## Core Utilities

### Column Detection

NYC TLC data uses different column names depending on the taxi type (yellow, green, FHV). These functions detect the correct columns in a DataFrame.

| Function | Description |
|----------|-------------|
| `find_pickup_datetime_col(df)` | Finds the pickup datetime column. Supports `tpep_pickup_datetime` (yellow), `lpep_pickup_datetime` (green), or `pickup_datetime` (generic). Matching is case-insensitive. Raises `ValueError` if none is found. |
| `find_pickup_location_col(df)` | Finds the pickup location column. Supports `PULocationID`, `pickup_location_id`, and similar variants. Falls back to dropoff location (`DOLocationID`) if pickup is not found. Raises `ValueError` if neither is found. |

### Path Inference

Many NYC TLC files encode taxi type and month in the file path (e.g., `yellow_tripdata_2023-01.parquet`). These functions extract that metadata from the path.

| Function | Description |
|----------|-------------|
| `infer_taxi_type_from_path(file_path)` | Extracts taxi type from the path. Supports `yellow`, `green`, `fhv`, and `fhvhv`. Returns lowercase string or `None` if not inferrable. |
| `infer_month_from_path(file_path)` | Extracts year and month from the path. Supports `YYYY-MM` (e.g., `2023-01`), `year=YYYY/month=MM` (partitioned), and `YYYY_MM`. Returns `(year, month)` tuple or `None`. |

### Pivoting

| Function | Description |
|----------|-------------|
| `pivot_counts_date_taxi_type_location(df)` | Pivots trip-level records into counts by (date × taxi_type × pickup_place × hour). Input needs a pickup datetime column, pickup location column, and `taxi_type`. Output has a MultiIndex `(taxi_type, date, pickup_place)` and columns `hour_0` through `hour_23`. Missing hours are filled with 0. |

### Cleanup

| Function | Description |
|----------|-------------|
| `cleanup_low_count_rows(df, min_rides=50)` | Removes rows with fewer than `min_rides` total rides (sum across hour columns). Returns a tuple of `(cleaned_df, stats)` where `stats` includes `rows_before`, `rows_after`, and `rows_dropped`. |

---

## Data Flow

A typical pipeline might use these utilities as follows:

1. **Load data** — Read parquet files into a DataFrame.
2. **Infer metadata** — Use `infer_taxi_type_from_path()` and `infer_month_from_path()` to get taxi type and month from each file path.
3. **Detect columns** — Use `find_pickup_datetime_col()` and `find_pickup_location_col()` to find the correct columns, then add `taxi_type` to the DataFrame.
4. **Pivot** — Call `pivot_counts_date_taxi_type_location()` to reshape trip records into hourly counts.
5. **Clean up** — Call `cleanup_low_count_rows()` to remove low-count rows before further analysis.

---
# DSC 291 - Part 2: S3 & File Discovery

## What Part 2 Asks For

From the assignment:

- **Path helpers**: `is_s3_path`, `get_storage_options`, `get_filesystem` — tell S3 from local paths, get storage config and a filesystem object (e.g. `s3fs` for S3, anonymous when needed).
- **Discovery**: `discover_parquet_files(input_path)` — one function that recursively finds all `.parquet` files for a given path and returns a **sorted list**, whether the path is **local or S3**.

---

## What This Code Does

The code in `pivot_and_bootstrap/pivot_utils.py` implements that: it discovers parquet files from either the local filesystem or an S3 bucket using the same API.

- **Path helpers**  
  `is_s3_path` detects `s3://` or `s3a://`. `get_storage_options` returns options (e.g. `anon=True` for public S3). `get_filesystem` returns `s3fs.S3FileSystem` for S3 or the local filesystem for disk paths.

- **Discovery**  
  `discover_parquet_files(input_path)` walks the path recursively (local: `rglob`, S3: `fs.find`), keeps only `.parquet` paths, sorts them, and returns the list. Same call works for `/data/taxi/` or `s3://bucket/prefix/`.

---

## How It Is Used in the Pipeline

Pipeline step 1 is **"Discover Parquet files (local dir or s3://...)"**. Part 2 is that step.

The main pipeline (e.g. Part 4's `pivot_all_files.py`) will:

1. Take an input path from the user (`--input-dir`), which may be local or S3.
2. Call `discover_parquet_files(input_path)` once to get the full list of parquet files.
3. Use that list for the rest of the pipeline: group by month, process each file (read → normalize → aggregate → pivot → cleanup), then combine into the final wide table.

So Part 2 is the **file-discovery layer**: it hides whether data lives on disk or S3 and gives the rest of the pipeline a single, sorted list of parquet paths to process.

---

## Quick Usage

```bash
# Setup
pip install -r requirements.txt

# Run tests
python3 -m pytest pivot_and_bootstrap/test_pivot_utils.py -v
```

```python
from pivot_and_bootstrap import discover_parquet_files

# Local or S3 — same call
files = discover_parquet_files('/data/taxi/')
files = discover_parquet_files('s3://nyc-tlc/trip data/', anon=True)
# → sorted list of .parquet paths
```

**Files**: `pivot_and_bootstrap/pivot_utils.py` (implementation), `pivot_and_bootstrap/test_pivot_utils.py` (tests).

---
# Part 3 — `partition_optimization.py`
## What Part 3 Asks For

From the assignment:

- **`parse_size(size_str)`** — parse human-readable size strings such as `"200MB"` or `"1.5GB"` into bytes.
- **`find_optimal_partition_size(...)`** — test candidate partition sizes (50MB–1GB), measure runtime and memory usage using PyArrow batching, and select the best size that stays within a specified memory limit.
- Support both **local files and S3 paths**.
- Use partitioning/batching to improve performance while avoiding out-of-memory errors.

---

## What This Code Does

The code in `pivot_and_bootstrap/partition_optimization.py` implements a lightweight batch-size tuning step for reading large Parquet files with PyArrow.

- **Size parsing**  
  `parse_size(size_str)` converts strings like `"512KB"`, `"200MB"`, or `"1.5GB"` into byte counts. It is case-insensitive, allows optional whitespace, and validates units and values.

- **Batch size benchmarking**  
  `find_optimal_partition_size(...)` selects a representative Parquet file (local or `s3://`) and estimates an approximate bytes-per-row ratio from Parquet metadata. Using this estimate, it converts the assignment’s byte-range targets (default 50MB–1GB) into a small set of candidate **row-based batch sizes**.

  Each candidate is tested by reading a few batches with `ParquetFile.iter_batches`, while measuring wall-clock time and peak resident memory usage (RSS). Candidates that exceed the configured memory limit are rejected, and among the remaining candidates the fastest option is selected. The final result is returned as an approximate batch size in bytes and logged for transparency.

---

## How It Is Used in the Pipeline

Partition optimization is an **optional performance step** that runs before large-scale Parquet processing.

In the main pipeline (Part 4), the flow is:

1. Discover all Parquet files (Part 2).
2. Optionally call `find_optimal_partition_size(...)` on a sample file to determine an efficient batch size.
3. Use the selected batch size when reading Parquet files during month-by-month processing.
4. Skip this step entirely if the user passes `--skip-partition-optimization`.

This allows the pipeline to scale to large datasets while keeping memory usage within bounds and improving overall throughput.

---

## Quick Usage

```python
from pivot_and_bootstrap.partition_optimization import (
    parse_size,
    find_optimal_partition_size,
)

# Parse human-readable sizes
parse_size("200MB")    # -> bytes
parse_size("1.5GB")    # -> bytes

# Find an optimal batch size for Parquet reads
batch_bytes = find_optimal_partition_size(
    parquet_paths="s3://nyc-tlc/trip-data/yellow_tripdata_2023-01.parquet",
    max_memory_usage_mb=4096,
)
```
**Files**: `pivot_and_bootstrap/partition_optimization.py` (implementation), `pivot_and_bootstrap/test_pivot_utils.py` (tests).


# Part 4: Main Pipeline — `pivot_all_files.py`

## What Part 4 Asks For

From the assignment:

- **`process_single_file`**: Read Parquet (local/S3) → infer expected month from path → normalize → aggregate by `(date, taxi_type, pickup_place, hour)` → pivot → **discard rows with < 50 rides** → write intermediate Parquet. **Count rows whose month ≠ file's expected month**; return this count (and any per-file stats) along with metadata.
- **Month-at-a-time processing**: Group discovered files by `(year, month)`. Process **one month at a time** (e.g. all files for 2023-01, then 2023-02). Within a month, parallelize across files if desired.
- **Month-mismatch reporting**: Aggregate and **report** the number of rows where the row's month does not match the Parquet file's expected month. Include at least a **total** across all files; optionally also per-file and/or per-month breakdown.
- **`combine_into_wide_table`**: Read all intermediates → aggregate by `(taxi_type, date, pickup_place)`, sum hour columns → produce **a single wide table** indexed by **taxi_type, date, pickup_place** for **all available data** → **store as Parquet** (final output).
- **Step 5 — Generate report**: Produce a **report** with: **input row count**, **output row count**, **bad rows ignored**, **memory use** (e.g. peak RSS), and **run time** (wall-clock). Output to a small .tex file (and optional JSON).
- **CLI `main()`**: `--input-dir`, `--output-dir`, `--min-rides` (default 50), `--workers`, `--partition-size` / `--skip-partition-optimization`, `--keep-intermediate`. Run discovery → group by month → (optional) partition optimization → **process one month at a time** (parallel within month) → **report month-mismatch counts** → **combine into single wide table** → **store as Parquet** → **upload to S3** → **generate report**. Use multiprocessing, tqdm, continue on per-file errors.

---

## What This Code Does

The script `pivot_and_bootstrap/pivot_all_files.py` implements the full pipeline:

1. **Discover** — Uses Part 2's `discover_parquet_files()` to get a sorted list of Parquet files (local or S3).
2. **Schema check** — Samples files and normalizes to a common schema (pickup datetime, pickup location).
3. **Group by month** — Infers `(year, month)` from path (e.g. `yellow_tripdata_2023-01.parquet` → 2023-01).
4. **Process** — For each file (or in parallel via `--workers` or `--parallel-files`): read → normalize → aggregate by `(date, taxi_type, pickup_place, hour)` → pivot → drop rows with < `--min-rides` (default 50) → write intermediate Parquet. Counts and reports month-mismatch rows.
5. **Combine** — Reads all intermediates, aggregates by `(taxi_type, date, pickup_place)`, sums hour columns → one wide table.
6. **Write & S3** — Saves `wide_table.parquet` under `--output-dir`, then uploads to S3 (default or `DSC291_S3_OUTPUT` / `--s3-output`).
7. **Report** — Writes `report.tex` (and optional `--report-json`) with row counts, bad rows, peak RSS, run time, resource utilization, and run-time breakdown.

**S3 location of the single Parquet (wide) table:** `s3://291-s3-bucket/wide.parquet`

---

## Quick Usage

```bash
# From repo root — defaults: input s3://dsc291-ucsd/taxi, output ./pivot_and_bootstrap, upload to s3://291-s3-bucket/wide.parquet
python pivot_and_bootstrap/pivot_all_files.py

# With 8 workers (e.g. on r8i.4xlarge)
python pivot_and_bootstrap/pivot_all_files.py --parallel-files 8

# Override paths
python pivot_and_bootstrap/pivot_all_files.py --input-dir /data/parquet --output-dir ./out --s3-output s3://my-bucket/wide.parquet
```

**Files**: `pivot_and_bootstrap/pivot_all_files.py` (main pipeline), `pivot_and_bootstrap/pivot_utils.py` (Part 1 & 2), `pivot_and_bootstrap/partition_optimization.py` (optional). See `pivot_and_bootstrap/README.md` for full CLI options and examples.
