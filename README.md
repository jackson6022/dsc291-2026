
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

---

# Part 5: Testing

## Overview

The test suite validates all core functionality of the NYC TLC taxi data pivoting pipeline (Part 5 of Homework Assignment 1).

**Test file**: `pivot_and_bootstrap/test_pivot_date_location_hour.py` (30+ test cases)

## Test Coverage

### 1. Column Detection Tests (`TestColumnDetection`)
Tests for finding pickup datetime, location, and taxi type from various column name formats:
- Standard variants: `tpep_pickup_datetime`, `lpep_pickup_datetime`, `PULocationID`
- Case-insensitive matching
- Error handling for missing columns
- Fallback behavior (e.g., using dropoff location if pickup not found)
- Taxi type inference from paths: yellow, green, fhv, fhvhv

### 2. Month Inference Tests (`TestMonthInference`)
Tests for extracting year and month from file paths:
- Hyphenated format: `yellow_tripdata_2023-01.parquet` → `(2023, 1)`
- Partitioned format: `year=2023/month=01/data.parquet` → `(2023, 1)`
- Underscore format: `yellow_2023_01_data.parquet` → `(2023, 1)`
- Handling non-inferrable paths

### 3. Pivot Function Tests (`TestPivotFunction`)
Tests for pivoting trip data into time-series format:
- Correct multi-index structure: `(taxi_type, date, pickup_place)`
- All 24 hour columns present: `hour_0` through `hour_23`
- Missing hours filled with 0
- Correct aggregation of counts
- Multiple dates and taxi types handled correctly

### 4. Cleanup Tests (`TestCleanupLowCount`)
Tests for removing rows with low ride counts:
- Rows with < 50 rides removed
- Rows with exactly 50 rides kept
- Statistics returned (rows before/after, rows dropped)
- Different thresholds tested
- Multi-index structure preserved

### 5. Error Handling Tests (`TestErrorHandling`)
Tests for graceful error handling:
- Missing required columns
- Null/invalid datetime values
- Invalid location IDs (negative numbers)
- Empty DataFrames

### 6. Month Mismatch Tests (`TestMonthMismatch`)
Tests for counting date inconsistencies:
- Rows where parsed date month ≠ file month
- Cases with no mismatches

### 7. Integration Test (`TestIntegration`)
End-to-end test with sample Parquet file:
- Create sample data
- Write to Parquet
- Read back and process
- Test all functions in sequence
- Verify final output

## Running the Tests

### Prerequisites

Install required packages:

```bash
pip install pytest pandas numpy pyarrow
```

### Running All Tests

```bash
# From the pivot_and_bootstrap directory
pytest test_pivot_date_location_hour.py -v

# Or with more detailed output
pytest test_pivot_date_location_hour.py -v --tb=short

# Or run from the file directly
python test_pivot_date_location_hour.py
```

### Running Specific Test Classes

```bash
# Test only column detection
pytest test_pivot_date_location_hour.py::TestColumnDetection -v

# Test only pivot function
pytest test_pivot_date_location_hour.py::TestPivotFunction -v

# Test only cleanup
pytest test_pivot_date_location_hour.py::TestCleanupLowCount -v
```

### Running Specific Tests

```bash
# Test a specific function
pytest test_pivot_date_location_hour.py::TestColumnDetection::test_find_pickup_datetime_col_standard -v
```

## Expected Output

When all tests pass, you should see:

```
test_pivot_date_location_hour.py::TestColumnDetection::test_find_pickup_datetime_col_standard PASSED
test_pivot_date_location_hour.py::TestColumnDetection::test_find_pickup_datetime_col_case_insensitive PASSED
test_pivot_date_location_hour.py::TestColumnDetection::test_find_pickup_location_col_standard PASSED
...
================================ 30 passed in 2.5s ================================
```

---

# Setup and Usage

## Installation

```bash
# Clone repository
git clone <repository-url>
cd dsc291-2026

# Install dependencies
pip install -r requirements.txt
```

## Running the Pipeline

```bash
# From repo root — defaults: input s3://dsc291-ucsd/taxi, output ./pivot_and_bootstrap, upload to s3://291-s3-bucket/wide.parquet
python pivot_and_bootstrap/pivot_all_files.py

# With 8 workers (e.g. on r8i.4xlarge)
python pivot_and_bootstrap/pivot_all_files.py --parallel-files 8

# Override paths
python pivot_and_bootstrap/pivot_all_files.py --input-dir /data/parquet --output-dir ./out --s3-output s3://my-bucket/wide.parquet
```

## CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--input-dir` | Path to input Parquet files (local or S3) | `s3://dsc291-ucsd/taxi` |
| `--output-dir` | Directory for output files | `./pivot_and_bootstrap` |
| `--s3-output` | S3 path for final wide table | `s3://291-s3-bucket/wide.parquet` |
| `--min-rides` | Minimum rides threshold (discard rows below this) | `50` |
| `--parallel-files` / `--workers` | Number of parallel workers | `4` |
| `--partition-size` | Manual partition size (e.g., `200MB`) | Auto-optimized |
| `--skip-partition-optimization` | Skip partition size optimization | False |
| `--keep-intermediate` | Keep intermediate monthly Parquet files | False |
| `--report-json` | Output JSON report in addition to .tex | False |

---

# Troubleshooting

## Common Issues

### Out of Memory Errors
- Reduce `--parallel-files` to use fewer workers
- Enable partition optimization (don't use `--skip-partition-optimization`)
- Process smaller batches by reducing `--partition-size`

### S3 Access Errors
- Ensure AWS credentials are configured: `aws configure`
- For public buckets, anonymous access is enabled by default
- Set environment variable: `export AWS_PROFILE=<your-profile>`

### Missing Columns
- The pipeline auto-detects column name variants (case-insensitive)
- Supports: `tpep_pickup_datetime`, `lpep_pickup_datetime`, `pickup_datetime`
- Supports: `PULocationID`, `pickup_location_id`, `DOLocationID` (fallback)

### Date/Month Mismatches
- Pipeline reports month-mismatch counts automatically
- Check logs for per-file breakdown
- Rows with incorrect months are processed but counted as mismatches

---

# Output and Deliverables

## S3 Storage

**Final wide table location:** `s3://291-s3-bucket/wide.parquet`

This Parquet file contains the complete aggregated dataset:
- **Index**: `(taxi_type, date, pickup_place)`
- **Columns**: `hour_0` through `hour_23` (pickup counts by hour)
- **Filtering**: Only rows with ≥ 50 total rides (across all hours)

## Generated Files

| File | Description |
|------|-------------|
| `wide_table.parquet` | Final aggregated wide table (all data) |
| `report.tex` | Performance report (LaTeX format) |
| `report.json` | Performance report (JSON format, if `--report-json` used) |
| `intermediate/YYYY-MM/*.parquet` | Monthly intermediate files (if `--keep-intermediate`) |

## Performance Report Contents

The `report.tex` file includes:
- **Total runtime** (wall-clock time)
- **Peak memory usage** (MB/GB)
- **Input row count** (total rows read)
- **Output row count** (rows in final wide table)
- **Discarded rows** (parse failures, <50 rides, etc.)
- **Month-mismatch counts** (rows where date month ≠ file month)
- **Resource utilization** (CPU cores, S3 throughput)
- **Runtime breakdown** (by pipeline stage)
