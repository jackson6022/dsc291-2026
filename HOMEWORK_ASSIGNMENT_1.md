# Homework: Taxi Data Pivoting

**Course:** Data Science / Big Data Analytics · **Points:** 100 · **Due:** Feb 6 5PM (5 points deducted for every hour late)

---

## Overview

Build a pipeline that processes NYC TLC taxi trip Parquet data: **pivot** trip-level records into (date × taxi_type × pickup_place × hour) counts, **generate a single wide table** corresponding to **all available data**, **indexed by taxi_type, date, and pickup_place**, **store it as Parquet**, and support **local + S3** inputs. **Discard rows with fewer than 50 rides** (total count across hours). Use modular utilities, handle large files via partitioning, and write production-style code (logging, error handling, tests).

**S3 storage of output:** **Store the resulting single table as Parquet on S3** (e.g., a course bucket or your own). Future assignments will consume this stored data; include the S3 path(s) and any access instructions in your README.

---

## Learning Objectives

- **Pivoting & aggregation**: Trip-level → time-series `(taxi_type, date, pickup_place) × hour_0..hour_23`; discard rows with fewer than 50 rides.
- **Large data**: Partition-based processing, optional partition-size optimization.
- **Cloud I/O**: Read from S3 (anonymous where applicable); **write the single wide table as Parquet to S3**.
- **Code quality**: Docstrings, type hints, tests, clear errors.

---

## Pipeline Summary

1. **Discover** Parquet files (local dir or `s3://...`).
2. **Check** if schemas are consistent across discovered files. If not, **define a common schema** that contains all required columns (`pickup_datetime`, `pickup_location`, etc.) as necessary for the assignment; ensure all files are processed into this schema before further aggregation.
3. **Group files by month** (year–month) and **process one month at a time**. For each file, infer the expected month from the path/filename (e.g. `yellow_tripdata_2023-01.parquet` → 2023-01). Process: normalize → aggregate by `(date, taxi_type, pickup_place, hour)` → pivot → **discard rows with &lt; 50 rides** → write intermediate Parquet. **Count and report** the number of rows where the row’s month does not match the month corresponding to the Parquet file; include per-file and/or per-month breakdown, and a total.
4. **Combine** intermediate results into **a single wide table** corresponding to **all available data**, **indexed by taxi_type, date, and pickup_place**; aggregate by `(taxi_type, date, pickup_place)` and sum hour columns across all files. **Store the result as Parquet** (single file or partitioned e.g. `year=YYYY/month=MM/...`) and **upload to S3**. Document S3 location and access.
5. **Generate a report** that includes: **number of rows in the input** (across all files), **number of rows in the output** (wide table), **number of bad rows ignored** (e.g. parse failures, invalid data, time-stamp mismatch with file path,low-count cleanup), **memory use** (e.g. peak RSS), and **run time** (wall-clock for the pipeline). Output the report to a small tex file.

---

## Part 1: Core Utilities (20 pts) — `pivot_utils.py`

- **Column detection**: `find_pickup_datetime_col`, `find_pickup_location_col`, `infer_taxi_type_from_path` (handle common name variants, case-insensitive).
- **Month from path**: `infer_month_from_path(file_path)` → `(year, month)` or `None` when not inferrable (e.g. `...2023-01...`, `year=2023/month=01`), for grouping and month-mismatch checks.
- **Pivoting**: `pivot_counts_date_taxi_type_location(pdf)` → `(taxi_type, date, pickup_place)` index, `hour_0`…`hour_23`, fill missing with 0.
- **Cleaning**: `cleanup_low_count_rows(df, min_rides=50)`; **discard rows with fewer than 50 rides** (sum across hour columns &lt; `min_rides`); return stats.

---

## Part 2: S3 & File Discovery (15 pts)

- **Path helpers**: `is_s3_path`, `get_storage_options`, `get_filesystem` (e.g. `s3fs` for S3, anonymous when needed).
- **Discovery**: `discover_parquet_files(input_path)` — recursive, local or S3, sorted `.parquet` list.

---

## Part 3: Partition Optimization (20 pts) — `partition_optimization.py`

- **`parse_size(size_str)`**: Parse `"200MB"`, `"1.5GB"`, etc. → bytes.
- **`find_optimal_partition_size(...)`**: Test candidate sizes (e.g. 50MB–1GB), use PyArrow batching, measure time/memory; pick best within `max_memory_usage`. Support S3.

---

## Part 4: Main Pipeline (30 pts) — `pivot_all_files.py`

- **`process_single_file`**: Read Parquet (local/S3) → infer expected month from path → normalize → aggregate by `(date, taxi_type, pickup_place, hour)` → pivot → **discard rows with &lt; 50 rides** → write intermediate Parquet. **Count rows whose month ≠ file’s expected month**; return this count (and any per-file stats) along with metadata.
- **Month-at-a-time processing**: Group discovered files by `(year, month)`. Process **one month at a time** (e.g. all files for 2023-01, then 2023-02). Within a month, parallelize across files if desired.
- **Month-mismatch reporting**: Aggregate and **report** the number of rows where the row’s month does not match the Parquet file’s expected month. Include at least a **total** across all files; optionally also per-file and/or per-month breakdown (e.g. printed to console, logged, or written to a small summary file).
- **`combine_into_wide_table`**: Read all intermediates → aggregate by `(taxi_type, date, pickup_place)`, sum hour columns → produce **a single wide table** indexed by **taxi_type, date, pickup_place** for **all available data** → **store as Parquet** (final output).
- **Step 5 — Generate report**: Produce a **report** with: **input row count** (total rows read from all Parquet files), **output row count** (rows in the final wide table), **bad rows ignored** (rows dropped due to parse failures, invalid data, **&lt; 50 rides**, etc.), **memory use** (e.g. peak RSS), and **run time** (wall-clock). Emit to console, log file, or a small report file (e.g. JSON/text).
- **CLI `main()`**: `--input-dir`, `--output-dir`, `--min-rides` (default 50; rows with fewer rides discarded), `--workers`, `--partition-size` / `--skip-partition-optimization`, `--keep-intermediate`. Run discovery → group by month → (optional) partition optimization → **process one month at a time** (parallel within month) → **report month-mismatch counts** → **combine into single wide table** (indexed by taxi_type, date, pickup_place) → **store as Parquet** → **upload to S3** → **generate report** (rows in/out, bad rows, memory, run time). Use multiprocessing, tqdm, continue on per-file errors.

---

## Part 5: Testing (10 pts)

- Tests for: column detection variants, pivot output shape/values (index `taxi_type`, `date`, `pickup_place`; hour columns), error handling (missing columns, bad data). Optionally: `infer_month_from_path` for common path patterns; correctness of month-mismatch counts; **cleanup** drops rows with fewer than 50 rides.

---

## Part 6: Documentation (5 pts)

- Docstrings, type hints. **README**: setup, usage, CLI args, **S3 path(s) for the single Parquet table**, and troubleshooting.

---

## Deliverables

- `pivot_utils.py`, `partition_optimization.py`, `pivot_all_files.py`
- Testing script (e.g., `test_pivot_date_location_hour.py`)
- `README.md` (must include **S3 location of the single Parquet table** and usage instructions)
- 'performance.md'A table describing the rows of the generated wide table, in the following format



- **A two-page Microsoft Word report written by a human (not AI) (30% of grade)** with these sections:
    - **Summary of Tasks:** Clear description of all pipeline steps performed (including file discovery, partition optimization, **month-at-a-time processing**, Parquet processing/output, **month-mismatch reporting**, production of the **single wide table stored as Parquet** for all available data, and **Step 5 report** generation).
    - **Resource Utilization:** Quantitative stats on total memory consumption, number of CPU cores/workers used, S3/local storage throughput, and specific S3-related performance notes. You may use the **Step 5 report** (memory use, run time) as a source for this section.
    - **Run-Time Analysis:** Overall wall time for the full pipeline and time breakdowns for key stages (such as file discovery, per-month file processing, and single-table combination/output). You may use the **Step 5 report** (run time, row counts) as a source.
    - **Conclusion:** Concise summary of findings, discussion of bottlenecks/limitations (if any), and statement of the S3 location for the **single Parquet table**.
---

## Performance Summary Report: Instructions

After running your complete pipeline, generate a performance summary file named `performance.md` in your project root. Your report must include, at minimum, the following measured and aggregated metrics:

- **Total runtime** of the end-to-end process (seconds/minutes).
- **Peak memory usage** (MB/GB).
- **Total input rows** and **discarded rows** (with percentage skipped).
- **Output row counts**:  
    - Rows in the intermediate pivoted table  
    - Rows in the final aggregated/wide table
- A **breakdown of discarded rows by reason** (e.g., date inconsistencies, parse failures).
- Counts and details for any **date consistency issues** (number of inconsistent rows, number of files affected).
- A **row breakdown** by year and taxi type (as produced in your wide table).
- Schema summary: number of output columns, and a listing of relevant column names.

> **Note:** See the file `pivot_and_bootstrap/processing_summary.md` for a concrete example of format and level of detail to follow.

**Tip:**  
Tabulate all metrics using the stats your code prints or emits (as per the pipeline requirements). The goal is to give a clear and quantitative overview of throughput, resource use, and data retention.

---


**Performance Expectation (30% of grade):**  
Your pipeline should be optimized so that **the full end-to-end program completes within 30mins on r8i.4xlarge instance** (8 vCPUs, 128 GB RAM) for the full dataset. This means that all steps—including input file discovery, **month-at-a-time** per-file processing (parallelized Parquet loading, pivot/cleanup), **month-mismatch reporting**, combination into **a single wide table stored as Parquet** for all available data, S3 output, and **Step 5 report** generation—should collectively fit within this wall-clock time budget. Memory usage must remain within instance capacity, and optimal parallel worker configuration is expected (`--workers`). Use efficient I/O, optimal partition size, batch operations, and robust multiprocessing to meet this requirement, and monitor resource utilization as requested for the Word report. **Run time performance is worth 30% of your grade.**

---

## Evaluation

| Area | Pts | % |
|------|-----|---|
| **MS Word Report** | 30 | 30% |
| **Run Time Performance** | 30 | 30% |
| Functionality | 15 | 15% |
| Code quality & documentation | 10 | 10% |
| Error handling | 10 | 10% |
| Testing | 5 | 5% |
| **Total** | **100** | **100%** |

---

## Submission

### Submission Instructions

- Develop the code initially on your laptop, using a sample of the Parquet data to validate functionality before scaling up.
- Use GitHub to move your code and sync updates between your development environment and the AWS instance.
- Code should reside in the `pivot_and_bootstrap/` directory. Ensure all necessary imports run without error.
- Clearly document the S3 paths for the **single Parquet table** (wide table for all available data) in your README and code comments.
- Actively monitor memory usage—out-of-memory issues are the most common runtime problem and can be tricky to diagnose after a crash. Consider using memory profiling tools and batch processing strategies.
- You may use standard libraries and API references as needed.
- Use Cursor to compare multiple implementation approaches to optimize code quality and performance.
