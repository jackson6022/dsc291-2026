# pivot_all_files.py — Main Pipeline

Single script that turns taxi trip Parquet data (local or S3) into one **wide table**: indexed by `(taxi_type, date, pickup_place)` with columns `hour_0`…`hour_23` (ride counts per hour). Drops rows with fewer than 50 rides, writes Parquet, and can upload the result to S3.

## What it does

1. **Discover** — Recursively finds `.parquet` files under `--input-dir` (local or `s3://`).
2. **Schema check** — Samples files and verifies required columns (pickup datetime, pickup location); normalizes to a common schema.
3. **Month grouping** — Groups files by `(year, month)` from path (e.g. `yellow_tripdata_2023-01.parquet` → 2023-01).
4. **Process by month** — For each month: read → normalize → pivot → drop rows with &lt; `--min-rides` → write intermediate Parquet. Counts rows where the row’s month ≠ file’s expected month.
5. **Combine** — Reads all intermediates, aggregates by `(taxi_type, date, pickup_place)`, sums hour columns → one wide table.
6. **Write** — Saves `wide_table.parquet` under `--output-dir`; optionally uploads to `--s3-output`.
7. **Report** — Writes JSON (and optional `.tex`) with input/output row counts, bad rows, peak RSS, run time.

## Run

From repo root:

```bash
python pivot_and_bootstrap/pivot_all_files.py
```

From `pivot_and_bootstrap/`:

```bash
python pivot_all_files.py
```

With no arguments, it uses **defaults defined in the script**:

- `DEFAULT_INPUT_DIR` = `s3://dsc291-ucsd/taxi/Dataset/2023/yellow_taxi/`
- `DEFAULT_OUTPUT_DIR` = `./pivot_and_bootstrap`

Edit these at the top of `pivot_all_files.py` to change behavior without passing CLI flags.

## CLI options

| Option | Default | Description |
|--------|---------|-------------|
| `--input-dir` | `DEFAULT_INPUT_DIR` | Local directory or S3 prefix containing Parquet files. |
| `--output-dir` | `DEFAULT_OUTPUT_DIR` | Local directory for intermediates and final `wide_table.parquet`. |
| `--min-rides` | 50 | Drop rows whose total rides (sum of hour_0…hour_23) are below this. |
| `--workers` | 1 | Number of parallel workers per month (process multiple files per month). |
| `--parallel-files` | — | Process **all** files in one pool with N workers. Overrides month-by-month processing. |
| `--max-months` | — | Process only the first N months (e.g. `12`). Use with `--parallel-files 32` to finish in ~30 min on S3 instead of full history. |
| `--keep-intermediate` | off | Keep `output-dir/intermediates/YYYY-MM/*.parquet` after combining. |
| `--s3-output` | — | S3 URI to upload the final wide table (e.g. `s3://bucket/wide.parquet`). |
| `--report-output` | `<output-dir>/report.json` | Path for the JSON report (default: next to wide table). |
| `--report-tex` | — | Optional path for a `.tex` report snippet. |
| `--partition-size` | — | Optional partition size; requires `partition_optimization` module. |
| `--skip-partition-optimization` | off | Do not run partition optimization. |
| `--max-memory-usage` | — | Max memory for partition optimization (e.g. `4GB`). |
| `--no-s3-anon` | off | Use AWS credentials for S3 (e.g. for `s3://nyc-tlc/`). |
| `-v` | off | Verbose (DEBUG) logging. |

## Examples

Default (code defaults; public S3):

```bash
python pivot_all_files.py
```

Override paths:

```bash
python pivot_all_files.py --input-dir /data/parquet --output-dir ./out
```

S3 with credentials (e.g. NYC TLC):

```bash
python pivot_all_files.py --input-dir "s3://nyc-tlc/trip data/" --output-dir ./out --no-s3-anon
```

Upload result to S3:

```bash
python pivot_all_files.py --s3-output "s3://my-bucket/wide_table.parquet"
```

More workers and keep intermediates:

```bash
python pivot_all_files.py --workers 4 --keep-intermediate
```

All files in one pool (faster; S3 may still take 2–4 h for 443 files):

```bash
python pivot_all_files.py --parallel-files 32
```

Target ~30 min on S3 (process only first 12 months):

```bash
python pivot_all_files.py --parallel-files 32 --max-months 12
```

## Dependencies

- **pivot_utils** (same directory): discovery, schema helpers, pivot, cleanup, S3 helpers.
- **Python packages:** `pandas`, `pyarrow`, `s3fs`, `fsspec`. Optional: `tqdm`, `botocore`/`boto3` for credential check when using `--no-s3-anon`.

## Output layout

- **Intermediates:** `{output-dir}/intermediates/YYYY-MM/<sanitized_filename>.parquet` (removed by default after combine).
- **Final table:** `{output-dir}/wide_table.parquet`.
- **Report:** Path from `--report-output` (default: `<output-dir>/report.json`, next to wide table).

## S3 notes

- **Public buckets** (e.g. `s3://dsc291-ucsd/...`, `s3://dask-data/...`): no flags; anonymous access is used.
- **NYC TLC** (`s3://nyc-tlc/trip data/`): use `--no-s3-anon` and configure AWS (`aws configure` or env vars). If credentials are missing, the script tries anonymous and may then hit 403.
- Quote paths with spaces: `"--input-dir \"s3://nyc-tlc/trip data/\""`.
