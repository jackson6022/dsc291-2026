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

Pipeline step 1 is **“Discover Parquet files (local dir or s3://...)”**. Part 2 is that step.

The main pipeline (e.g. Part 4’s `pivot_all_files.py`) will:

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
