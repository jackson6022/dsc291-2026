# DSC 291 - Part 2: S3 & File Discovery

## Quick Start

```bash
# 1. Setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Run tests
python3 -m pytest test_s3_utils.py -v

# 3. Use the module
python3 -c "from s3_utils import discover_parquet_files; print(discover_parquet_files('.'))"
```

---

## What This Code Does

This module finds parquet files from **local folders** or **Amazon S3 buckets** using a single function.

```python
from s3_utils import discover_parquet_files

# Works with local paths
files = discover_parquet_files('/data/taxi/')

# Works with S3 paths
files = discover_parquet_files('s3://nyc-tlc/trip data/', anon=True)
```

Both return a **sorted list** of all `.parquet` file paths found recursively.

---

## Assignment Requirements (Part 2)

From `HOMEWORK_ASSIGNMENT_1.md`:

> **Part 2: S3 & File Discovery (15 pts)**
> - **Path helpers**: `is_s3_path`, `get_storage_options`, `get_filesystem`
> - **Discovery**: `discover_parquet_files(input_path)` — recursive, local or S3, sorted `.parquet` list

### Implementation Status

| Required Function | Implemented | Location |
|-------------------|-------------|----------|
| `is_s3_path` | ✅ | `s3_utils.py` line 20 |
| `get_storage_options` | ✅ | `s3_utils.py` line 78 |
| `get_filesystem` | ✅ | `s3_utils.py` line 114 |
| `discover_parquet_files` | ✅ | `s3_utils.py` line 243 |

---

## Files

| File | Purpose |
|------|---------|
| `s3_utils.py` | Main module with all Part 2 functions |
| `test_s3_utils.py` | 34 pytest tests for s3_utils |
| `requirements.txt` | Python dependencies |

---

## Functions Reference

### `is_s3_path(path)`
Returns `True` if path starts with `s3://` or `s3a://`.
```python
is_s3_path('s3://bucket/data')  # True
is_s3_path('/local/path')       # False
```

### `get_storage_options(path, anon=None)`
Returns storage options dict for pandas/pyarrow.
```python
get_storage_options('s3://bucket/path')  # {'anon': True}
get_storage_options('/local/path')       # {}
```

### `get_filesystem(path, anon=None)`
Returns filesystem object for the path type.
```python
fs = get_filesystem('s3://bucket/path')  # s3fs.S3FileSystem
fs = get_filesystem('/local/path')       # LocalFileSystem
```

### `discover_parquet_files(input_path, anon=None)`
Recursively finds all `.parquet` files. Returns sorted list.
```python
files = discover_parquet_files('/data/')
# ['/data/file1.parquet', '/data/subdir/file2.parquet']

files = discover_parquet_files('s3://nyc-tlc/trip data/', anon=True)
# ['s3://nyc-tlc/trip data/yellow_2023-01.parquet', ...]
```

---

## Running Tests

```bash
# All tests
python3 -m pytest test_s3_utils.py -v

# Specific test class
python3 -m pytest test_s3_utils.py::TestDiscoverParquetFiles -v

# With coverage
python3 -m pytest test_s3_utils.py --cov=s3_utils
```

**Expected output**: 34 passed

---

## How Person 4 Can Use This

Import the functions in their code:

```python
from s3_utils import discover_parquet_files, is_s3_path, get_filesystem

# In the main pipeline
def main(input_dir):
    files = discover_parquet_files(input_dir)
    for file in files:
        # process each parquet file
        pass
```

---

## Dependencies

```
pandas>=2.0.0
pyarrow>=14.0.0
s3fs>=2024.2.0
fsspec>=2024.2.0
boto3>=1.34.0
pytest>=7.4.0
```

Install with: `pip install -r requirements.txt`
