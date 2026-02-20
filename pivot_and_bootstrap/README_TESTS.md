# Part 5: Testing Documentation

## Overview

This document describes the test suite for the NYC TLC taxi data pivoting pipeline (Part 5 of Homework Assignment 1).

## Files Created

- **`test_pivot_date_location_hour.py`**: Comprehensive test suite with 30+ test cases
- **`pivot_utils.py`**: Stub file with function signatures and docstrings (to be implemented in Parts 1-4)

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

## Test-Driven Development (TDD) Approach

These tests were created **before** the implementation (Parts 1-4). This follows TDD principles:

1. **Write tests first**: Define expected behavior through tests
2. **Run tests (they should fail)**: Currently all tests will fail with `NotImplementedError`
3. **Implement code**: Write the actual implementation in `pivot_utils.py`
4. **Run tests again**: Tests should pass once implementation is correct
5. **Refactor**: Improve code while keeping tests passing

## Current Status

- ✅ Test suite complete (30+ tests covering all requirements)
- ✅ Stub file created with function signatures
- ⏳ Implementation pending (Parts 1-4)

## Next Steps

To make the tests pass, implement the following in `pivot_utils.py`:

1. **`find_pickup_datetime_col(df)`**: Detect pickup datetime column
2. **`find_pickup_location_col(df)`**: Detect pickup location column
3. **`infer_taxi_type_from_path(path)`**: Extract taxi type from file path
4. **`infer_month_from_path(path)`**: Extract (year, month) from file path
5. **`pivot_counts_date_taxi_type_location(df)`**: Pivot trip data into time-series
6. **`cleanup_low_count_rows(df, min_rides)`**: Remove low-count rows and return stats

## Example Test Output

When implementation is complete, you should see:

```
test_pivot_date_location_hour.py::TestColumnDetection::test_find_pickup_datetime_col_standard PASSED
test_pivot_date_location_hour.py::TestColumnDetection::test_find_pickup_datetime_col_case_insensitive PASSED
test_pivot_date_location_hour.py::TestColumnDetection::test_find_pickup_location_col_standard PASSED
...
================================ 30 passed in 2.5s ================================
```

## Notes

- Tests use pytest fixtures for sample data creation
- Tests are independent and can run in any order
- Integration test uses temporary directories (cleaned up automatically)
- Error handling tests use `pytest.raises()` for expected exceptions

## Grading Criteria (Part 5: 10 pts)

The tests cover all requirements from the homework:
- ✅ Column detection variants
- ✅ Pivot output shape/values (correct index and hour columns)
- ✅ Error handling (missing columns, bad data)
- ✅ `infer_month_from_path` for common patterns
- ✅ Month-mismatch counting
- ✅ Cleanup drops rows with < 50 rides
- ✅ Integration test with Parquet file
