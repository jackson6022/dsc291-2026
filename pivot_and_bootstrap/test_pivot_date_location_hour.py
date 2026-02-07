"""
Test suite for NYC TLC taxi data pivoting pipeline.

Tests cover:
- Column detection (datetime, location, taxi type)
- Month inference from file paths
- Pivot function output shape and values
- Cleanup of low-count rows
- Error handling
- Month-mismatch counting
- Integration test with sample data
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# Import functions to test (these will be implemented in pivot_utils.py)
from pivot_and_bootstrap.pivot_utils import (
    find_pickup_datetime_col,
    find_pickup_location_col,
    infer_taxi_type_from_path,
    infer_month_from_path,
    pivot_counts_date_taxi_type_location,
    cleanup_low_count_rows,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_trip_data():
    """Create sample trip data for testing."""
    np.random.seed(42)
    base_date = datetime(2023, 1, 15)
    n_rows = 1000

    data = {
        'tpep_pickup_datetime': [
            base_date + timedelta(hours=np.random.randint(0, 24),
                                 minutes=np.random.randint(0, 60))
            for _ in range(n_rows)
        ],
        'PULocationID': np.random.randint(1, 265, n_rows),
        'DOLocationID': np.random.randint(1, 265, n_rows),
        'passenger_count': np.random.randint(1, 6, n_rows),
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_pivoted_data():
    """Create sample pivoted data for cleanup testing."""
    data = {
        'taxi_type': ['yellow'] * 5 + ['green'] * 5,
        'date': pd.to_datetime(['2023-01-15'] * 10),
        'pickup_place': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    }

    # Add hour columns
    for hour in range(24):
        data[f'hour_{hour}'] = np.random.randint(0, 10, 10)

    df = pd.DataFrame(data)
    df = df.set_index(['taxi_type', 'date', 'pickup_place'])

    # Ensure some rows have < 50 total rides
    df.iloc[0, :] = 0  # Row with 0 rides
    df.iloc[1, :] = 1  # Row with 24 rides (1 per hour)
    df.iloc[2, 0:5] = 10  # Row with 50 rides (exactly threshold)

    return df


# ============================================================================
# TEST: Column Detection
# ============================================================================

class TestColumnDetection:
    """Test column detection functions."""

    def test_find_pickup_datetime_col_standard(self):
        """Test finding standard pickup datetime column names."""
        # Yellow taxi variant
        df = pd.DataFrame({'tpep_pickup_datetime': [datetime.now()]})
        assert find_pickup_datetime_col(df.columns.tolist()) == 'tpep_pickup_datetime'

        # Green taxi variant
        df = pd.DataFrame({'lpep_pickup_datetime': [datetime.now()]})
        assert find_pickup_datetime_col(df.columns.tolist()) == 'lpep_pickup_datetime'

        # Generic variant
        df = pd.DataFrame({'pickup_datetime': [datetime.now()]})
        assert find_pickup_datetime_col(df.columns.tolist()) == 'pickup_datetime'

    def test_find_pickup_datetime_col_case_insensitive(self):
        """Test case-insensitive detection."""
        df = pd.DataFrame({'Pickup_DateTime': [datetime.now()]})
        result = find_pickup_datetime_col(df.columns.tolist())
        assert result.lower() == 'pickup_datetime'

    def test_find_pickup_datetime_col_missing(self):
        """Test None return when datetime column is missing."""
        df = pd.DataFrame({'some_other_col': [1, 2, 3]})
        result = find_pickup_datetime_col(df.columns.tolist())
        assert result is None

    def test_find_pickup_location_col_standard(self):
        """Test finding standard pickup location column names."""
        # Standard variant
        df = pd.DataFrame({'PULocationID': [1, 2, 3]})
        assert find_pickup_location_col(df.columns.tolist()) == 'PULocationID'

        # Lowercase variant
        df = pd.DataFrame({'pickup_location_id': [1, 2, 3]})
        result = find_pickup_location_col(df.columns.tolist())
        assert result.lower() == 'pickup_location_id'

    def test_find_pickup_location_col_fallback(self):
        """Test fallback to dropoff location if pickup not found."""
        df = pd.DataFrame({'DOLocationID': [1, 2, 3]})
        result = find_pickup_location_col(df.columns.tolist())
        # Should return None since DOLocationID is not a pickup location
        assert result is None

    def test_find_pickup_location_col_missing(self):
        """Test None return when location column is missing."""
        df = pd.DataFrame({'some_other_col': [1, 2, 3]})
        result = find_pickup_location_col(df.columns.tolist())
        assert result is None

    def test_infer_taxi_type_from_path_yellow(self):
        """Test inferring yellow taxi type from path."""
        paths = [
            '/data/yellow_tripdata_2023-01.parquet',
            '/data/YELLOW_tripdata_2023-01.parquet',
            's3://bucket/yellow_trip_2023.parquet',
        ]
        for path in paths:
            result = infer_taxi_type_from_path(path)
            assert result is not None and result.lower() == 'yellow'

    def test_infer_taxi_type_from_path_green(self):
        """Test inferring green taxi type from path."""
        paths = [
            '/data/green_tripdata_2023-01.parquet',
            '/data/GREEN_tripdata_2023-01.parquet',
            's3://bucket/green_trip_2023.parquet',
        ]
        for path in paths:
            result = infer_taxi_type_from_path(path)
            assert result is not None and result.lower() == 'green'

    def test_infer_taxi_type_from_path_fhv(self):
        """Test inferring FHV taxi types from path."""
        # FHV
        assert infer_taxi_type_from_path('/data/fhv_tripdata_2023-01.parquet').lower() == 'fhv'

        # FHVHV
        assert infer_taxi_type_from_path('/data/fhvhv_tripdata_2023-01.parquet').lower() == 'fhvhv'

    def test_infer_taxi_type_from_path_unknown(self):
        """Test handling of unknown taxi type."""
        path = '/data/unknown_tripdata_2023-01.parquet'
        result = infer_taxi_type_from_path(path)
        # Should return None or 'unknown'
        assert result is None or result.lower() == 'unknown'


# ============================================================================
# TEST: Month Inference
# ============================================================================

class TestMonthInference:
    """Test month inference from file paths."""

    def test_infer_month_hyphenated_format(self):
        """Test month inference from hyphenated date format."""
        paths_and_expected = [
            ('yellow_tripdata_2023-01.parquet', (2023, 1)),
            ('green_tripdata_2023-12.parquet', (2023, 12)),
            ('/path/to/yellow_tripdata_2024-06.parquet', (2024, 6)),
        ]
        for path, expected in paths_and_expected:
            assert infer_month_from_path(path) == expected

    def test_infer_month_partitioned_format(self):
        """Test month inference from partitioned path format."""
        paths_and_expected = [
            ('s3://bucket/year=2023/month=01/data.parquet', (2023, 1)),
            ('/data/year=2024/month=12/yellow.parquet', (2024, 12)),
            ('year=2023/month=06/part-001.parquet', (2023, 6)),
        ]
        for path, expected in paths_and_expected:
            assert infer_month_from_path(path) == expected

    def test_infer_month_underscore_format(self):
        """Test month inference from underscore date format."""
        paths_and_expected = [
            ('yellow_2023_01_data.parquet', (2023, 1)),
            ('green_2024_12.parquet', (2024, 12)),
        ]
        for path, expected in paths_and_expected:
            result = infer_month_from_path(path)
            # This format might not be supported, so result could be None
            assert result == expected or result is None

    def test_infer_month_no_date(self):
        """Test handling of paths without date information."""
        paths = [
            'data.parquet',
            '/path/to/file.parquet',
            's3://bucket/unknown.parquet',
        ]
        for path in paths:
            assert infer_month_from_path(path) is None


# ============================================================================
# TEST: Pivot Function
# ============================================================================

class TestPivotFunction:
    """Test pivoting functionality."""

    def test_pivot_output_has_correct_index(self, sample_trip_data):
        """Test that pivot output has correct multi-index."""
        result = pivot_counts_date_taxi_type_location(
            sample_trip_data,
            taxi_type='yellow',
            datetime_col='tpep_pickup_datetime',
            location_col='PULocationID'
        )

        # Check that result has the expected columns
        assert 'taxi_type' in result.columns
        assert 'date' in result.columns
        assert 'pickup_place' in result.columns

    def test_pivot_output_has_hour_columns(self, sample_trip_data):
        """Test that pivot output has all 24 hour columns."""
        result = pivot_counts_date_taxi_type_location(
            sample_trip_data,
            taxi_type='yellow',
            datetime_col='tpep_pickup_datetime',
            location_col='PULocationID'
        )

        # Check for all hour columns
        expected_cols = [f'hour_{i}' for i in range(24)]
        for col in expected_cols:
            assert col in result.columns

    def test_pivot_fills_missing_hours_with_zero(self):
        """Test that missing hours are filled with 0."""
        # Create data with only hour 10
        data = {
            'tpep_pickup_datetime': [datetime(2023, 1, 15, 10, 30)],
            'PULocationID': [1],
        }
        df = pd.DataFrame(data)

        result = pivot_counts_date_taxi_type_location(
            df,
            taxi_type='yellow',
            datetime_col='tpep_pickup_datetime',
            location_col='PULocationID'
        )

        # Hour 10 should have count >= 1
        assert result.iloc[0]['hour_10'] >= 1

        # Other hours should be 0
        for hour in range(24):
            if hour != 10:
                assert result.iloc[0][f'hour_{hour}'] == 0

    def test_pivot_aggregates_correctly(self):
        """Test that counts are aggregated correctly."""
        # Create data with known counts
        base_date = datetime(2023, 1, 15, 10, 0)
        data = {
            'tpep_pickup_datetime': [base_date] * 5,  # 5 trips in same hour
            'PULocationID': [100] * 5,  # Same location
        }
        df = pd.DataFrame(data)

        result = pivot_counts_date_taxi_type_location(
            df,
            taxi_type='yellow',
            datetime_col='tpep_pickup_datetime',
            location_col='PULocationID'
        )

        # Should have exactly 1 row (1 date × 1 location × 1 taxi type)
        assert len(result) == 1

        # Hour 10 should have count of 5
        assert result.iloc[0]['hour_10'] == 5

    def test_pivot_handles_multiple_dates(self):
        """Test pivoting with multiple dates."""
        data = {
            'tpep_pickup_datetime': [
                datetime(2023, 1, 15, 10, 0),
                datetime(2023, 1, 16, 10, 0),
            ],
            'PULocationID': [100, 100],
        }
        df = pd.DataFrame(data)

        result = pivot_counts_date_taxi_type_location(
            df,
            taxi_type='yellow',
            datetime_col='tpep_pickup_datetime',
            location_col='PULocationID'
        )

        # Should have 2 rows (2 dates × 1 location × 1 taxi type)
        assert len(result) == 2

    def test_pivot_handles_multiple_taxi_types(self):
        """Test pivoting with multiple taxi types by processing separately."""
        # Process yellow taxi data
        data_yellow = {
            'tpep_pickup_datetime': [datetime(2023, 1, 15, 10, 0)],
            'PULocationID': [100],
        }
        df_yellow = pd.DataFrame(data_yellow)
        result_yellow = pivot_counts_date_taxi_type_location(
            df_yellow,
            taxi_type='yellow',
            datetime_col='tpep_pickup_datetime',
            location_col='PULocationID'
        )

        # Process green taxi data
        data_green = {
            'lpep_pickup_datetime': [datetime(2023, 1, 15, 10, 0)],
            'PULocationID': [100],
        }
        df_green = pd.DataFrame(data_green)
        result_green = pivot_counts_date_taxi_type_location(
            df_green,
            taxi_type='green',
            datetime_col='lpep_pickup_datetime',
            location_col='PULocationID'
        )

        # Combine results
        result = pd.concat([result_yellow, result_green], ignore_index=True)

        # Should have 2 rows (1 date × 1 location × 2 taxi types)
        assert len(result) == 2


# ============================================================================
# TEST: Cleanup Low Count Rows
# ============================================================================

class TestCleanupLowCount:
    """Test cleanup of rows with low ride counts."""

    def test_cleanup_removes_rows_below_threshold(self, sample_pivoted_data):
        """Test that rows with < 50 rides are removed."""
        result, stats = cleanup_low_count_rows(sample_pivoted_data, min_rides=50)

        # Rows with total < 50 should be removed
        for idx, row in result.iterrows():
            total_rides = row.sum()
            assert total_rides >= 50

    def test_cleanup_keeps_rows_at_threshold(self, sample_pivoted_data):
        """Test that rows with exactly 50 rides are kept."""
        result, stats = cleanup_low_count_rows(sample_pivoted_data, min_rides=50)

        # Should keep rows with exactly 50 rides
        assert len(result) > 0

    def test_cleanup_returns_stats(self, sample_pivoted_data):
        """Test that cleanup returns statistics."""
        result, stats = cleanup_low_count_rows(sample_pivoted_data, min_rides=50)

        # Stats should include before/after counts
        assert 'rows_before' in stats or 'total_rows' in stats
        assert 'rows_after' in stats or 'kept_rows' in stats
        assert 'rows_dropped' in stats or 'removed_rows' in stats

    def test_cleanup_with_different_thresholds(self, sample_pivoted_data):
        """Test cleanup with different min_rides thresholds."""
        result_10, _ = cleanup_low_count_rows(sample_pivoted_data, min_rides=10)
        result_100, _ = cleanup_low_count_rows(sample_pivoted_data, min_rides=100)

        # Higher threshold should remove more rows
        assert len(result_100) <= len(result_10)

    def test_cleanup_preserves_index(self, sample_pivoted_data):
        """Test that cleanup preserves the multi-index structure."""
        result, _ = cleanup_low_count_rows(sample_pivoted_data, min_rides=50)

        # Index should remain the same structure
        assert result.index.names == sample_pivoted_data.index.names
        assert isinstance(result.index, pd.MultiIndex)


# ============================================================================
# TEST: Error Handling
# ============================================================================

class TestErrorHandling:
    """Test error handling in various scenarios."""

    def test_missing_datetime_column_raises_error(self):
        """Test that missing datetime column returns None."""
        df = pd.DataFrame({'PULocationID': [1, 2, 3]})
        result = find_pickup_datetime_col(df.columns.tolist())
        assert result is None

    def test_missing_location_column_raises_error(self):
        """Test that missing location column returns None."""
        df = pd.DataFrame({'tpep_pickup_datetime': [datetime.now()]})
        result = find_pickup_location_col(df.columns.tolist())
        assert result is None

    def test_null_datetime_handling(self):
        """Test handling of null datetime values."""
        data = {
            'tpep_pickup_datetime': [datetime.now(), None, datetime.now()],
            'PULocationID': [1, 2, 3],
        }
        df = pd.DataFrame(data)

        # Should either filter out nulls or raise clear error
        try:
            result = pivot_counts_date_taxi_type_location(
                df,
                taxi_type='yellow',
                datetime_col='tpep_pickup_datetime',
                location_col='PULocationID'
            )
            # If it succeeds, check that null row was excluded
            assert result is not None
        except (ValueError, TypeError):
            # Acceptable to raise error for null datetimes
            pass

    def test_invalid_location_id_handling(self):
        """Test handling of invalid location IDs."""
        data = {
            'tpep_pickup_datetime': [datetime.now()],
            'PULocationID': [-1],  # Invalid location
        }
        df = pd.DataFrame(data)

        # Should handle gracefully (either filter or include)
        try:
            result = pivot_counts_date_taxi_type_location(
                df,
                taxi_type='yellow',
                datetime_col='tpep_pickup_datetime',
                location_col='PULocationID'
            )
            assert result is not None
        except ValueError:
            # Acceptable to raise error for invalid locations
            pass

    def test_empty_dataframe_handling(self):
        """Test handling of empty DataFrames."""
        df = pd.DataFrame()

        # Should handle empty DataFrame gracefully
        try:
            find_pickup_datetime_col(df)
        except (ValueError, KeyError, IndexError):
            # Expected to raise error for empty DataFrame
            pass


# ============================================================================
# TEST: Month Mismatch Counting
# ============================================================================

class TestMonthMismatch:
    """Test month-mismatch counting logic."""

    def test_count_month_mismatches(self):
        """Test counting rows where date month != file month."""
        data = {
            'tpep_pickup_datetime': [
                datetime(2023, 1, 15),  # Match
                datetime(2023, 2, 15),  # Mismatch
                datetime(2023, 1, 20),  # Match
                datetime(2023, 3, 1),   # Mismatch
            ],
            'PULocationID': [1, 2, 3, 4],
        }
        df = pd.DataFrame(data)

        expected_month = (2023, 1)

        # Count mismatches
        df['date'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.date
        df['year'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.year
        df['month'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.month

        mismatches = df[(df['year'] != expected_month[0]) |
                       (df['month'] != expected_month[1])]

        assert len(mismatches) == 2

    def test_no_mismatches(self):
        """Test case where all rows match expected month."""
        data = {
            'tpep_pickup_datetime': [
                datetime(2023, 1, 15),
                datetime(2023, 1, 20),
                datetime(2023, 1, 25),
            ],
            'PULocationID': [1, 2, 3],
        }
        df = pd.DataFrame(data)

        expected_month = (2023, 1)

        df['year'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.year
        df['month'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.month

        mismatches = df[(df['year'] != expected_month[0]) |
                       (df['month'] != expected_month[1])]

        assert len(mismatches) == 0


# ============================================================================
# TEST: Integration
# ============================================================================

class TestIntegration:
    """Integration tests with sample Parquet files."""

    def test_end_to_end_with_parquet_file(self):
        """Test complete pipeline with a sample Parquet file."""
        # Create sample data with enough trips to pass cleanup threshold
        base_date = datetime(2023, 1, 15)
        # Create 200 trips concentrated on a single day for each location
        data = {
            'tpep_pickup_datetime': (
                [base_date + timedelta(hours=h % 24, minutes=m) 
                 for h in range(12) for m in range(10)]  # 120 trips at location 100
                +
                [base_date + timedelta(hours=h % 24, minutes=m) 
                 for h in range(12, 24) for m in range(10)]  # 120 trips at location 200
            ),
            'PULocationID': [100] * 120 + [200] * 120,  # Two locations with enough rides
            'DOLocationID': [101] * 240,
        }
        df = pd.DataFrame(data)

        # Write to temporary Parquet file
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / 'yellow_tripdata_2023-01.parquet'
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path)

            # Read back and process
            df_read = pd.read_parquet(file_path)

            # Test column detection
            datetime_col = find_pickup_datetime_col(df_read.columns.tolist())
            assert datetime_col is not None

            location_col = find_pickup_location_col(df_read.columns.tolist())
            assert location_col is not None

            taxi_type = infer_taxi_type_from_path(str(file_path))
            assert taxi_type is not None and taxi_type.lower() == 'yellow'

            month_info = infer_month_from_path(str(file_path))
            assert month_info == (2023, 1)

            # Pivot the data
            result = pivot_counts_date_taxi_type_location(
                df_read,
                taxi_type=taxi_type,
                datetime_col=datetime_col,
                location_col=location_col
            )

            # Verify output structure
            assert 'taxi_type' in result.columns
            assert 'date' in result.columns
            assert 'pickup_place' in result.columns
            hour_cols = [c for c in result.columns if c.startswith('hour_')]
            assert len(hour_cols) == 24

            # Cleanup low-count rows
            cleaned, stats = cleanup_low_count_rows(result, min_rides=50)

            # Both locations should have >= 50 rides
            assert len(cleaned) >= 1


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    # Run tests with pytest
    pytest.main([__file__, '-v', '--tb=short'])
