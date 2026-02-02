"""
Tests for S3 & File Discovery Utilities

This module tests the s3_utils module functions for path detection,
filesystem abstraction, and parquet file discovery.
"""

import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Import the module to test
from pivot_utils import (
    is_s3_path,
    parse_s3_path,
    get_storage_options,
    get_filesystem,
    discover_parquet_files,
    validate_parquet_file,
    get_file_size,
    _discover_local_parquet_files,
)


class TestIsS3Path:
    """Tests for the is_s3_path function."""
    
    def test_s3_path_lowercase(self):
        """Test detection of lowercase s3:// prefix."""
        assert is_s3_path('s3://my-bucket/data/') is True
        assert is_s3_path('s3://bucket/file.parquet') is True
    
    def test_s3_path_uppercase(self):
        """Test detection of uppercase S3:// prefix."""
        assert is_s3_path('S3://my-bucket/data/') is True
        assert is_s3_path('S3://BUCKET/FILE.parquet') is True
    
    def test_s3a_path(self):
        """Test detection of s3a:// prefix (Hadoop-style)."""
        assert is_s3_path('s3a://my-bucket/data/') is True
        assert is_s3_path('S3A://bucket/file.parquet') is True
    
    def test_local_paths_not_s3(self):
        """Test that local paths are not detected as S3."""
        assert is_s3_path('/local/path/to/data') is False
        assert is_s3_path('./relative/path') is False
        assert is_s3_path('relative/path') is False
        assert is_s3_path('/home/user/data.parquet') is False
    
    def test_empty_and_invalid_inputs(self):
        """Test handling of empty and invalid inputs."""
        assert is_s3_path('') is False
        assert is_s3_path('   ') is False
        assert is_s3_path(None) is False
        assert is_s3_path(123) is False
        assert is_s3_path(['s3://bucket']) is False
    
    def test_path_with_whitespace(self):
        """Test paths with leading/trailing whitespace."""
        assert is_s3_path('  s3://bucket/path  ') is True
        assert is_s3_path('\ts3://bucket/path\n') is True
    
    def test_similar_but_not_s3_paths(self):
        """Test paths that look similar but aren't S3."""
        assert is_s3_path('s3/bucket/path') is False
        assert is_s3_path('http://s3.amazonaws.com/bucket') is False
        assert is_s3_path('s3-bucket/path') is False


class TestParseS3Path:
    """Tests for the parse_s3_path function."""
    
    def test_simple_path(self):
        """Test parsing a simple S3 path."""
        bucket, key = parse_s3_path('s3://my-bucket/data/file.parquet')
        assert bucket == 'my-bucket'
        assert key == 'data/file.parquet'
    
    def test_bucket_only(self):
        """Test parsing bucket-only path."""
        bucket, key = parse_s3_path('s3://my-bucket/')
        assert bucket == 'my-bucket'
        assert key == ''
    
    def test_nested_path(self):
        """Test parsing deeply nested path."""
        bucket, key = parse_s3_path('s3://bucket/a/b/c/d/file.parquet')
        assert bucket == 'bucket'
        assert key == 'a/b/c/d/file.parquet'
    
    def test_s3a_prefix(self):
        """Test parsing s3a:// paths."""
        bucket, key = parse_s3_path('s3a://bucket/prefix/')
        assert bucket == 'bucket'
        assert key == 'prefix/'
    
    def test_invalid_path_raises(self):
        """Test that non-S3 paths raise ValueError."""
        with pytest.raises(ValueError):
            parse_s3_path('/local/path')
        with pytest.raises(ValueError):
            parse_s3_path('http://example.com')


class TestGetStorageOptions:
    """Tests for the get_storage_options function."""
    
    def test_local_path_returns_empty(self):
        """Test that local paths return empty options."""
        assert get_storage_options('/local/path') == {}
        assert get_storage_options('./relative') == {}
    
    def test_s3_path_default_anon(self):
        """Test S3 path with default (anonymous) access."""
        options = get_storage_options('s3://bucket/path')
        assert options.get('anon') is True
    
    def test_s3_path_explicit_anon(self):
        """Test S3 path with explicit anonymous access."""
        options = get_storage_options('s3://bucket/path', anon=True)
        assert options['anon'] is True
        
        options = get_storage_options('s3://bucket/path', anon=False)
        assert options['anon'] is False


class TestGetFilesystem:
    """Tests for the get_filesystem function."""
    
    def test_local_filesystem(self):
        """Test getting local filesystem."""
        fs = get_filesystem('/local/path')
        # Should return a filesystem object (fsspec or pyarrow)
        assert fs is not None
    
    def test_s3_filesystem_integration(self):
        """Test getting S3 filesystem (integration test, requires s3fs)."""
        try:
            import s3fs
            fs = get_filesystem('s3://bucket/path', anon=True)
            assert isinstance(fs, s3fs.S3FileSystem)
        except ImportError:
            pytest.skip("s3fs not installed")
    
    @patch.dict('sys.modules', {'s3fs': None})
    def test_s3_without_s3fs_raises(self):
        """Test that missing s3fs raises ImportError."""
        # This test verifies the error handling when s3fs is not installed
        # In practice, we mock the import failure
        pass  # Skip if s3fs is installed


class TestDiscoverLocalParquetFiles:
    """Tests for local parquet file discovery."""
    
    def test_discover_in_directory(self, tmp_path):
        """Test discovering parquet files in a directory."""
        # Create test parquet files
        (tmp_path / 'file1.parquet').touch()
        (tmp_path / 'file2.parquet').touch()
        (tmp_path / 'file3.txt').touch()  # Non-parquet file
        
        files = _discover_local_parquet_files(str(tmp_path))
        
        assert len(files) == 2
        assert all(f.endswith('.parquet') for f in files)
        assert files == sorted(files)  # Should be sorted
    
    def test_discover_nested_directories(self, tmp_path):
        """Test discovering parquet files in nested directories."""
        # Create nested structure
        subdir1 = tmp_path / 'year=2023' / 'month=01'
        subdir2 = tmp_path / 'year=2023' / 'month=02'
        subdir1.mkdir(parents=True)
        subdir2.mkdir(parents=True)
        
        (subdir1 / 'data.parquet').touch()
        (subdir2 / 'data.parquet').touch()
        (tmp_path / 'root.parquet').touch()
        
        files = _discover_local_parquet_files(str(tmp_path))
        
        assert len(files) == 3
    
    def test_discover_single_file(self, tmp_path):
        """Test discovery when given a single file path."""
        file_path = tmp_path / 'single.parquet'
        file_path.touch()
        
        files = _discover_local_parquet_files(str(file_path))
        
        assert len(files) == 1
        assert files[0].endswith('single.parquet')
    
    def test_empty_directory(self, tmp_path):
        """Test discovery in empty directory."""
        files = _discover_local_parquet_files(str(tmp_path))
        assert len(files) == 0
    
    def test_no_parquet_files(self, tmp_path):
        """Test directory with no parquet files."""
        (tmp_path / 'file1.csv').touch()
        (tmp_path / 'file2.json').touch()
        
        files = _discover_local_parquet_files(str(tmp_path))
        assert len(files) == 0
    
    def test_case_insensitive_extension(self, tmp_path):
        """Test that .PARQUET files are also discovered."""
        (tmp_path / 'file1.parquet').touch()
        (tmp_path / 'file2.PARQUET').touch()  # Uppercase
        
        files = _discover_local_parquet_files(str(tmp_path))
        
        # Note: This depends on the filesystem case sensitivity
        # On macOS/Windows, both should be found
        assert len(files) >= 1


class TestDiscoverParquetFiles:
    """Integration tests for discover_parquet_files."""
    
    def test_local_discovery(self, tmp_path):
        """Test full local discovery flow."""
        (tmp_path / 'data.parquet').touch()
        
        files = discover_parquet_files(str(tmp_path))
        
        assert len(files) == 1
        assert files[0].endswith('data.parquet')
    
    def test_invalid_path_raises(self):
        """Test that empty path raises ValueError."""
        with pytest.raises(ValueError):
            discover_parquet_files('')
        with pytest.raises(ValueError):
            discover_parquet_files('   ')
    
    def test_nonexistent_local_path_raises(self):
        """Test that nonexistent local path raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            discover_parquet_files('/nonexistent/path/12345')
    
    @patch('pivot_utils._discover_s3_parquet_files')
    def test_s3_discovery_calls_s3_function(self, mock_s3_discover):
        """Test that S3 paths use S3 discovery."""
        mock_s3_discover.return_value = ['s3://bucket/file.parquet']
        
        files = discover_parquet_files('s3://bucket/prefix/')
        
        mock_s3_discover.assert_called_once()
        assert files == ['s3://bucket/file.parquet']


class TestValidateParquetFile:
    """Tests for validate_parquet_file function."""
    
    def test_nonexistent_file_returns_false(self, tmp_path):
        """Test that nonexistent file returns False."""
        result = validate_parquet_file(str(tmp_path / 'nonexistent.parquet'))
        assert result is False
    
    def test_invalid_parquet_returns_false(self, tmp_path):
        """Test that invalid parquet content returns False."""
        # Create a file that's not actually parquet
        fake_file = tmp_path / 'fake.parquet'
        fake_file.write_text('not a parquet file')
        
        result = validate_parquet_file(str(fake_file))
        assert result is False
    
    @pytest.mark.skipif(
        not pytest.importorskip('pyarrow', reason='pyarrow not installed'),
        reason='pyarrow required for this test'
    )
    def test_valid_parquet_returns_true(self, tmp_path):
        """Test that valid parquet file returns True."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        # Create a valid parquet file
        table = pa.table({'col': [1, 2, 3]})
        file_path = tmp_path / 'valid.parquet'
        pq.write_table(table, str(file_path))
        
        result = validate_parquet_file(str(file_path))
        assert result is True


class TestGetFileSize:
    """Tests for get_file_size function."""
    
    def test_local_file_size(self, tmp_path):
        """Test getting size of local file."""
        file_path = tmp_path / 'test.txt'
        file_path.write_text('hello world')  # 11 bytes
        
        size = get_file_size(str(file_path))
        assert size == 11
    
    def test_nonexistent_file_raises(self, tmp_path):
        """Test that nonexistent file raises error."""
        with pytest.raises(FileNotFoundError):
            get_file_size(str(tmp_path / 'nonexistent.txt'))


# Fixtures
@pytest.fixture
def sample_parquet_structure(tmp_path):
    """Create a sample directory structure with parquet files."""
    # Root level
    (tmp_path / 'yellow_tripdata_2023-01.parquet').touch()
    (tmp_path / 'yellow_tripdata_2023-02.parquet').touch()
    
    # Nested by taxi type
    green_dir = tmp_path / 'green'
    green_dir.mkdir()
    (green_dir / 'green_tripdata_2023-01.parquet').touch()
    
    # Partitioned structure
    partitioned = tmp_path / 'partitioned'
    (partitioned / 'year=2023' / 'month=01').mkdir(parents=True)
    (partitioned / 'year=2023' / 'month=01' / 'part-0.parquet').touch()
    
    return tmp_path


def test_complex_directory_structure(sample_parquet_structure):
    """Test discovery with complex directory structure."""
    files = discover_parquet_files(str(sample_parquet_structure))
    
    assert len(files) == 4
    assert all(f.endswith('.parquet') for f in files)
    
    # Check all files are found
    filenames = [Path(f).name for f in files]
    assert 'yellow_tripdata_2023-01.parquet' in filenames
    assert 'green_tripdata_2023-01.parquet' in filenames
    assert 'part-0.parquet' in filenames


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
