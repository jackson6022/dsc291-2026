"""
S3 & File Discovery Utilities

This module provides utilities for working with both local and S3 file systems,
including path detection, filesystem abstraction, and recursive parquet file discovery.

Part 2 of the Taxi Data Pivoting Pipeline (15 pts)
"""

import os
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from urllib.parse import urlparse

# Configure logging
logger = logging.getLogger(__name__)


def is_s3_path(path: str) -> bool:
    """
    Check if a given path is an S3 path.
    
    Args:
        path: A file or directory path string.
        
    Returns:
        True if the path is an S3 path (starts with 's3://' or 's3a://'), 
        False otherwise.
        
    Examples:
        >>> is_s3_path('s3://my-bucket/data/')
        True
        >>> is_s3_path('s3a://my-bucket/data/')
        True
        >>> is_s3_path('/local/path/to/data')
        False
        >>> is_s3_path('./relative/path')
        False
    """
    if not isinstance(path, str):
        return False
    
    path_lower = path.lower().strip()
    return path_lower.startswith('s3://') or path_lower.startswith('s3a://')


def parse_s3_path(path: str) -> tuple[str, str]:
    """
    Parse an S3 path into bucket and key components.
    
    Args:
        path: An S3 path (e.g., 's3://bucket/prefix/file.parquet')
        
    Returns:
        Tuple of (bucket_name, key/prefix)
        
    Raises:
        ValueError: If the path is not a valid S3 path.
        
    Examples:
        >>> parse_s3_path('s3://my-bucket/data/file.parquet')
        ('my-bucket', 'data/file.parquet')
        >>> parse_s3_path('s3://my-bucket/')
        ('my-bucket', '')
    """
    if not is_s3_path(path):
        raise ValueError(f"Not a valid S3 path: {path}")
    
    # Handle both s3:// and s3a:// prefixes
    parsed = urlparse(path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    
    return bucket, key


def get_storage_options(path: str, anon: Optional[bool] = None) -> Dict[str, Any]:
    """
    Get storage options for reading/writing to a given path.
    
    For S3 paths, returns appropriate options for s3fs/pyarrow.
    For local paths, returns an empty dict.
    
    Args:
        path: A file or directory path (local or S3).
        anon: If True, use anonymous access for S3. If None, defaults to True
              for public buckets (will attempt anonymous first).
              
    Returns:
        Dictionary of storage options suitable for pandas/pyarrow S3 operations.
        
    Examples:
        >>> get_storage_options('/local/path')
        {}
        >>> get_storage_options('s3://nyc-tlc/trip data/', anon=True)
        {'anon': True}
    """
    if not is_s3_path(path):
        return {}
    
    options: Dict[str, Any] = {}
    
    # Default to anonymous access for public buckets
    if anon is None:
        # Try anonymous first - common for public datasets
        options['anon'] = True
    else:
        options['anon'] = anon
    
    return options


def get_filesystem(path: str, anon: Optional[bool] = None):
    """
    Get an appropriate filesystem object for the given path.
    
    Args:
        path: A file or directory path (local or S3).
        anon: If True, use anonymous access for S3. If None, attempts
              anonymous access first.
              
    Returns:
        For S3 paths: an s3fs.S3FileSystem instance
        For local paths: a pyarrow.fs.LocalFileSystem or fsspec LocalFileSystem
        
    Raises:
        ImportError: If required packages (s3fs, fsspec) are not installed.
        
    Examples:
        >>> fs = get_filesystem('s3://nyc-tlc/trip data/')
        >>> fs = get_filesystem('/local/data/')
    """
    if is_s3_path(path):
        try:
            import s3fs
        except ImportError:
            raise ImportError(
                "s3fs is required for S3 operations. "
                "Install with: pip install s3fs"
            )
        
        # Determine anonymous access setting
        use_anon = anon if anon is not None else True
        
        logger.debug(f"Creating S3FileSystem with anon={use_anon}")
        return s3fs.S3FileSystem(anon=use_anon)
    
    else:
        # For local filesystem, use fsspec's LocalFileSystem
        try:
            import fsspec
            return fsspec.filesystem('file')
        except ImportError:
            # Fallback to basic approach if fsspec not available
            try:
                from pyarrow.fs import LocalFileSystem
                return LocalFileSystem()
            except ImportError:
                raise ImportError(
                    "Either fsspec or pyarrow is required for filesystem operations. "
                    "Install with: pip install fsspec or pip install pyarrow"
                )


def _discover_local_parquet_files(input_path: str) -> List[str]:
    """
    Recursively discover all parquet files in a local directory.
    
    Args:
        input_path: Local directory or file path.
        
    Returns:
        Sorted list of absolute paths to parquet files.
    """
    path = Path(input_path).resolve()
    parquet_files: List[str] = []
    
    if path.is_file():
        if path.suffix.lower() == '.parquet':
            parquet_files.append(str(path))
    elif path.is_dir():
        # Recursive glob for all .parquet files
        for parquet_path in path.rglob('*.parquet'):
            if parquet_path.is_file():
                parquet_files.append(str(parquet_path))
    else:
        logger.warning(f"Path does not exist: {input_path}")
    
    # Sort for consistent ordering
    parquet_files.sort()
    
    logger.info(f"Discovered {len(parquet_files)} parquet file(s) in {input_path}")
    return parquet_files


def _discover_s3_parquet_files(input_path: str, anon: Optional[bool] = None) -> List[str]:
    """
    Recursively discover all parquet files in an S3 location.
    
    Args:
        input_path: S3 path (e.g., 's3://bucket/prefix/').
        anon: If True, use anonymous access.
        
    Returns:
        Sorted list of S3 paths to parquet files (with s3:// prefix).
    """
    fs = get_filesystem(input_path, anon=anon)
    bucket, prefix = parse_s3_path(input_path)
    
    parquet_files: List[str] = []
    
    # Construct the full S3 path for listing
    s3_path = f"{bucket}/{prefix}" if prefix else bucket
    
    try:
        # Check if it's a single file
        if s3_path.lower().endswith('.parquet'):
            if fs.exists(s3_path):
                parquet_files.append(f"s3://{s3_path}")
        else:
            # List all files recursively
            all_files = fs.find(s3_path)
            
            for file_path in all_files:
                if file_path.lower().endswith('.parquet'):
                    # Ensure proper s3:// prefix
                    if not file_path.startswith('s3://'):
                        file_path = f"s3://{file_path}"
                    parquet_files.append(file_path)
                    
    except Exception as e:
        logger.error(f"Error listing S3 path {input_path}: {e}")
        raise
    
    # Sort for consistent ordering
    parquet_files.sort()
    
    logger.info(f"Discovered {len(parquet_files)} parquet file(s) in {input_path}")
    return parquet_files


def discover_parquet_files(
    input_path: str,
    anon: Optional[bool] = None
) -> List[str]:
    """
    Recursively discover all parquet files in a local directory or S3 location.
    
    This function handles both local filesystem paths and S3 paths transparently.
    For S3 paths, it uses s3fs for efficient listing.
    
    Args:
        input_path: Path to a directory or file. Can be:
            - Local path: '/path/to/data/' or './relative/path/'
            - S3 path: 's3://bucket/prefix/' or 's3a://bucket/prefix/'
        anon: For S3 paths, whether to use anonymous access. 
              Defaults to True for public buckets.
              
    Returns:
        Sorted list of paths to parquet files. For S3, paths include 
        the 's3://' prefix. For local, paths are absolute.
        
    Raises:
        ValueError: If the input path is empty or invalid.
        FileNotFoundError: If a local path doesn't exist.
        ImportError: If required packages for S3 are not installed.
        
    Examples:
        >>> # Local discovery
        >>> files = discover_parquet_files('/data/taxi/')
        >>> print(files)
        ['/data/taxi/yellow_2023-01.parquet', '/data/taxi/yellow_2023-02.parquet']
        
        >>> # S3 discovery (public bucket)
        >>> files = discover_parquet_files('s3://nyc-tlc/trip data/')
        >>> print(files[:2])
        ['s3://nyc-tlc/trip data/yellow_tripdata_2023-01.parquet', ...]
    """
    if not input_path or not isinstance(input_path, str):
        raise ValueError("input_path must be a non-empty string")
    
    input_path = input_path.strip()
    
    # Check again after stripping whitespace
    if not input_path:
        raise ValueError("input_path must be a non-empty string")
    
    logger.debug(f"Discovering parquet files in: {input_path}")
    
    if is_s3_path(input_path):
        return _discover_s3_parquet_files(input_path, anon=anon)
    else:
        # Local path
        resolved_path = Path(input_path).resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(f"Local path does not exist: {input_path}")
        return _discover_local_parquet_files(input_path)


def validate_parquet_file(path: str, anon: Optional[bool] = None) -> bool:
    """
    Validate that a file exists and is a valid parquet file.
    
    Args:
        path: Path to the parquet file (local or S3).
        anon: For S3 paths, whether to use anonymous access.
        
    Returns:
        True if the file exists and appears to be a valid parquet file,
        False otherwise.
    """
    try:
        import pyarrow.parquet as pq
        
        if is_s3_path(path):
            fs = get_filesystem(path, anon=anon)
            bucket, key = parse_s3_path(path)
            full_path = f"{bucket}/{key}"
            
            # Try to read metadata only (fast validation)
            with fs.open(full_path, 'rb') as f:
                pq.read_metadata(f)
        else:
            # Local file
            if not Path(path).exists():
                return False
            pq.read_metadata(path)
        
        return True
        
    except Exception as e:
        logger.warning(f"Parquet validation failed for {path}: {e}")
        return False


# Convenience function for getting file size
def get_file_size(path: str, anon: Optional[bool] = None) -> int:
    """
    Get the size of a file in bytes.
    
    Args:
        path: Path to the file (local or S3).
        anon: For S3 paths, whether to use anonymous access.
        
    Returns:
        File size in bytes.
        
    Raises:
        FileNotFoundError: If the file doesn't exist.
    """
    if is_s3_path(path):
        fs = get_filesystem(path, anon=anon)
        bucket, key = parse_s3_path(path)
        full_path = f"{bucket}/{key}"
        info = fs.info(full_path)
        return info.get('size', info.get('Size', 0))
    else:
        return Path(path).stat().st_size


if __name__ == "__main__":
    # Simple test/demo when run directly
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) > 1:
        test_path = sys.argv[1]
    else:
        test_path = "."
    
    print(f"\nTesting path: {test_path}")
    print(f"Is S3 path: {is_s3_path(test_path)}")
    
    try:
        files = discover_parquet_files(test_path)
        print(f"\nDiscovered {len(files)} parquet file(s):")
        for f in files[:10]:  # Show first 10
            print(f"  - {f}")
        if len(files) > 10:
            print(f"  ... and {len(files) - 10} more")
    except Exception as e:
        print(f"Error: {e}")
