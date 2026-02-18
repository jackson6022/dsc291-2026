#!/usr/bin/env python3
"""
List contents of the course S3 bucket to find zone lookup / CSV files.

Run from repo root with AWS credentials set (or --anon for public bucket):

  python mapping_pca_and_bootstrap_stability/list_s3_bucket.py
  python mapping_pca_and_bootstrap_stability/list_s3_bucket.py --prefix taxi/
  python mapping_pca_and_bootstrap_stability/list_s3_bucket.py --prefix "" --anon
"""

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="List S3 bucket contents (e.g. find zone CSV)")
    parser.add_argument(
        "--bucket",
        default="dsc291-ucsd",
        help="S3 bucket name (default: dsc291-ucsd)",
    )
    parser.add_argument(
        "--prefix",
        default="taxi/",
        help="Prefix to list (default: taxi/). Use empty string to list bucket root.",
    )
    parser.add_argument("--anon", action="store_true", help="Use anonymous access")
    parser.add_argument("--csv-only", action="store_true", help="Only print paths ending in .csv")
    args = parser.parse_args()

    try:
        import s3fs
    except ImportError:
        print("Install s3fs: pip install s3fs", file=sys.stderr)
        sys.exit(1)

    fs = s3fs.S3FileSystem(anon=args.anon)
    bucket_path = f"{args.bucket}/{args.prefix}".rstrip("/")
    if args.prefix:
        bucket_path += "/"

    print(f"Listing s3://{bucket_path} (anon={args.anon})...")
    try:
        entries = fs.find(bucket_path, withdirs=True)
        paths = sorted(set(entries))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    for p in paths:
        if not p.startswith(args.bucket + "/"):
            p = f"{args.bucket}/{p}"
        if args.csv_only and not p.lower().endswith(".csv"):
            continue
        size = ""
        try:
            info = fs.info(p)
            if "Size" in info and info["Size"]:
                size = f"  ({info['Size']:,} bytes)"
            elif "size" in info and info["size"]:
                size = f"  ({info['size']:,} bytes)"
        except Exception:
            pass
        print(f"s3://{p}{size}")

    print(f"\nTotal: {len(paths)} entries.")


if __name__ == "__main__":
    main()
