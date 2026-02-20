"""
Part 1: PCA on Unnormalized Pivot Tables.

Loads the wide table from S3, extracts the feature matrix (hour_0..hour_23),
fits PCA using Dask for covariance computation (averaging outer products once),
and saves the model and variance-explained plot.
"""

from __future__ import annotations

import pickle
import logging
from pathlib import Path

import dask.array as da
import dask.dataframe as dd
import matplotlib.pyplot as plt
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default paths
WIDE_PARQUET_PATH = "s3://291-s3-bucket/wide.parquet"
HOUR_COLS = [f"hour_{i}" for i in range(24)]
INDEX_COLS = ["taxi_type", "date", "pickup_place"]


def load_and_extract_x(
    path: str = WIDE_PARQUET_PATH,
    storage_options: dict | None = None,
) -> dd.DataFrame:
    """
    Load the wide parquet and return the feature matrix as a Dask DataFrame.

    Rows are (taxi_type, date, pickup_place); columns are hour_0..hour_23.
    """
    if storage_options is None:
        storage_options = {"anon": True}
    logger.info("Loading %s", path)
    df = dd.read_parquet(path, storage_options=storage_options)
    # Ensure we have the hour columns
    missing = [c for c in HOUR_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing hour columns: {missing}")
    X = df[HOUR_COLS].copy()
    return X


def column_means_excluding_nan(X: dd.DataFrame) -> dd.Series:
    """Compute mean of each column, excluding NaN (for imputation)."""
    return X.mean(skipna=True)


def fit_pca_dask(
    X: dd.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Fit PCA using Dask: impute NaN with column mean, then compute covariance
    via Dask arrays -- sum of outer products divided by n once.

    Returns:
        components: orthonormal matrix (eigenvectors), shape (24, 24)
        variances: eigenvalues (explained variance), shape (24,)
    """
    logger.info("Computing column means (excluding NaN) via Dask...")
    means = column_means_excluding_nan(X)
    means_np = means.compute().values

    logger.info("Imputing NaN and centering via Dask...")
    X_filled = X.fillna(dict(zip(HOUR_COLS, means_np)))
    X_centered = X_filled - means_np

    logger.info("Computing covariance matrix via Dask (X^T @ X / n)...")
    X_da = X_centered.to_dask_array(lengths=True)
    n = X_da.shape[0]
    cov = (X_da.T @ X_da) / n
    cov = cov.compute()
    logger.info("Covariance shape %s, n = %s", cov.shape, n)

    eigenvalues, eigenvectors = np.linalg.eigh(cov)
    idx = np.argsort(eigenvalues)[::-1]
    variances = eigenvalues[idx]
    components = eigenvectors[:, idx]
    return components.astype(np.float64), variances.astype(np.float64)


def save_model(components: np.ndarray, variances: np.ndarray, out_path: str = "pca_model.pkl") -> None:
    """Save orthonormal matrix (components) and variances to pkl."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump({"components": components, "variances": variances}, f)
    logger.info("Saved PCA model to %s", out_path)


def plot_variance_explained(variances: np.ndarray, out_path: str = "variance_explained.png") -> None:
    """Plot explained variance (e.g. scree and/or cumulative) and save."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    total = variances.sum()
    explained_ratio = variances / total
    cumsum = np.cumsum(explained_ratio)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    ax1.bar(range(len(variances)), explained_ratio, alpha=0.8, label="Per component")
    ax1.set_xlabel("Principal component")
    ax1.set_ylabel("Variance explained ratio")
    ax1.set_title("Scree plot (unnormalized PCA)")
    ax1.legend()
    ax2.plot(range(len(cumsum)), cumsum, "o-")
    ax2.set_xlabel("Number of components")
    ax2.set_ylabel("Cumulative variance explained")
    ax2.set_title("Cumulative variance explained")
    ax2.set_ylim(0, 1.05)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Saved variance plot to %s", out_path)


def main(
    input_path: str = WIDE_PARQUET_PATH,
    output_dir: str = ".",
    storage_options: dict | None = None,
) -> None:
    output_dir = Path(output_dir)
    X = load_and_extract_x(path=input_path, storage_options=storage_options)
    components, variances = fit_pca_dask(X)
    save_model(components, variances, out_path=str(output_dir / "pca_model.pkl"))
    plot_variance_explained(variances, out_path=str(output_dir / "variance_explained.png"))


if __name__ == "__main__":
    import argparse
    import os
    parser = argparse.ArgumentParser(description="Part 1: PCA on unnormalized wide table")
    parser.add_argument("--input", default=WIDE_PARQUET_PATH, help="Path to wide.parquet (S3 or local)")
    parser.add_argument("--output-dir", default=".", help="Directory for pca_model.pkl and variance_explained.png")
    parser.add_argument("--anon", action="store_true", help="Use anonymous S3 access (default: use credentials from env)")
    args = parser.parse_args()
    # If --anon is set, use anonymous access; otherwise, use credentials from environment
    if args.anon:
        opts = {"anon": True}
    else:
        # Get credentials from environment variables
        opts = {}
        if os.getenv("AWS_ACCESS_KEY_ID"):
            opts["key"] = os.getenv("AWS_ACCESS_KEY_ID")
        if os.getenv("AWS_SECRET_ACCESS_KEY"):
            opts["secret"] = os.getenv("AWS_SECRET_ACCESS_KEY")
        if os.getenv("AWS_SESSION_TOKEN"):
            opts["token"] = os.getenv("AWS_SESSION_TOKEN")
        # If no credentials found, use None (s3fs will try to use default boto3 credentials)
        if not opts:
            opts = None
    main(input_path=args.input, output_dir=args.output_dir, storage_options=opts)
