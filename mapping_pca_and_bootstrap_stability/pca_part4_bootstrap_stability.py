"""
Part 4: Bootstrap Stability of Eigenvectors.

Resamples rows of the wide table with replacement, fits PCA on each
bootstrap replicate, and compares the resulting eigenvectors to the
original PCA basis from Part 1.

Outputs:
  - bootstrap_stability_report.json
  - eigenvector_stability.png
"""

from __future__ import annotations

import json
import logging
import pickle
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np

from .pca_part1_unnormalized import (
    WIDE_PARQUET_PATH,
    HOUR_COLS,
    load_and_extract_x,
    fit_pca_dask,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class MetricSummary:
    mean: float
    std: float
    min: float
    max: float
    p5: float
    p25: float
    p50: float
    p75: float
    p95: float
    ci_lower: float  # 95% CI lower bound
    ci_upper: float  # 95% CI upper bound
    values: List[float]


@dataclass
class BootstrapReport:
    B: int
    K: int
    n_rows: int
    n_features: int
    subspace_affinity: MetricSummary
    procrustes_distance: MetricSummary
    component_correlations: Dict[str, MetricSummary]
    interpretation: Dict[str, str]  # Human-readable interpretations


def compute_subspace_affinity(
    U: np.ndarray, V: np.ndarray
) -> float:
    """
    Compute subspace affinity between column spaces of U and V.

    U, V: shape (d, K), orthonormal columns.
    We use ||U^T V||_F^2 / K, which lies in [0, 1].
    """
    M = U.T @ V  # shape (K, K)
    frob_sq = np.sum(M * M)
    return float(frob_sq / U.shape[1])


def compute_procrustes_distance(
    U: np.ndarray, V: np.ndarray
) -> float:
    """
    Procrustes distance between two orthonormal bases U and V.

    We solve min_Q ||U - V Q||_F over orthogonal Q via SVD of V^T U.
    Returns the minimized Frobenius norm.
    """
    M = V.T @ U
    U_svd, _, Vt_svd = np.linalg.svd(M)
    Q = U_svd @ Vt_svd
    diff = U - V @ Q
    return float(np.linalg.norm(diff, ord="fro"))


def component_wise_correlation(
    U: np.ndarray, V: np.ndarray
) -> np.ndarray:
    """
    Compute component-wise Pearson correlation between corresponding
    eigenvectors (columns of U and V).

    Returns array of shape (K,) with correlation for each component.
    """
    K = U.shape[1]
    corrs = np.empty(K, dtype=float)
    for k in range(K):
        u = U[:, k]
        v = V[:, k]
        # Guard against zero-variance vectors
        if np.allclose(u, 0) or np.allclose(v, 0):
            corrs[k] = np.nan
            continue
        cov = np.cov(u, v)
        # np.cov returns 2x2 matrix; off-diagonal is covariance
        denom = np.sqrt(cov[0, 0] * cov[1, 1])
        corrs[k] = float(cov[0, 1] / denom) if denom > 0 else np.nan
    return corrs


def compute_eigenvector_angle(u: np.ndarray, v: np.ndarray) -> float:
    """Compute angle (in degrees) between two eigenvectors."""
    cos_angle = np.clip(np.dot(u, v), -1.0, 1.0)
    return float(np.arccos(abs(cos_angle)) * 180 / np.pi)


def bootstrap_pca_stability(
    X_arr: np.ndarray,
    base_components: np.ndarray,
    B: int = 100,
    K: int = 2,
    random_state: int | None = 42,
) -> Tuple[BootstrapReport, Tuple[np.ndarray, np.ndarray | None]]:
    """
    Run B bootstrap replicates and compute stability metrics.

    Args:
        X_arr: (n, d) numpy array of centered feature matrix.
        base_components: (d, d) PCA components from full data.
        B: number of bootstrap replicates.
        K: number of leading components to compare.
        random_state: seed for reproducibility.

    Returns:
        report: BootstrapReport dataclass with summary statistics.
        bootstrap_components: tuple of (pc1_bootstrap_components, pc2_bootstrap_components | None)
            where pc1_bootstrap_components is shape (B, d) and pc2_bootstrap_components is shape (B, d) if K >= 2.
    """
    rng = np.random.default_rng(random_state)
    n, d = X_arr.shape
    logger.info("Bootstrap PCA stability: n=%d, d=%d, B=%d, K=%d", n, d, B, K)

    U_ref = base_components[:, :K]

    subspace_affinities: List[float] = []
    procrustes_distances: List[float] = []
    comp_corrs_all: List[np.ndarray] = []
    pc1_bootstrap_components = np.empty((B, d), dtype=float)
    pc2_bootstrap_components = np.empty((B, d), dtype=float) if K >= 2 else None

    for b in range(B):
        idx = rng.integers(low=0, high=n, size=n)
        X_boot = X_arr[idx, :]
        cov_boot = (X_boot.T @ X_boot) / X_boot.shape[0]
        eigvals, eigvecs = np.linalg.eigh(cov_boot)
        order = np.argsort(eigvals)[::-1]
        eigvecs = eigvecs[:, order]

        U_boot = eigvecs[:, :K]

        aff = compute_subspace_affinity(U_ref, U_boot)
        dist = compute_procrustes_distance(U_ref, U_boot)
        corrs = component_wise_correlation(U_ref, U_boot)

        subspace_affinities.append(aff)
        procrustes_distances.append(dist)
        comp_corrs_all.append(corrs)
        pc1_bootstrap_components[b, :] = eigvecs[:, 0]
        if K >= 2 and pc2_bootstrap_components is not None:
            pc2_bootstrap_components[b, :] = eigvecs[:, 1]

        if (b + 1) % max(1, B // 10) == 0:
            logger.info("Completed %d / %d bootstraps", b + 1, B)

    comp_corrs_all_arr = np.vstack(comp_corrs_all)  # shape (B, K)

    def summarize(values: np.ndarray) -> MetricSummary:
        finite_vals = values[np.isfinite(values)]
        if finite_vals.size == 0:
            return MetricSummary(
                mean=float("nan"), std=float("nan"), min=float("nan"), max=float("nan"),
                p5=float("nan"), p25=float("nan"), p50=float("nan"), p75=float("nan"), p95=float("nan"),
                ci_lower=float("nan"), ci_upper=float("nan"), values=[]
            )
        # Bootstrap 95% CI using percentile method
        ci_lower = float(np.percentile(finite_vals, 2.5))
        ci_upper = float(np.percentile(finite_vals, 97.5))
        return MetricSummary(
            mean=float(np.mean(finite_vals)),
            std=float(np.std(finite_vals)),
            min=float(np.min(finite_vals)),
            max=float(np.max(finite_vals)),
            p5=float(np.percentile(finite_vals, 5)),
            p25=float(np.percentile(finite_vals, 25)),
            p50=float(np.percentile(finite_vals, 50)),
            p75=float(np.percentile(finite_vals, 75)),
            p95=float(np.percentile(finite_vals, 95)),
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            values=[float(v) for v in finite_vals],
        )

    subspace_summary = summarize(np.asarray(subspace_affinities))
    procrustes_summary = summarize(np.asarray(procrustes_distances))

    component_summaries: Dict[str, MetricSummary] = {}
    for k in range(K):
        component_summaries[f"PC{k+1}"] = summarize(comp_corrs_all_arr[:, k])

    # Generate interpretations
    interpretations = generate_interpretations(
        subspace_summary, procrustes_summary, component_summaries, K
    )

    report = BootstrapReport(
        B=B,
        K=K,
        n_rows=n,
        n_features=d,
        subspace_affinity=subspace_summary,
        procrustes_distance=procrustes_summary,
        component_correlations=component_summaries,
        interpretation=interpretations,
    )

    bootstrap_components = (pc1_bootstrap_components, pc2_bootstrap_components)
    return report, bootstrap_components


def generate_interpretations(
    subspace_affinity: MetricSummary,
    procrustes_distance: MetricSummary,
    component_correlations: Dict[str, MetricSummary],
    K: int,
) -> Dict[str, str]:
    """Generate human-readable interpretations of stability metrics."""
    interpretations = {}
    
    # Subspace affinity interpretation
    aff_mean = subspace_affinity.mean
    if aff_mean > 0.99:
        aff_interp = f"Excellent stability: subspace affinity = {aff_mean:.6f} (very close to 1.0). The top-{K} dimensional subspace is highly consistent across bootstrap replicates."
    elif aff_mean > 0.95:
        aff_interp = f"Good stability: subspace affinity = {aff_mean:.6f}. The top-{K} dimensional subspace is reasonably consistent, with minor variations."
    elif aff_mean > 0.90:
        aff_interp = f"Moderate stability: subspace affinity = {aff_mean:.6f}. Some variability in the subspace structure across bootstraps."
    else:
        aff_interp = f"Low stability: subspace affinity = {aff_mean:.6f}. Significant variability in the subspace structure. Consider investigating data quality or using fewer components."
    interpretations["subspace_affinity"] = aff_interp
    
    # Procrustes distance interpretation
    proc_mean = procrustes_distance.mean
    proc_std = procrustes_distance.std
    if proc_mean < 0.01:
        proc_interp = f"Very stable alignment: Procrustes distance = {proc_mean:.6f} ± {proc_std:.6f}. Eigenvectors align almost perfectly after optimal rotation."
    elif proc_mean < 0.05:
        proc_interp = f"Stable alignment: Procrustes distance = {proc_mean:.6f} ± {proc_std:.6f}. Good consistency in eigenvector directions."
    elif proc_mean < 0.10:
        proc_interp = f"Moderate alignment: Procrustes distance = {proc_mean:.6f} ± {proc_std:.6f}. Some variability in eigenvector directions."
    else:
        proc_interp = f"Unstable alignment: Procrustes distance = {proc_mean:.6f} ± {proc_std:.6f}. Significant variability in eigenvector directions."
    interpretations["procrustes_distance"] = proc_interp
    
    # Component-wise correlation interpretations
    for k in range(K):
        key = f"PC{k+1}"
        corr_summary = component_correlations[key]
        corr_mean = corr_summary.mean
        corr_ci_lower = corr_summary.ci_lower
        corr_ci_upper = corr_summary.ci_upper
        
        if corr_mean > 0.99:
            corr_interp = f"Excellent: PC{k+1} correlation = {corr_mean:.4f} (95% CI: [{corr_ci_lower:.4f}, {corr_ci_upper:.4f}]). This component is extremely stable across bootstraps."
        elif corr_mean > 0.95:
            corr_interp = f"Very good: PC{k+1} correlation = {corr_mean:.4f} (95% CI: [{corr_ci_lower:.4f}, {corr_ci_upper:.4f}]). This component is highly stable."
        elif corr_mean > 0.90:
            corr_interp = f"Good: PC{k+1} correlation = {corr_mean:.4f} (95% CI: [{corr_ci_lower:.4f}, {corr_ci_upper:.4f}]). This component shows good stability."
        elif corr_mean > 0.80:
            corr_interp = f"Moderate: PC{k+1} correlation = {corr_mean:.4f} (95% CI: [{corr_ci_lower:.4f}, {corr_ci_upper:.4f}]). Some variability in this component."
        else:
            corr_interp = f"Low stability: PC{k+1} correlation = {corr_mean:.4f} (95% CI: [{corr_ci_lower:.4f}, {corr_ci_upper:.4f}]). This component varies substantially across bootstraps."
        interpretations[key] = corr_interp
    
    # Overall summary
    avg_corr = np.mean([component_correlations[f"PC{k+1}"].mean for k in range(K)])
    if avg_corr > 0.95 and aff_mean > 0.99:
        overall = "Overall: The PCA eigenvectors are highly stable. The results are robust to sampling variation and can be reliably used for downstream analysis."
    elif avg_corr > 0.90 and aff_mean > 0.95:
        overall = "Overall: The PCA eigenvectors show good stability. Minor variations exist but should not significantly impact interpretations."
    else:
        overall = "Overall: The PCA eigenvectors show moderate to low stability. Consider investigating sources of variability or using robust alternatives."
    interpretations["overall_summary"] = overall
    
    return interpretations


def save_report(report: BootstrapReport, out_path: str) -> None:
    """Save the bootstrap report as JSON and generate a human-readable summary."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    
    # Save full JSON report
    with open(out, "w") as f:
        json.dump(asdict(report), f, indent=2)
    logger.info("Saved bootstrap report to %s", out)
    
    # Generate and save human-readable summary
    summary_path = out.parent / (out.stem + "_summary.txt")
    with open(summary_path, "w") as f:
        f.write("=" * 80 + "\n")
        f.write("BOOTSTRAP STABILITY ANALYSIS SUMMARY\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Dataset: {report.n_rows:,} rows × {report.n_features} features\n")
        f.write(f"Bootstrap replicates: {report.B}\n")
        f.write(f"Components analyzed: {report.K}\n\n")
        
        f.write("-" * 80 + "\n")
        f.write("METRICS\n")
        f.write("-" * 80 + "\n\n")
        
        f.write("Subspace Affinity:\n")
        aff = report.subspace_affinity
        f.write(f"  Mean: {aff.mean:.6f} ± {aff.std:.6f}\n")
        f.write(f"  95% CI: [{aff.ci_lower:.6f}, {aff.ci_upper:.6f}]\n")
        f.write(f"  Range: [{aff.min:.6f}, {aff.max:.6f}]\n")
        f.write(f"  Interpretation: {report.interpretation['subspace_affinity']}\n\n")
        
        f.write("Procrustes Distance:\n")
        proc = report.procrustes_distance
        f.write(f"  Mean: {proc.mean:.6f} ± {proc.std:.6f}\n")
        f.write(f"  95% CI: [{proc.ci_lower:.6f}, {proc.ci_upper:.6f}]\n")
        f.write(f"  Range: [{proc.min:.6f}, {proc.max:.6f}]\n")
        f.write(f"  Interpretation: {report.interpretation['procrustes_distance']}\n\n")
        
        f.write("Component-wise Correlations:\n")
        for k in range(report.K):
            key = f"PC{k+1}"
            corr = report.component_correlations[key]
            f.write(f"  {key}:\n")
            f.write(f"    Mean: {corr.mean:.4f} ± {corr.std:.4f}\n")
            f.write(f"    95% CI: [{corr.ci_lower:.4f}, {corr.ci_upper:.4f}]\n")
            f.write(f"    Range: [{corr.min:.4f}, {corr.max:.4f}]\n")
            f.write(f"    Interpretation: {report.interpretation[key]}\n\n")
        
        f.write("-" * 80 + "\n")
        f.write("OVERALL ASSESSMENT\n")
        f.write("-" * 80 + "\n\n")
        f.write(report.interpretation["overall_summary"] + "\n\n")
        
        f.write("=" * 80 + "\n")
    
    logger.info("Saved human-readable summary to %s", summary_path)


def plot_eigenvector_stability(
    base_components: np.ndarray,
    bootstrap_components: Tuple[np.ndarray, np.ndarray | None],
    report: BootstrapReport,
    out_path: str,
) -> None:
    """
    Compact visualization: PC1 band, PC2 band, boxplot of correlations, and summary.
    """
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    d = base_components.shape[0]
    x = np.arange(d)
    pc1_ref = base_components[:, 0]
    pc1_bootstrap_components, pc2_bootstrap_components = bootstrap_components

    n_cols = 3  # PC1 band, PC2 band (or empty), boxplot
    fig = plt.figure(figsize=(20, 9))
    gs = fig.add_gridspec(2, n_cols, hspace=0.4, wspace=0.3)

    # --- Row 1: PC1 band ---
    ax1 = fig.add_subplot(gs[0, 0])
    q05_pc1 = np.nanpercentile(pc1_bootstrap_components, 5, axis=0)
    q50_pc1 = np.nanpercentile(pc1_bootstrap_components, 50, axis=0)
    q95_pc1 = np.nanpercentile(pc1_bootstrap_components, 95, axis=0)
    ax1.fill_between(x, q05_pc1, q95_pc1, color="lightblue", alpha=0.5, label="5–95% band")
    ax1.plot(x, q50_pc1, color="blue", linestyle="--", linewidth=1.5, label="Median")
    ax1.plot(x, pc1_ref, color="black", linewidth=2, label="Original")
    ax1.set_xlabel("Hour index")
    ax1.set_ylabel("PC1 loading")
    ax1.set_title("PC1 Bootstrap Band", fontsize=11, fontweight="bold")
    ax1.set_xticks(x[::4])
    ax1.grid(True, alpha=0.3)
    ax1.legend(fontsize=8)

    # --- Row 1: PC2 band (if available) ---
    if pc2_bootstrap_components is not None:
        ax2 = fig.add_subplot(gs[0, 1])
        pc2_ref = base_components[:, 1]
        q05_pc2 = np.nanpercentile(pc2_bootstrap_components, 5, axis=0)
        q50_pc2 = np.nanpercentile(pc2_bootstrap_components, 50, axis=0)
        q95_pc2 = np.nanpercentile(pc2_bootstrap_components, 95, axis=0)
        ax2.fill_between(x, q05_pc2, q95_pc2, color="lightcoral", alpha=0.5, label="5–95% band")
        ax2.plot(x, q50_pc2, color="red", linestyle="--", linewidth=1.5, label="Median")
        ax2.plot(x, pc2_ref, color="black", linewidth=2, label="Original")
        ax2.set_xlabel("Hour index")
        ax2.set_ylabel("PC2 loading")
        ax2.set_title("PC2 Bootstrap Band", fontsize=11, fontweight="bold")
        ax2.set_xticks(x[::4])
        ax2.grid(True, alpha=0.3)
        ax2.legend(fontsize=8)
        boxplot_col = 2
    else:
        boxplot_col = 1

    # --- Row 1: Boxplot of component-wise correlations ---
    ax_box = fig.add_subplot(gs[0, boxplot_col])
    comp_corrs = report.component_correlations
    labels = []
    data = []
    for k in range(report.K):
        key = f"PC{k+1}"
        labels.append(key)
        data.append(comp_corrs[key].values)
    bp = ax_box.boxplot(data, labels=labels, showmeans=True, patch_artist=True)
    for patch in bp["boxes"]:
        patch.set_facecolor("lightgreen")
        patch.set_alpha(0.7)
    ax_box.set_ylabel("Correlation (bootstrap vs original)")
    ax_box.set_title("Component-wise Correlations", fontsize=11, fontweight="bold")
    ax_box.grid(True, axis="y", alpha=0.3)
    # Use data-driven ylim so the box is visible; start at 0.9998 (no values below that)
    all_vals = [v for vals in data for v in vals]
    if all_vals:
        y_min = 0.9998
        y_max = min(1.0, max(all_vals) + 0.0001)
        ax_box.set_ylim([y_min, y_max])

    # --- Row 2: Summary text ---
    ax_sum = fig.add_subplot(gs[1, :])
    ax_sum.axis("off")
    summary_text = "Summary  |  "
    summary_text += f"Subspace affinity: {report.subspace_affinity.mean:.6f} ± {report.subspace_affinity.std:.6f}  |  "
    summary_text += f"Procrustes dist: {report.procrustes_distance.mean:.6f} ± {report.procrustes_distance.std:.6f}  |  "
    for k in range(report.K):
        corr = report.component_correlations[f"PC{k+1}"]
        summary_text += f"PC{k+1} corr: {corr.mean:.4f} ± {corr.std:.4f}  |  "
    summary_text += "\n\n" + report.interpretation["overall_summary"]
    ax_sum.text(0.05, 0.95, summary_text, transform=ax_sum.transAxes, fontsize=9,
                verticalalignment="top",
                bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5))

    plt.suptitle("Bootstrap Eigenvector Stability", fontsize=12, fontweight="bold", y=1.02)
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Saved eigenvector stability plot to %s", out)


def load_pca_model(path: str) -> np.ndarray:
    """Load PCA components from Part 1's pca_model.pkl. Returns components array (d, d)."""
    with open(path, "rb") as f:
        obj = pickle.load(f)
    return obj["components"]


def main(
    input_path: str = WIDE_PARQUET_PATH,
    output_dir: str = ".",
    pca_model_path: str | None = None,
    B: int = 100,
    K: int = 2,
    storage_options: dict | None = None,
) -> None:
    """
    Entry point for Part 4 bootstrap stability analysis.

    Data (rows to resample) is always read from input_path (S3 or local Parquet).
    Reference PCA: if pca_model_path is set, load from Part 1's pca_model.pkl;
    otherwise recompute PCA from the same Parquet.
    """
    output_dir_path = Path(output_dir)

    # Load wide table (from S3 or local) for bootstrap resampling
    X_dask = load_and_extract_x(path=input_path, storage_options=storage_options)

    if pca_model_path:
        logger.info("Loading reference PCA from Part 1 output: %s", pca_model_path)
        components = load_pca_model(pca_model_path)
    else:
        logger.info("Recomputing reference PCA from input data")
        components, _ = fit_pca_dask(X_dask)

    # Convert to centered numpy array for bootstrapping
    logger.info("Converting data to numpy array for bootstrapping...")
    X_pd = X_dask.compute()
    means = X_pd[HOUR_COLS].mean(skipna=True).values
    X_filled = X_pd[HOUR_COLS].fillna(dict(zip(HOUR_COLS, means)))
    X_centered = X_filled.values - means

    report, bootstrap_components = bootstrap_pca_stability(
        X_centered,
        components,
        B=B,
        K=K,
    )

    save_report(report, out_path=str(output_dir_path / "bootstrap_stability_report.json"))
    plot_eigenvector_stability(
        components,
        bootstrap_components,
        report,
        out_path=str(output_dir_path / "eigenvector_stability.png"),
    )


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description="Part 4: Bootstrap stability of PCA eigenvectors"
    )
    parser.add_argument(
        "--input",
        default=WIDE_PARQUET_PATH,
        help="Path to wide.parquet (S3 or local)",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory for bootstrap_stability_report.json and eigenvector_stability.png",
    )
    parser.add_argument(
        "--pca-model",
        default=None,
        help="Path to Part 1's pca_model.pkl; if set, use it as reference PCA instead of recomputing",
    )
    parser.add_argument(
        "--B",
        type=int,
        default=100,
        help="Number of bootstrap replicates",
    )
    parser.add_argument(
        "--K",
        type=int,
        default=2,
        help="Number of leading components to evaluate",
    )
    parser.add_argument(
        "--anon",
        action="store_true",
        help="Use anonymous S3 access (default: use credentials from env)",
    )
    args = parser.parse_args()

    if args.anon:
        opts = {"anon": True}
    else:
        opts = {}
        if os.getenv("AWS_ACCESS_KEY_ID"):
            opts["key"] = os.getenv("AWS_ACCESS_KEY_ID")
        if os.getenv("AWS_SECRET_ACCESS_KEY"):
            opts["secret"] = os.getenv("AWS_SECRET_ACCESS_KEY")
        if os.getenv("AWS_SESSION_TOKEN"):
            opts["token"] = os.getenv("AWS_SESSION_TOKEN")
        if not opts:
            opts = None

    main(
        input_path=args.input,
        output_dir=args.output_dir,
        pca_model_path=args.pca_model,
        B=args.B,
        K=args.K,
        storage_options=opts,
    )

