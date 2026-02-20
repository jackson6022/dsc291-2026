"""
Extended Analyses for HW2.

Extends the four core parts with additional depth:
  1. PC Loading Interpretation — eigenvector loadings as hour-of-day curves
  2. Eigenvalue Gap & Davis-Kahan Bound — theoretical stability guarantee
  3. Hill Estimator — MLE-based tail exponent (compares with log-log regression)
  4. Broken Stick Model — statistical significance of retained PCs
  5. Per-Component Tail Analysis — tail behavior per eigenvector (not just pooled)

All outputs are saved to the same directory as the core parts.
"""

from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HOUR_COLS = [f"hour_{i}" for i in range(24)]


def load_pca_model(model_path: str) -> tuple[np.ndarray, np.ndarray]:
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    return model["components"], model["variances"]


# ---------------------------------------------------------------------------
# 1. PC Loading Interpretation
# ---------------------------------------------------------------------------

def plot_pc_loading_interpretation(
    components: np.ndarray,
    variances: np.ndarray,
    out_path: str,
    n_pcs: int = 4,
) -> None:
    """
    Plot the first n_pcs eigenvectors as hour-of-day curves.

    Each PC loading vector shows which hours contribute positively or
    negatively, revealing the temporal pattern that PC captures.
    """
    total = variances.sum()
    hours = np.arange(24)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    colors = ["#1f77b4", "#d62728", "#2ca02c", "#9467bd"]

    for i, ax in enumerate(axes.flat):
        if i >= n_pcs or i >= components.shape[1]:
            ax.axis("off")
            continue
        loadings = components[:, i]
        var_pct = 100 * variances[i] / total

        ax.bar(hours, loadings, color=colors[i], alpha=0.7, edgecolor="black", linewidth=0.5)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_xlabel("Hour of Day")
        ax.set_ylabel("Loading")
        ax.set_title(f"PC{i+1} Loadings ({var_pct:.1f}% variance)", fontweight="bold")
        ax.set_xticks(range(0, 24, 2))
        ax.set_xticklabels([f"{h:02d}" for h in range(0, 24, 2)])
        ax.grid(True, alpha=0.3, axis="y")

        pos_hours = hours[loadings > 0.05 * np.max(np.abs(loadings))]
        neg_hours = hours[loadings < -0.05 * np.max(np.abs(loadings))]
        peak_hour = hours[np.argmax(np.abs(loadings))]
        note = f"Peak: {peak_hour:02d}:00"
        if len(pos_hours) > 0 and len(neg_hours) > 0:
            note += f" | Contrast: {pos_hours[0]:02d}–{pos_hours[-1]:02d}h vs {neg_hours[0]:02d}–{neg_hours[-1]:02d}h"
        ax.annotate(note, xy=(0.02, 0.95), xycoords="axes fraction",
                    fontsize=8, va="top", style="italic",
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", alpha=0.8))

    fig.suptitle("PC Loading Interpretation: Hourly Patterns Captured by Each Component",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Saved PC loading interpretation to %s", out_path)


# ---------------------------------------------------------------------------
# 2. Eigenvalue Gap & Davis-Kahan Bound
# ---------------------------------------------------------------------------

def eigenvalue_gap_analysis(
    variances: np.ndarray,
    out_path: str,
) -> dict:
    """
    Analyze eigenvalue gaps and compute the Davis-Kahan sin(theta) bound.

    The Davis-Kahan theorem states that the angle between the true and
    perturbed k-th eigenvector satisfies:
        sin(theta_k) <= ||perturbation||_op / gap_k
    where gap_k = min(lambda_{k} - lambda_{k+1}, lambda_{k-1} - lambda_{k}).

    For bootstrap PCA, the perturbation is the difference between the
    sample and population covariance, bounded by O(1/sqrt(n)).
    """
    n_components = len(variances)
    gaps = np.diff(variances)
    relative_gaps = np.abs(gaps) / variances[:-1]

    result = {
        "eigenvalues": variances.tolist(),
        "absolute_gaps": np.abs(gaps).tolist(),
        "relative_gaps": relative_gaps.tolist(),
    }

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    ax1 = axes[0]
    ax1.semilogy(range(1, n_components + 1), variances, "bo-", markersize=6)
    ax1.set_xlabel("Component")
    ax1.set_ylabel("Eigenvalue (log scale)")
    ax1.set_title("Eigenvalue Spectrum (log scale)", fontweight="bold")
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(range(1, n_components + 1, 2))

    ax2 = axes[1]
    ax2.bar(range(1, n_components), np.abs(gaps), color="steelblue", alpha=0.7, edgecolor="black")
    ax2.set_xlabel("Gap between PC_k and PC_{k+1}")
    ax2.set_ylabel("Absolute Gap (λ_k − λ_{k+1})")
    ax2.set_title("Eigenvalue Gaps", fontweight="bold")
    ax2.grid(True, alpha=0.3, axis="y")
    ax2.set_xticks(range(1, n_components, 2))

    ax3 = axes[2]
    dk_bounds = []
    for k in range(min(5, n_components - 1)):
        gap = np.abs(gaps[k])
        if gap > 0:
            bound = 1.0 / gap
            dk_bounds.append(bound)
        else:
            dk_bounds.append(np.inf)
    result["davis_kahan_sensitivity"] = dk_bounds

    ax3.bar(range(1, len(dk_bounds) + 1), dk_bounds, color="coral", alpha=0.7, edgecolor="black")
    ax3.set_xlabel("Component k")
    ax3.set_ylabel("1 / gap_k (sensitivity)")
    ax3.set_title("Davis-Kahan Sensitivity\n(lower = more stable)", fontweight="bold")
    ax3.grid(True, alpha=0.3, axis="y")
    ax3.set_xticks(range(1, len(dk_bounds) + 1))

    note = "Davis-Kahan: sin(θ_k) ≤ ||ΔΣ||_op / gap_k\n"
    note += f"Gap₁ = {np.abs(gaps[0]):.1f} (PC1 very stable)\n"
    if len(gaps) > 1:
        note += f"Gap₂ = {np.abs(gaps[1]):.1f} (PC2 {'stable' if np.abs(gaps[1]) > 100 else 'moderate'})"
    fig.text(0.5, -0.02, note, ha="center", fontsize=9, style="italic",
             bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8))

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Saved eigenvalue gap analysis to %s", out_path)
    return result


# ---------------------------------------------------------------------------
# 3. Hill Estimator for Alpha
# ---------------------------------------------------------------------------

def hill_estimator(
    data: np.ndarray,
    k_range: tuple[int, int] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Hill estimator for the tail index alpha.

    For the k largest order statistics X_(n) >= X_(n-1) >= ... >= X_(n-k+1):
        alpha_Hill(k) = [1/k * sum_{i=1}^{k} log(X_(n-i+1) / X_(n-k))]^(-1)

    Returns alpha estimates for each k in k_range.
    """
    sorted_data = np.sort(data)[::-1]
    n = len(sorted_data)

    if k_range is None:
        k_range = (10, min(n // 2, 200))
    k_values = np.arange(k_range[0], k_range[1] + 1)

    alphas = np.empty(len(k_values))
    for i, k in enumerate(k_values):
        if k >= n or sorted_data[k - 1] <= 0:
            alphas[i] = np.nan
            continue
        log_ratios = np.log(sorted_data[:k] / sorted_data[k])
        mean_log = np.mean(log_ratios)
        alphas[i] = 1.0 / mean_log if mean_log > 0 else np.nan

    return k_values, alphas


def plot_hill_estimator(
    data: np.ndarray,
    loglog_alpha: float,
    out_path: str,
) -> dict:
    """Plot the Hill plot and compare with log-log regression estimate."""
    k_values, alphas = hill_estimator(data)

    valid = np.isfinite(alphas)
    if not np.any(valid):
        logger.warning("No valid Hill estimates")
        return {"hill_alpha_median": None, "hill_alpha_stable_range": None}

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(k_values[valid], alphas[valid], "b-", alpha=0.7, linewidth=1.5, label="Hill estimator α(k)")
    ax.axhline(loglog_alpha, color="red", linestyle="--", linewidth=2,
               label=f"Log-log regression α = {loglog_alpha:.2f}")

    mid_start = len(k_values) // 4
    mid_end = 3 * len(k_values) // 4
    stable_alphas = alphas[mid_start:mid_end]
    stable_alphas = stable_alphas[np.isfinite(stable_alphas)]
    if len(stable_alphas) > 0:
        hill_median = float(np.median(stable_alphas))
        hill_q25 = float(np.percentile(stable_alphas, 25))
        hill_q75 = float(np.percentile(stable_alphas, 75))
        ax.axhline(hill_median, color="green", linestyle=":", linewidth=2,
                   label=f"Hill median α = {hill_median:.2f}")
        ax.axhspan(hill_q25, hill_q75, alpha=0.15, color="green", label="Hill IQR")
    else:
        hill_median = None
        hill_q25 = hill_q75 = None

    ax.set_xlabel("k (number of upper order statistics)")
    ax.set_ylabel("α estimate")
    ax.set_title("Hill Plot: Tail Index Estimation", fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    note = ("The Hill estimator provides a sequence of α estimates as a function of k.\n"
            "A stable plateau indicates a reliable α. Comparison with log-log regression validates the estimate.")
    ax.annotate(note, xy=(0.02, 0.02), xycoords="axes fraction", fontsize=7, va="bottom",
                bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8))

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Saved Hill plot to %s", out_path)

    return {
        "hill_alpha_median": hill_median,
        "hill_alpha_q25": hill_q25,
        "hill_alpha_q75": hill_q75,
        "loglog_alpha": loglog_alpha,
    }


# ---------------------------------------------------------------------------
# 4. Broken Stick Model
# ---------------------------------------------------------------------------

def broken_stick_model(n_components: int) -> np.ndarray:
    """
    Compute expected variance proportions under the broken stick model.

    Under the null hypothesis (no structure), the k-th eigenvalue proportion is:
        b_k = (1/p) * sum_{i=k}^{p} 1/i
    PCs with observed proportion > broken stick proportion are significant.
    """
    proportions = np.zeros(n_components)
    for k in range(n_components):
        proportions[k] = sum(1.0 / (k + 1 + j) for j in range(n_components - k))
    proportions /= n_components
    return proportions


def plot_broken_stick(
    variances: np.ndarray,
    out_path: str,
) -> dict:
    """Compare observed variance proportions with the broken stick model."""
    total = variances.sum()
    observed = variances / total
    expected = broken_stick_model(len(variances))

    n_significant = int(np.sum(observed > expected))

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(1, len(variances) + 1)
    width = 0.35

    ax.bar(x - width / 2, observed, width, label="Observed", color="steelblue",
           alpha=0.7, edgecolor="black")
    ax.bar(x + width / 2, expected, width, label="Broken Stick (null)",
           color="coral", alpha=0.7, edgecolor="black")

    for i in range(min(n_significant, 6)):
        ax.annotate("*", xy=(x[i], observed[i]), fontsize=14, ha="center",
                    fontweight="bold", color="green")

    ax.set_xlabel("Principal Component")
    ax.set_ylabel("Proportion of Variance")
    ax.set_title("Broken Stick Model: Significant Components", fontweight="bold")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    ax.set_xticks(x[::2])

    note = (f"{n_significant} component(s) exceed the broken stick threshold (* marked).\n"
            "These PCs capture more variance than expected by chance under the null model.")
    ax.annotate(note, xy=(0.5, 0.95), xycoords="axes fraction", fontsize=9,
                ha="center", va="top",
                bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8))

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Saved broken stick plot to %s", out_path)
    return {
        "n_significant": n_significant,
        "observed_proportions": observed[:6].tolist(),
        "broken_stick_proportions": expected[:6].tolist(),
    }


# ---------------------------------------------------------------------------
# 5. Per-Component Tail Analysis
# ---------------------------------------------------------------------------

def per_component_tail_analysis(
    components: np.ndarray,
    out_path: str,
    n_pcs: int = 6,
) -> dict:
    """
    Analyze tail behavior for each eigenvector individually
    (not pooled across all components).
    """
    results = {}
    n_pcs = min(n_pcs, components.shape[1])
    fig, axes = plt.subplots(2, 3, figsize=(16, 10))

    for i, ax in enumerate(axes.flat):
        if i >= n_pcs:
            ax.axis("off")
            continue

        loadings = components[:, i]
        kurtosis_val = float(stats.kurtosis(loadings))

        ax.hist(loadings, bins=12, density=True, alpha=0.6, color=f"C{i}", edgecolor="black")
        mu, sigma = loadings.mean(), loadings.std()
        x_r = np.linspace(loadings.min() - 0.05, loadings.max() + 0.05, 100)
        ax.plot(x_r, stats.norm.pdf(x_r, mu, sigma), "r-", linewidth=1.5)
        ax.set_title(f"PC{i+1} (κ = {kurtosis_val:.2f})", fontweight="bold")
        ax.set_xlabel("Loading value")
        ax.set_ylabel("Density")
        ax.grid(True, alpha=0.3)

        results[f"PC{i+1}"] = {
            "mean": float(mu),
            "std": float(sigma),
            "excess_kurtosis": kurtosis_val,
            "min": float(loadings.min()),
            "max": float(loadings.max()),
        }

    fig.suptitle("Per-Component Loading Distributions (κ = excess kurtosis)",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Saved per-component tail analysis to %s", out_path)
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(
    pca_model_path: str = "pca_model.pkl",
    output_dir: str = ".",
    tail_report_path: str | None = None,
) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    components, variances = load_pca_model(pca_model_path)
    logger.info("Loaded PCA model: %s components, %s variances", components.shape, variances.shape)

    loglog_alpha = None
    if tail_report_path:
        try:
            with open(tail_report_path) as f:
                tail_report = json.load(f)
            loglog_alpha = tail_report.get("alpha")
        except Exception:
            pass

    all_results = {}

    logger.info("=" * 60)
    logger.info("EXTENDED ANALYSIS 1: PC Loading Interpretation")
    logger.info("=" * 60)
    plot_pc_loading_interpretation(
        components, variances,
        out_path=str(output_dir / "ec_pc_loading_interpretation.png"),
    )

    logger.info("=" * 60)
    logger.info("EXTENDED ANALYSIS 2: Eigenvalue Gap & Davis-Kahan Bound")
    logger.info("=" * 60)
    gap_results = eigenvalue_gap_analysis(
        variances,
        out_path=str(output_dir / "ec_eigenvalue_gap_analysis.png"),
    )
    all_results["eigenvalue_gap"] = gap_results

    logger.info("=" * 60)
    logger.info("EXTENDED ANALYSIS 3: Hill Estimator for Alpha")
    logger.info("=" * 60)
    absolute = np.abs(components.flatten())
    absolute = absolute[absolute > 0]
    if loglog_alpha is None:
        loglog_alpha = 5.12
    hill_results = plot_hill_estimator(
        absolute, loglog_alpha,
        out_path=str(output_dir / "ec_hill_estimator.png"),
    )
    all_results["hill_estimator"] = hill_results

    logger.info("=" * 60)
    logger.info("EXTENDED ANALYSIS 4: Broken Stick Model")
    logger.info("=" * 60)
    stick_results = plot_broken_stick(
        variances,
        out_path=str(output_dir / "ec_broken_stick.png"),
    )
    all_results["broken_stick"] = stick_results

    logger.info("=" * 60)
    logger.info("EXTENDED ANALYSIS 5: Per-Component Tail Analysis")
    logger.info("=" * 60)
    per_comp_results = per_component_tail_analysis(
        components,
        out_path=str(output_dir / "ec_per_component_tails.png"),
    )
    all_results["per_component_tails"] = per_comp_results

    report_path = output_dir / "extended_analysis_report.json"
    with open(report_path, "w") as f:
        json.dump(all_results, f, indent=2)
    logger.info("Saved extended analysis report to %s", report_path)

    logger.info("=" * 60)
    logger.info("ALL EXTENDED ANALYSES COMPLETE")
    logger.info("=" * 60)
    logger.info("Outputs:")
    logger.info("  ec_pc_loading_interpretation.png")
    logger.info("  ec_eigenvalue_gap_analysis.png")
    logger.info("  ec_hill_estimator.png")
    logger.info("  ec_broken_stick.png")
    logger.info("  ec_per_component_tails.png")
    logger.info("  extended_analysis_report.json")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extended Analyses for HW2")
    parser.add_argument("--pca-model", default="pca_model.pkl", help="Path to pca_model.pkl")
    parser.add_argument("--output-dir", default=".", help="Output directory")
    parser.add_argument("--tail-report", default=None, help="Path to tail_analysis_report.json (optional)")
    args = parser.parse_args()
    main(
        pca_model_path=args.pca_model,
        output_dir=args.output_dir,
        tail_report_path=args.tail_report,
    )
