"""
Part 2: Distribution of Individual Coefficients — Light Tail vs Heavy Tail, Alpha.

Analyzes the distribution of PCA eigenvector loadings to characterize tail behavior.
Produces diagnostic plots (histogram, Q-Q plot, log-log survival) and estimates
the tail exponent (alpha) if the distribution appears to follow a power law.
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


def load_pca_model(model_path: str) -> tuple[np.ndarray, np.ndarray]:
    """
    Load PCA model from pickle file.

    Returns:
        components: orthonormal matrix (eigenvectors), shape (24, 24)
        variances: eigenvalues (explained variance), shape (24,)
    """
    logger.info("Loading PCA model from %s", model_path)
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    return model["components"], model["variances"]


def pool_coefficients(components: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Pool all eigenvector loadings into 1D arrays (signed and absolute).

    Args:
        components: (n_features, n_components) matrix where each column is an eigenvector

    Returns:
        signed: 1D array of all coefficient values (for Q-Q / histogram)
        absolute: 1D array of |coefficient| values (for survival / alpha estimation)
    """
    logger.info("Pooling coefficients from components shape %s", components.shape)
    signed = components.flatten()
    signed = signed[np.isfinite(signed)]
    absolute = np.abs(signed)
    logger.info("Pooled %d coefficient values", len(signed))
    return signed, absolute


def compute_survival_function(data: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Compute empirical survival function P(X > x).

    Returns:
        x_vals: sorted unique data values
        survival: P(X > x) for each x
    """
    sorted_data = np.sort(data)
    n = len(sorted_data)
    # Survival probability: fraction of data points greater than x
    survival = np.arange(n, 0, -1) / n
    return sorted_data, survival


def estimate_alpha_loglog(
    data: np.ndarray, tail_percentile: float = 90.0
) -> tuple[float, float]:
    """
    Estimate power-law tail exponent alpha using log-log regression on the tail.

    For a power law: P(X > x) ~ x^(-alpha)
    In log-log space: log(P(X > x)) ~ -alpha * log(x)

    Args:
        data: coefficient values (should be positive)
        tail_percentile: consider only values above this percentile for fitting

    Returns:
        alpha: estimated tail exponent
        r_squared: R^2 of the log-log fit
    """
    logger.info("Estimating alpha using log-log regression (tail >= %.1f%%)", tail_percentile)

    # Get tail values
    threshold = np.percentile(data, tail_percentile)
    tail_data = data[data >= threshold]

    if len(tail_data) < 10:
        logger.warning("Too few tail values (%d), using lower percentile", len(tail_data))
        tail_percentile = 80.0
        threshold = np.percentile(data, tail_percentile)
        tail_data = data[data >= threshold]

    # Compute survival function for tail
    x_vals, survival = compute_survival_function(tail_data)

    # Remove zeros to avoid log(0)
    mask = (x_vals > 0) & (survival > 0)
    x_vals = x_vals[mask]
    survival = survival[mask]

    if len(x_vals) < 5:
        logger.warning("Insufficient tail data for fitting")
        return np.nan, np.nan

    # Log-log regression
    log_x = np.log(x_vals)
    log_survival = np.log(survival)

    # Linear fit: log(survival) = intercept + slope * log(x)
    # alpha = -slope
    slope, intercept, r_value, _, _ = stats.linregress(log_x, log_survival)
    alpha = -slope
    r_squared = r_value ** 2

    logger.info("Estimated alpha = %.3f, R^2 = %.3f", alpha, r_squared)
    return alpha, r_squared


def compute_qq_deviation(signed_coefficients: np.ndarray) -> dict:
    """
    Measure Q-Q deviation from normality using the Shapiro-Wilk test
    and excess kurtosis.

    Returns dict with test statistics useful for tail classification.
    """
    excess_kurtosis = float(stats.kurtosis(signed_coefficients))

    n = len(signed_coefficients)
    if n <= 5000:
        sw_stat, sw_p = stats.shapiro(signed_coefficients)
    else:
        rng = np.random.default_rng(42)
        sample = rng.choice(signed_coefficients, size=5000, replace=False)
        sw_stat, sw_p = stats.shapiro(sample)

    return {
        "excess_kurtosis": excess_kurtosis,
        "shapiro_w": float(sw_stat),
        "shapiro_p": float(sw_p),
    }


def classify_tail(
    qq_stats: dict, alpha: float, r_squared: float, threshold_r2: float = 0.9
) -> str:
    """
    Classify tail as light (Gaussian-like) or heavy (power-law).

    Criteria (both must hold for "heavy"):
      1. Log-log power-law fit is good (R^2 >= threshold) and alpha > 0
      2. Q-Q deviation from normality: excess kurtosis > 0 OR Shapiro-Wilk
         rejects normality (p < 0.05)

    Returns:
        "heavy" or "light"
    """
    good_powerlaw_fit = r_squared >= threshold_r2 and alpha > 0

    qq_deviates = (
        qq_stats.get("excess_kurtosis", 0) > 0
        or qq_stats.get("shapiro_p", 1.0) < 0.05
    )

    if good_powerlaw_fit and qq_deviates:
        return "heavy"
    return "light"


def create_diagnostic_plots(
    signed: np.ndarray,
    absolute: np.ndarray,
    alpha: float,
    r_squared: float,
    tail_type: str,
    output_path: str = "coefficient_distribution.png",
) -> None:
    """
    Create three-panel diagnostic plot.

    Panels 1-2 (histogram, Q-Q) use *signed* coefficients so the normal
    comparison is valid.  Panel 3 (log-log survival) uses *absolute* values
    because tail exponent estimation requires positive data.
    """
    logger.info("Creating diagnostic plots")

    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    # Panel 1: Histogram of signed coefficients with Gaussian overlay
    ax1 = axes[0]
    ax1.hist(signed, bins=50, density=True, alpha=0.7, edgecolor="black")
    mu, sigma = signed.mean(), signed.std()
    x_range = np.linspace(signed.min(), signed.max(), 200)
    ax1.plot(x_range, stats.norm.pdf(x_range, mu, sigma), "r-", linewidth=2,
             label=f"Gaussian fit\n(μ={mu:.3f}, σ={sigma:.3f})")
    ax1.set_xlabel("Coefficient value")
    ax1.set_ylabel("Density")
    ax1.set_title("Histogram of PCA Coefficients (signed)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Panel 2: Q-Q plot of signed coefficients vs normal
    ax2 = axes[1]
    stats.probplot(signed, dist="norm", plot=ax2)
    ax2.set_title("Q-Q Plot (Normal)")
    ax2.grid(True, alpha=0.3)

    # Panel 3: Log-Log Survival of |coefficients|
    ax3 = axes[2]
    x_vals, survival = compute_survival_function(absolute)
    mask = (x_vals > 0) & (survival > 0)
    x_vals, survival = x_vals[mask], survival[mask]
    ax3.loglog(x_vals, survival, "b.", alpha=0.5, markersize=3, label="Empirical")

    if not np.isnan(alpha) and alpha > 0:
        tail_idx = int(len(x_vals) * 0.9)
        x_tail = x_vals[tail_idx:]
        if len(x_tail) > 0:
            log_C = np.log(survival[tail_idx]) + alpha * np.log(x_tail[0])
            fitted_survival = np.exp(log_C - alpha * np.log(x_tail))
            ax3.loglog(x_tail, fitted_survival, "r-", linewidth=2,
                       label=f"Power law fit\n(α={alpha:.2f}, R²={r_squared:.3f})")

    ax3.set_xlabel("|Coefficient| (x)")
    ax3.set_ylabel("P(|X| > x)")
    ax3.set_title("Log-Log Survival Plot (|coeff|)")
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    fig.suptitle(
        f"PCA Coefficient Distribution Analysis (Tail: {tail_type.upper()})",
        fontsize=14, fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Saved diagnostic plots to %s", output_path)


def compute_statistics(coefficients: np.ndarray) -> dict:
    """Compute descriptive statistics for coefficients."""
    return {
        "mean": float(coefficients.mean()),
        "std": float(coefficients.std()),
        "min": float(coefficients.min()),
        "max": float(coefficients.max()),
        "median": float(np.median(coefficients)),
        "skewness": float(stats.skew(coefficients)),
        "kurtosis": float(stats.kurtosis(coefficients)),
    }


def save_tail_analysis_report(
    signed: np.ndarray,
    absolute: np.ndarray,
    tail_type: str,
    alpha: float,
    r_squared: float,
    qq_stats: dict,
    output_path: str = "tail_analysis_report.json",
) -> None:
    """Save tail analysis report as JSON."""
    logger.info("Saving tail analysis report")

    report = {
        "tail_type": tail_type,
        "alpha": float(alpha) if not np.isnan(alpha) else None,
        "alpha_r2": float(r_squared) if not np.isnan(r_squared) else None,
        "n_coefficients": len(signed),
        "statistics_signed": compute_statistics(signed),
        "statistics_absolute": compute_statistics(absolute),
        "qq_deviation": qq_stats,
        "methodology": (
            "Log-log regression on |coefficients| tail (top 10%) to estimate "
            "power-law exponent. Tail classified as heavy if R² > 0.9, alpha > 0, "
            "AND Q-Q deviation from normality is detected (excess kurtosis > 0 or "
            "Shapiro-Wilk p < 0.05)."
        ),
    }

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("Saved tail analysis report to %s", output_path)


def main(
    model_path: str = "pca_model.pkl",
    output_dir: str = ".",
) -> None:
    """Main function for Part 2: tail analysis of PCA coefficients."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    components, variances = load_pca_model(model_path)

    signed, absolute = pool_coefficients(components)

    alpha, r_squared = estimate_alpha_loglog(absolute, tail_percentile=90.0)

    qq_stats = compute_qq_deviation(signed)
    tail_type = classify_tail(qq_stats, alpha, r_squared, threshold_r2=0.9)

    create_diagnostic_plots(
        signed, absolute, alpha, r_squared, tail_type,
        output_path=str(output_dir / "coefficient_distribution.png"),
    )

    save_tail_analysis_report(
        signed, absolute, tail_type, alpha, r_squared, qq_stats,
        output_path=str(output_dir / "tail_analysis_report.json"),
    )

    logger.info("=" * 60)
    logger.info("TAIL ANALYSIS SUMMARY")
    logger.info("=" * 60)
    logger.info("Coefficients: %d (signed range [%.4f, %.4f])",
                len(signed), signed.min(), signed.max())
    logger.info("Excess kurtosis: %.4f, Shapiro-Wilk p: %.4g",
                qq_stats["excess_kurtosis"], qq_stats["shapiro_p"])
    logger.info("Tail type: %s", tail_type.upper())
    logger.info("Alpha (tail exponent): %.3f", alpha)
    logger.info("R² (power-law fit): %.3f", r_squared)
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Part 2: Tail analysis of PCA coefficients")
    parser.add_argument("--model", default="pca_model.pkl", help="Path to pca_model.pkl")
    parser.add_argument("--output-dir", default=".", help="Output directory")
    args = parser.parse_args()

    main(model_path=args.model, output_dir=args.output_dir)
