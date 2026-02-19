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


def pool_coefficients(components: np.ndarray, use_absolute: bool = True) -> np.ndarray:
    """
    Pool all eigenvector loadings into a single 1D array.

    Args:
        components: (n_features, n_components) matrix where each column is an eigenvector
        use_absolute: if True, take absolute values (typical for tail analysis)

    Returns:
        pooled: 1D array of all coefficient values
    """
    logger.info("Pooling coefficients from components shape %s", components.shape)
    pooled = components.flatten()
    if use_absolute:
        pooled = np.abs(pooled)
    # Remove any NaN/inf values
    pooled = pooled[np.isfinite(pooled)]
    logger.info("Pooled %d coefficient values (absolute=%s)", len(pooled), use_absolute)
    return pooled


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


def classify_tail(
    qq_stats: dict, alpha: float, r_squared: float, threshold_r2: float = 0.9
) -> str:
    """
    Classify tail as light (Gaussian-like) or heavy (power-law).

    Criteria:
    - Heavy tail if: Q-Q plot shows deviation AND log-log fit is good (R^2 > threshold)
    - Light tail otherwise

    Args:
        qq_stats: statistics from Q-Q plot analysis
        alpha: estimated power-law exponent
        r_squared: goodness of fit for power-law
        threshold_r2: minimum R^2 to consider power-law fit good

    Returns:
        "heavy" or "light"
    """
    # Check if log-log fit is good
    good_powerlaw_fit = r_squared >= threshold_r2

    # For now, use a simple heuristic: if R^2 is high, it's heavy tail
    # In practice, you'd also check Q-Q plot deviation
    if good_powerlaw_fit and alpha > 0:
        return "heavy"
    else:
        return "light"


def create_diagnostic_plots(
    coefficients: np.ndarray,
    alpha: float,
    r_squared: float,
    tail_type: str,
    output_path: str = "coefficient_distribution.png",
) -> None:
    """
    Create three-panel diagnostic plot: histogram, Q-Q plot, log-log survival plot.

    Args:
        coefficients: pooled coefficient values
        alpha: estimated tail exponent
        r_squared: R^2 of power-law fit
        tail_type: "light" or "heavy"
        output_path: path to save figure
    """
    logger.info("Creating diagnostic plots")

    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    # Panel 1: Histogram with Gaussian overlay
    ax1 = axes[0]
    ax1.hist(coefficients, bins=50, density=True, alpha=0.7, edgecolor="black")

    # Fit Gaussian and overlay
    mu, sigma = coefficients.mean(), coefficients.std()
    x_range = np.linspace(coefficients.min(), coefficients.max(), 100)
    gaussian_pdf = stats.norm.pdf(x_range, mu, sigma)
    ax1.plot(x_range, gaussian_pdf, "r-", linewidth=2, label=f"Gaussian fit\n(μ={mu:.3f}, σ={sigma:.3f})")

    ax1.set_xlabel("Coefficient value")
    ax1.set_ylabel("Density")
    ax1.set_title("Histogram of PCA Coefficients")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Panel 2: Q-Q plot
    ax2 = axes[1]
    stats.probplot(coefficients, dist="norm", plot=ax2)
    ax2.set_title("Q-Q Plot (Normal)")
    ax2.grid(True, alpha=0.3)

    # Panel 3: Log-Log Survival Plot
    ax3 = axes[2]
    x_vals, survival = compute_survival_function(coefficients)

    # Remove zeros for log scale
    mask = (x_vals > 0) & (survival > 0)
    x_vals = x_vals[mask]
    survival = survival[mask]

    ax3.loglog(x_vals, survival, "b.", alpha=0.5, markersize=3, label="Empirical")

    # Add fitted line if alpha is valid
    if not np.isnan(alpha) and alpha > 0:
        # Fit line on tail portion (top 10%)
        tail_idx = int(len(x_vals) * 0.9)
        x_tail = x_vals[tail_idx:]

        # Power law: P(X > x) = C * x^(-alpha)
        # Fit C using the start of tail region
        if len(x_tail) > 0:
            log_C = np.log(survival[tail_idx]) + alpha * np.log(x_tail[0])
            fitted_survival = np.exp(log_C - alpha * np.log(x_tail))
            ax3.loglog(
                x_tail,
                fitted_survival,
                "r-",
                linewidth=2,
                label=f"Power law fit\n(α={alpha:.2f}, R²={r_squared:.3f})",
            )

    ax3.set_xlabel("Coefficient value (x)")
    ax3.set_ylabel("P(X > x)")
    ax3.set_title("Log-Log Survival Plot")
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # Add overall title with classification
    fig.suptitle(
        f"PCA Coefficient Distribution Analysis (Tail: {tail_type.upper()})",
        fontsize=14,
        fontweight="bold",
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
    coefficients: np.ndarray,
    tail_type: str,
    alpha: float,
    r_squared: float,
    output_path: str = "tail_analysis_report.json",
) -> None:
    """
    Save tail analysis report as JSON.

    Args:
        coefficients: pooled coefficient values
        tail_type: "light" or "heavy"
        alpha: estimated tail exponent
        r_squared: R^2 of power-law fit
        output_path: path to save JSON report
    """
    logger.info("Saving tail analysis report")

    report = {
        "tail_type": tail_type,
        "alpha": float(alpha) if not np.isnan(alpha) else None,
        "alpha_r2": float(r_squared) if not np.isnan(r_squared) else None,
        "n_coefficients": len(coefficients),
        "statistics": compute_statistics(coefficients),
        "methodology": "Log-log regression on tail (top 10% of values) to estimate power-law exponent. "
                      "Tail classified as heavy if R² > 0.9 and alpha > 0, otherwise light.",
    }

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info("Saved tail analysis report to %s", output_path)


def main(
    model_path: str = "pca_model.pkl",
    output_dir: str = ".",
    use_absolute: bool = True,
) -> None:
    """
    Main function for Part 2: tail analysis of PCA coefficients.

    Args:
        model_path: path to pca_model.pkl
        output_dir: directory for outputs
        use_absolute: whether to use absolute values of coefficients
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load PCA model
    components, variances = load_pca_model(model_path)

    # Pool coefficients
    coefficients = pool_coefficients(components, use_absolute=use_absolute)

    # Estimate alpha via log-log regression
    alpha, r_squared = estimate_alpha_loglog(coefficients, tail_percentile=90.0)

    # Classify tail type
    qq_stats = {}  # Placeholder for Q-Q statistics
    tail_type = classify_tail(qq_stats, alpha, r_squared, threshold_r2=0.9)

    # Create diagnostic plots
    create_diagnostic_plots(
        coefficients,
        alpha,
        r_squared,
        tail_type,
        output_path=str(output_dir / "coefficient_distribution.png"),
    )

    # Save report
    save_tail_analysis_report(
        coefficients,
        tail_type,
        alpha,
        r_squared,
        output_path=str(output_dir / "tail_analysis_report.json"),
    )

    # Print summary
    logger.info("=" * 60)
    logger.info("TAIL ANALYSIS SUMMARY")
    logger.info("=" * 60)
    logger.info("Number of coefficients: %d", len(coefficients))
    logger.info("Mean: %.4f, Std: %.4f", coefficients.mean(), coefficients.std())
    logger.info("Tail type: %s", tail_type.upper())
    logger.info("Alpha (tail exponent): %.3f", alpha)
    logger.info("R² (power-law fit): %.3f", r_squared)
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Part 2: Tail analysis of PCA coefficients")
    parser.add_argument(
        "--model",
        default="pca_model.pkl",
        help="Path to pca_model.pkl",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory for coefficient_distribution.png and tail_analysis_report.json",
    )
    parser.add_argument(
        "--use-signed",
        action="store_true",
        help="Use signed coefficient values instead of absolute values",
    )

    args = parser.parse_args()

    main(
        model_path=args.model,
        output_dir=args.output_dir,
        use_absolute=not args.use_signed,
    )
