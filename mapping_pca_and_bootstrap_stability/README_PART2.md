# Part 2: Distribution of Individual Coefficients — Tail Analysis

## Overview

This implementation analyzes the distribution of PCA eigenvector loadings to characterize their tail behavior, determining whether they follow a light-tailed (Gaussian-like) or heavy-tailed (power-law) distribution.

## Implementation

**Script:** `pca_part2_tail_analysis.py`

### What It Does

1. **Loads PCA Model** - Reads `pca_model.pkl` from Part 1 containing eigenvectors and eigenvalues
2. **Pools Coefficients** - Extracts all 576 loadings (24 eigenvectors × 24 features) as absolute values
3. **Creates Diagnostic Plots** - Generates three-panel visualization:
   - **Histogram**: Distribution with Gaussian overlay
   - **Q-Q Plot**: Quantile-quantile plot against normal distribution
   - **Log-Log Survival Plot**: Empirical survival function P(X > x) with power-law fit
4. **Estimates Alpha** - Uses log-log regression on the tail (top 10% of values) to estimate power-law exponent
5. **Classifies Tail Type** - Determines if distribution is heavy-tailed (power-law) or light-tailed (Gaussian)
6. **Generates Report** - Saves analysis results as JSON

## Results

**From the analysis:**
- **Number of coefficients:** 576
- **Tail type:** HEAVY
- **Alpha (tail exponent):** 5.12
- **R² (power-law fit):** 0.986
- **Mean:** 0.162, **Std:** 0.124
- **Skewness:** 0.81, **Kurtosis:** 0.49

The high R² (0.986) indicates an excellent power-law fit in the tail region, confirming heavy-tailed behavior.

## Outputs

| File | Description |
|------|-------------|
| `coefficient_distribution.png` | Three-panel diagnostic plot (histogram, Q-Q, log-log survival) |
| `tail_analysis_report.json` | Complete analysis report with statistics and classification |

## Usage

```bash
# From repo root with virtual environment
cd mapping_pca_and_bootstrap_stability
../venv/bin/python pca_part2_tail_analysis.py

# With custom paths
../venv/bin/python pca_part2_tail_analysis.py \
  --model pca_model.pkl \
  --output-dir .

# Use signed coefficients instead of absolute values
../venv/bin/python pca_part2_tail_analysis.py --use-signed
```

## Methodology

### Power-Law Estimation

For a power-law distribution: **P(X > x) ∝ x^(-α)**

In log-log space this becomes linear:
**log(P(X > x)) = log(C) - α·log(x)**

The slope of this line gives us **-α**.

### Tail Classification

A distribution is classified as **heavy-tailed** if:
1. Log-log regression on tail (top 10%) yields R² > 0.9
2. Estimated α > 0
3. Q-Q plot shows deviation from normality at extremes

Otherwise, it's classified as **light-tailed**.

## Interpretation

The heavy-tailed distribution with α ≈ 5.12 indicates:
- The eigenvector loadings have more extreme values than a Gaussian distribution
- Large coefficients decay as a power law rather than exponentially
- This suggests the presence of significant structure or patterns in the taxi data that PCA captures with strong loadings

## Dependencies

- `numpy` - Array operations
- `scipy` - Statistical functions, Q-Q plots
- `matplotlib` - Visualization
- `pickle` - Loading PCA model
- `json` - Saving report

All dependencies are listed in `requirements.txt`.
