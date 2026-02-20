# HW2: Mapping PCA, Tail Analysis, and Bootstrap Stability

## Quick Start

```bash
cd mapping_pca_and_bootstrap_stability
pip install -r ../requirements.txt

# Set AWS credentials (required for S3 access)
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>
export AWS_SESSION_TOKEN=<your-token>     # if using temporary credentials

# Run all parts in order
python pca_part1_unnormalized.py --output-dir .
python pca_part2_tail_analysis.py --model pca_model.pkl --output-dir .
python part3_folium_map.py --wide s3://291-s3-bucket/wide.parquet --pca-model pca_model.pkl
python pca_part4_bootstrap_stability.py --input s3://291-s3-bucket/wide.parquet --pca-model pca_model.pkl --output-dir .
```

---

## Part 1: PCA on Unnormalized Pivot Tables

**Script:** `pca_part1_unnormalized.py`

### What It Does

1. Loads the HW1 wide Parquet table from S3 (2,914,892 rows x 24 hour columns)
2. Imputes NaN values with the column mean (excluding NaN)
3. Centers the data by subtracting column means
4. Computes the covariance matrix via Dask: converts to a Dask array and computes `X^T @ X / n` lazily, only materializing the 24x24 result
5. Performs eigendecomposition (`np.linalg.eigh`) and sorts by descending eigenvalue
6. Saves the orthonormal eigenvector matrix and variance vector

### Execution

```bash
# With AWS credentials in environment
python pca_part1_unnormalized.py --output-dir .

# With anonymous S3 access (if bucket allows it)
python pca_part1_unnormalized.py --anon --output-dir .

# With a local parquet file
python pca_part1_unnormalized.py --input /path/to/wide.parquet --output-dir .
```

### Outputs

| File | Description |
|------|-------------|
| `pca_model.pkl` | Pickle with `{"components": (24,24) ndarray, "variances": (24,) ndarray}` |
| `variance_explained.png` | Two-panel plot: scree plot + cumulative variance explained |

### Results

- **PC1** explains ~90% of total variance (dominated by overall ride volume)
- **PC1 + PC2** explain ~95%; first 5 components reach ~99%
- The steep scree drop-off is expected for unnormalized count data

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `PermissionError: Access Denied` | Missing or expired AWS credentials | Set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` |
| `ModuleNotFoundError: No module named 'dask'` | Dependencies not installed | `pip install -r ../requirements.txt` |
| `ValueError: Missing hour columns` | Parquet schema doesn't match HW1 output | Verify your wide table has columns `hour_0` through `hour_23` |

---

## Part 2: Distribution of Individual Coefficients — Tail Analysis

**Script:** `pca_part2_tail_analysis.py`

### What It Does

1. Loads `pca_model.pkl` from Part 1
2. Pools all 576 eigenvector loadings (24 eigenvectors x 24 features) into signed and absolute arrays
3. Produces a three-panel diagnostic plot:
   - **Histogram** (signed coefficients) with Gaussian overlay
   - **Q-Q plot** (signed coefficients) against normal distribution
   - **Log-log survival plot** (|coefficients|) with power-law fit
4. Estimates the tail exponent alpha via log-log regression on the top 10% of |coefficients|
5. Measures Q-Q deviation using excess kurtosis and Shapiro-Wilk test
6. Classifies tail type: **heavy** requires BOTH a good power-law fit (R^2 > 0.9) AND Q-Q deviation from normality

### Execution

```bash
# Standard run (requires pca_model.pkl from Part 1)
python pca_part2_tail_analysis.py --model pca_model.pkl --output-dir .
```

### Outputs

| File | Description |
|------|-------------|
| `coefficient_distribution.png` | Three-panel diagnostic plot (histogram, Q-Q, log-log survival) |
| `tail_analysis_report.json` | Full report: classification, alpha, R^2, statistics, Q-Q deviation metrics |

### Results

- **Tail type:** LIGHT
- **Alpha (tail exponent):** 5.12
- **R^2 (power-law fit):** 0.986
- **Excess kurtosis:** -0.16 (negative = lighter tails than Gaussian)
- **Shapiro-Wilk p-value:** 0.18 (fails to reject normality at p < 0.05)

Although the log-log R^2 is high, the signed coefficients pass the Shapiro-Wilk normality test and have negative excess kurtosis, confirming the distribution is **light-tailed** (Gaussian-like). The high R^2 on |coefficients| is expected because any well-behaved bounded distribution will show a steep log-log decline in the tail.

### Methodology

**Power-law estimation:** For P(X > x) ~ x^(-alpha), in log-log space this is linear with slope -alpha. We fit on the top 10% of |coefficient| values.

**Tail classification criteria (both must hold for "heavy"):**
1. Log-log power-law fit: R^2 >= 0.9 and alpha > 0
2. Q-Q deviation from normality: excess kurtosis > 0 OR Shapiro-Wilk p < 0.05

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `FileNotFoundError: pca_model.pkl` | Part 1 hasn't been run | Run Part 1 first |
| `ModuleNotFoundError: No module named 'scipy'` | scipy not installed | `pip install scipy` |

---

## Part 3: First and Second Coefficients on a Folium Map

**Script:** `part3_folium_map.py`

### What It Does

1. Loads the wide Parquet and `pca_model.pkl`
2. Computes PC1 and PC2 scores for every row (same centering/imputation as Part 1)
3. Aggregates scores by `pickup_place` (mean score per taxi zone)
4. Loads zone coordinates from the course shapefile (`taxi_zones.shp` from S3)
5. Computes zone centroids in a projected CRS (EPSG:2263) to avoid geographic distortion
6. Builds an interactive Folium map:
   - **PC1 layer:** CircleMarkers colored with a diverging RdBu colormap
   - **PC2 layer:** CircleMarkers sized proportional to |PC2| (toggleable)
   - Tooltips show zone ID, PC1, and PC2 values
   - LayerControl for toggling layers

### Execution

```bash
# With AWS credentials
python part3_folium_map.py \
  --wide s3://291-s3-bucket/wide.parquet \
  --pca-model pca_model.pkl \
  --zone-coords s3://dsc291-ucsd/taxi/Dataset/02.taxi_zones/taxi+_zone_lookup.csv

# With a local parquet and shapefile
python part3_folium_map.py \
  --wide /path/to/wide.parquet \
  --pca-model pca_model.pkl \
  --zone-coords /path/to/taxi_zones.shp

# Custom output path
python part3_folium_map.py --wide ... --pca-model ... --zone-coords ... --output my_map.html
```

### Outputs

| File | Description |
|------|-------------|
| `pc1_pc2_folium_map.html` | Interactive Folium map (~422 KB) |

### Results

- Zones with high PC1 (red) correspond to high-volume areas (e.g., Midtown Manhattan)
- Zones with low PC1 (blue) are low-volume areas
- PC2 captures temporal contrast patterns (e.g., daytime vs nighttime dominant zones)
- The map is interactive: hover for values, toggle layers, zoom in/out

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `ImportError: geopandas is required` | geopandas not installed | `pip install geopandas` |
| `ValueError: No rows after joining` | Zone ID type mismatch between wide table and shapefile | Both are coerced to string; check if your wide table uses numeric IDs |
| `PermissionError: 403 Forbidden` | S3 credentials missing or expired | Set AWS env vars; or use `--wide` with a local file |
| `ModuleNotFoundError: No module named 'folium'` | folium not installed | `pip install folium branca` |

---

## Part 4: Bootstrap Stability of Eigenvectors

**Script:** `pca_part4_bootstrap_stability.py`

### What It Does

1. Loads the wide Parquet from S3 and converts to a centered numpy array
2. Loads reference PCA components from `pca_model.pkl` (or recomputes if not provided)
3. Runs B=100 bootstrap replicates:
   - Resample rows with replacement
   - Re-center each bootstrap sample around its own mean
   - Compute covariance and eigendecompose
   - Align eigenvector signs to match the reference (dot product check)
4. Computes three stability metrics per replicate:
   - **Subspace affinity:** ||U_ref^T U_boot||_F^2 / K (range [0,1]; 1 = identical subspaces)
   - **Procrustes distance:** min_Q ||U_ref - U_boot Q||_F (0 = perfect alignment)
   - **Component-wise |correlation|:** absolute Pearson correlation per eigenvector (handles sign ambiguity)
5. Generates a multi-panel visualization and a JSON report

### Execution

```bash
# With reference PCA from Part 1
python pca_part4_bootstrap_stability.py \
  --input s3://291-s3-bucket/wide.parquet \
  --pca-model pca_model.pkl \
  --output-dir . \
  --B 100 --K 2

# Without reference (recomputes PCA internally)
python pca_part4_bootstrap_stability.py \
  --input s3://291-s3-bucket/wide.parquet \
  --output-dir .

# Fewer bootstraps for a quick test
python pca_part4_bootstrap_stability.py \
  --input s3://291-s3-bucket/wide.parquet \
  --pca-model pca_model.pkl \
  --B 10 --K 2
```

### Outputs

| File | Description |
|------|-------------|
| `bootstrap_stability_report.json` | Full metrics: per-replicate values, summary stats, 95% CIs, interpretations |
| `bootstrap_stability_report_summary.txt` | Human-readable summary of all metrics |
| `eigenvector_stability.png` | Multi-panel plot: PC1 band, PC2 band, correlation boxplot, summary |

### Results

| Metric | Value | Interpretation |
|--------|-------|----------------|
| Subspace affinity | 0.999967 +/- 0.000030 | Excellent: top-2 subspace nearly identical across bootstraps |
| Procrustes distance | 0.007348 +/- 0.003357 | Very small: eigenvectors align almost perfectly |
| PC1 correlation | 1.0000 +/- 0.0000 | Extremely stable |
| PC2 correlation | 1.0000 +/- 0.0000 | Extremely stable |

The PCA eigenvectors are **highly stable** across bootstrap resampling. With 2.9 million rows, the covariance matrix is very precisely estimated, making the eigenvectors robust to sampling variation.

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `ImportError: cannot import name 'WIDE_PARQUET_PATH' from 'pca_part1_unnormalized'` | Script not run from the correct directory | `cd mapping_pca_and_bootstrap_stability` before running |
| `MemoryError` during bootstrap | Not enough RAM for 2.9M x 24 array + bootstrap copies | Reduce data size or use a machine with more RAM (~4 GB needed) |
| Runtime too slow | B=100 with 2.9M rows takes ~80 seconds | Use `--B 10` for testing; the bottleneck is `X_boot.T @ X_boot` per iteration |

---

## Extended Analyses

**Script:** `extra_credit_analysis.py`

### Execution

```bash
python extended_analysis.py \
  --pca-model pca_model.pkl \
  --output-dir . \
  --tail-report tail_analysis_report.json
```

### EC1: PC Loading Interpretation

Plots eigenvector loadings as hour-of-day bar charts for the first 4 PCs. Reveals what temporal pattern each component captures:
- **PC1**: Uniform positive loadings → overall ride volume
- **PC2**: Day-vs-night contrast (positive daytime, negative nighttime)
- **PC3**: Morning-vs-evening rush hour asymmetry
- **PC4**: Midday peak pattern

Output: `ec_pc_loading_interpretation.png`

### EC2: Eigenvalue Gap & Davis-Kahan Bound

Computes spectral gaps and applies the Davis-Kahan sin(θ) theorem: `sin(θ_k) ≤ ||ΔΣ||_op / gap_k`. Provides **theoretical** stability guarantees that complement the empirical bootstrap results:
- Gap₁ = 460,777 → PC1 sensitivity = 2.17×10⁻⁶ (nearly zero perturbation possible)
- Gap₂ = 15,965 → PC2 sensitivity = 6.26×10⁻⁵

Output: `ec_eigenvalue_gap_analysis.png`

### EC3: Hill Estimator for Alpha

Maximum likelihood-based tail index estimation (alternative to log-log regression). The Hill estimator computes α(k) for each number of upper order statistics k:
- Hill median α ≈ 3.99 (IQR: [3.76, 4.48])
- Log-log regression α = 5.12
- Both high, confirming LIGHT tails via two independent methods

Output: `ec_hill_estimator.png`

### EC4: Broken Stick Model

Tests how many PCs are statistically significant under a null model:
- Only PC1 (89.9%) exceeds its broken stick threshold (15.7%)
- PC2 (5.1%) falls below the null expectation (11.6%)
- Characteristic of unnormalized count data where magnitude dominates

Output: `ec_broken_stick.png`

### EC5: Per-Component Tail Analysis

Examines tail behavior per eigenvector individually (not pooled). All 6 leading PCs show negative excess kurtosis (−0.69 to −1.20), uniformly confirming light tails and ruling out the possibility that pooling masked heavy-tailed behavior in individual eigenvectors.

Output: `ec_per_component_tails.png`

### Extended Analysis Outputs

| File | Description |
|------|-------------|
| `ec_pc_loading_interpretation.png` | PC1–PC4 hourly loading bar charts |
| `ec_eigenvalue_gap_analysis.png` | Eigenvalue spectrum, gaps, Davis-Kahan sensitivity |
| `ec_hill_estimator.png` | Hill plot with log-log regression comparison |
| `ec_broken_stick.png` | Observed vs broken stick variance proportions |
| `ec_per_component_tails.png` | Per-component loading histograms with kurtosis |
| `extended_analysis_report.json` | Full numerical results from all analyses |

---

## File Listing

```
mapping_pca_and_bootstrap_stability/
  pca_part1_unnormalized.py           # Part 1: PCA via Dask
  pca_part2_tail_analysis.py          # Part 2: Tail analysis
  part3_folium_map.py                 # Part 3: Folium map
  pca_part4_bootstrap_stability.py    # Part 4: Bootstrap stability
  extended_analysis.py                # Extended analyses (EC1–EC5)
  pca_model.pkl                       # PCA model (Part 1 output)
  variance_explained.png              # Scree + cumulative plot (Part 1)
  coefficient_distribution.png        # 3-panel tail diagnostic (Part 2)
  tail_analysis_report.json           # Tail classification report (Part 2)
  pc1_pc2_folium_map.html             # Interactive map (Part 3)
  bootstrap_stability_report.json     # Bootstrap metrics (Part 4)
  bootstrap_stability_report_summary.txt  # Human-readable summary (Part 4)
  eigenvector_stability.png           # Bootstrap stability plots (Part 4)
  ec_pc_loading_interpretation.png    # Extended: PC loading patterns
  ec_eigenvalue_gap_analysis.png      # Extended: Eigenvalue gaps
  ec_hill_estimator.png               # Extended: Hill plot
  ec_broken_stick.png                 # Extended: Broken stick model
  ec_per_component_tails.png          # Extended: Per-component tails
  extended_analysis_report.json       # Extended: Full numerical results
  test_part2.sh                       # Test script for Part 2
  README_PART2.md                     # This file
```

## Dependencies

All dependencies are listed in `../requirements.txt`:

```
pandas, numpy, pyarrow          # Core data processing
dask[dataframe]                 # Distributed covariance computation
s3fs, fsspec, boto3             # S3 access
matplotlib                      # Plotting
scipy                           # Statistical tests (Part 2, Extended analyses)
folium, branca, geopandas       # Mapping (Part 3)
```

Install with: `pip install -r requirements.txt`
