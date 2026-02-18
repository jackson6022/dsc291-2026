# HW2: Mapping PCA, Tail Analysis, and Bootstrap Stability
**Points:** 100 · **Due:** Feb 20 5PM (5 points deducted for every hour late)

**Prerequisite:** HW1 produces a single Parquet wide table with schema `(taxi_type, date, pickup_place) × hour_0..hour_23` (unnormalized counts). Rows with fewer than 50 rides are discarded.

**Input:** The Parquet output from HW1 (from S3).

---

## Part 1: PCA on Unnormalized Pivot Tables

Fit PCA on the wide table **without** row normalization (L1) or column standardization.

- Load HW1 Parquet; extract feature matrix `X`: rows = `(taxi_type, date, pickup_place)`, columns = `hour_0..hour_23`
- Fit PCA:Use Dask to compute the covariance matrix. Perform the averaging of the outer products only once. Replace missing values with average of column (excluding missing values)
- Save: `pca_model.pkl` (contains orthonormal matrix and variances vector), `variance_explained.png`
---

## Part 2: Distribution of Individual Coefficients — Light Tail vs Heavy Tail, Alpha

Analyze the distribution of PCA coefficients (eigenvector loadings) to characterize tail behavior.

- Pool loading values from eigenvectors; produce histogram, Q-Q plot, log-log survival plot
- Classify tail: light (Gaussian-like) vs heavy (power-law)
- Estimate alpha (tail exponent) via power-law fit; report alpha and R^2
- Outputs: `coefficient_distribution.png`, `tail_analysis_report.json`

---

## Part 3: First and Second Coefficients on a Folium Map

Visualize PC1 and PC2 scores on an interactive Folium map of NYC taxi zones.

- Aggregate PC scores by `pickup_place`; join with zone coordinates (dataset includes table of zone to coordinates)
- Color zones by PC1; encode PC2 (size, opacity, or second map/layer)
- Save: `pc1_pc2_folium_map.html`

---

## Part 4: Bootstrap Stability of Eigenvectors

Assess eigenvector stability under bootstrap resampling of rows.

- Resample rows with replacement B times; fit PCA for each; extract first K eigenvectors
- Compute: subspace affinity, Procrustes distance, component-wise correlation (bootstrap vs original)
- Visualize: bootstrap PC1 band, boxplot of correlations
- Outputs: `bootstrap_stability_report.json`, `eigenvector_stability.png`

---

## Deliverables

| Deliverable | Description |
|-------------|-------------|
| PCA on unnormalized data | Model, components, scores, variance explained |
| Tail analysis | Light/heavy tail classification, alpha estimate |
| Folium map | PC1 and PC2 by pickup_place |
| Bootstrap stability | Metrics and plots for eigenvector stability |
| Report (MS Word) | Narrative, figures, and interpretations |
