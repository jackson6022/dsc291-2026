DSC 291 HW4 — XGBoost Taxi Type Classification
==============================================

This README describes how to run and understand the `hw4.ipynb` notebook in this folder. The notebook implements:

- **Part A**: Classification of taxi type (yellow vs green) from raw trip-level data.
- **Part B**: Classification of taxi type from pivoted hourly profiles reduced by PCA.
- **Extra credit**: Enhanced Part B model with metadata features and bootstrap-based stability and confidence analyses.

The notebook is designed to execute top-to-bottom and to be reproducible when run with the same data and environment.

---

Dependencies
------------

The notebook expects the following Python packages:

- `numpy`
- `pandas`
- `dask[dataframe]`
- `pyarrow`
- `s3fs`
- `xgboost`
- `scikit-learn`
- `matplotlib`
- `python-dotenv`
  
You can install them with:

```bash
pip install numpy pandas dask[DataFrame] pyarrow s3fs xgboost scikit-learn matplotlib python-dotenv
```

---

Data & AWS Credentials
----------------------

The notebook reads data from two S3 locations:

- **Part A raw trips**: `s3://dsc291-ucsd/taxi`
- **Part B pivoted profiles**: `s3://291-s3-bucket/wide.parquet`

AWS credentials are loaded from environment variables. For local use in this homework, the recommended pattern is:

1. Create a `.env` file in this `HW4/` directory with:
   ```bash
   AWS_ACCESS_KEY_ID=YOUR_KEY
   AWS_SECRET_ACCESS_KEY=YOUR_SECRET
   AWS_SESSION_TOKEN=YOUR_SESSION_TOKEN
   ```
2. Make sure `.env` is **not** committed (it is already in `.gitignore`).

At runtime, the notebook:

- Searches common paths for `.env` (including this `HW4/` directory).
- Calls `load_dotenv(..., override=True)` to ensure fresh AWS\_* values.
- Performs a small sanity check (flags session tokens with embedded spaces).

If S3 access fails, check:

- `.env` format (no `export` prefixes; no spaces in token).
- That your AWS credentials are valid and not expired.

---

Reproducibility
---------------

To make results reproducible:

- A global seed is set near the top:
  ```python
  # RANDOM_SEED is defined in the shared config cell
  np.random.seed(RANDOM_SEED)
  ```
- All uses of `train_test_split` set `random_state=RANDOM_SEED`.
- All `XGBClassifier` models set `random_state=RANDOM_SEED`.
- To further reduce run-to-run variance from parallelism, the notebook sets `n_jobs=1` in `XGBClassifier`.
- Part A sampling uses deterministic seeds per class.
- Extra-credit bootstrap explicitly seeds `np.random` and uses deterministic sampling.

Given the same environment, S3 data, and package versions, rerunning the notebook from top-to-bottom should yield the same metrics and plots (up to minor numeric differences from XGBoost parallelism).

---

Pipeline Overview
-----------------

### Part A: Raw trip-level classification

1. **Configuration**
   - Paths for raw and pivoted data.
   - `MAX_ROWS_PER_CLASS`, `RANDOM_SEED`, `TRAIN_FRAC`.

2. **Discovery & normalization**
   - Uses `fsspec` to discover yellow/green Parquet files in S3.
   - Normalizes schema across vendors (pickup/dropoff times, distance, passenger count).

3. **Loading + sampling**
   - Reads one yellow and one green file (matched by year/month when possible) with Dask.
   - **Approximate downsampling in Dask** (`frac=0.10`) before converting to pandas for speed.
   - **Exact per-class cap in pandas** (`sample(n=..., random_state=...)`) to keep the final sample size reproducible.

4. **Feature engineering**
   - Converts pickup/dropoff to datetimes.
   - Derives:
     - `trip_duration_min` (minutes)
     - `hour` (0–23)
     - `day_of_week` (0=Monday, 6=Sunday)
   - Drops missing values and invalid trips (distance <= 0 or duration <= 0).

5. **Modeling & evaluation**
   - Uses `FEATURE_COLS_A = [trip_distance, trip_duration_min, hour, day_of_week, passenger_count]`.
   - Label-encodes `taxi_type` (`green`, `yellow`).
   - Stratified train/test split.
   - Trains an `XGBClassifier` with `objective="multi:softmax"`, `num_class` set from labels.
   - Prints accuracy and a full classification report.
   - Plots:
     - Confusion matrix
     - Feature importance
     - Boxplots of features for one taxi type.

### Part B: PCA on pivoted hourly profiles

1. **Configuration**
   - `PIVOT_PARQUET_PATH`, `RANDOM_SEED`, `TRAIN_FRAC`, `N_PCA_COMPONENTS = 5`.

2. **Loading & filtering**
   - Reads a wide pivoted Parquet table with Dask.
   - Normalizes schema, and keeps only rows with `taxi_type` in `{yellow, green}` and `hour_0`–`hour_23`.

3. **Features & PCA**
   - Builds a 24-dimensional hourly count profile.
   - Converts counts to per-row proportions; drops rows with all-zero profiles.
   - Encodes `taxi_type` labels.
   - Stratified train/test split on **raw proportions**.
   - Fits `PCA(n_components=5, random_state=RANDOM_SEED)` on TRAIN only and transforms train/test.

4. **Modeling & evaluation**
   - Trains an `XGBClassifier` on the 5 PCA features with correct `num_class`.
   - Prints accuracy + per-class precision/recall/F1.
   - Plots:
     - Confusion matrix
     - Feature importance for `PC1..PC5`.

5. **Interpretation**
   - Notes that while overall accuracy is high (~0.91), recall for green is very low due to class imbalance.
   - The notebook **explicitly prints macro-F1 and per-class recall** (in addition to the full classification report), since accuracy can be misleading under imbalance.
   - Suggests looking at macro-F1 / per-class recall and considering additional features (as in extra credit).
   - Uses a single shared `RANDOM_SEED` (defined once in config) for splits/sampling.
   - Uses **approximate Dask downsampling** for speed, then an **exact pandas cap** for reproducible per-class sample size.
   - Sets `n_jobs=1` in XGBoost models to reduce run-to-run variability from parallelism.

---

### Extra Credit (Optional)

The extra-credit section:

- Adds metadata features from `date` and `pickup_place` (calendar features, lon/lat, zone id).
- Reuses the same train/test split indices as the main Part B model.
- Fits a new PCA (train-only) and XGBoost model with both PCs and metadata.
- Evaluates performance, plots confusion matrix and feature importance for the augmented feature set.
- Uses bootstrap resampling to report stability (accuracy & F1 confidence intervals).
- Analyzes prediction confidence for correct vs incorrect predictions.

---

How to Run
----------

1. Ensure all dependencies are installed and AWS credentials are configured (see above).
2. Open `hw4.ipynb` in Jupyter or VSCode/Cursor.
3. Run all cells top-to-bottom.
4. Review:
   - Part A summary, metrics, and plots.
   - Part B summary, PCA variance explained, metrics, and plots.
   - Extra-credit section for improved model and stability analysis (optional).

If a cell fails:

- Check the traceback for S3/credential issues vs. pure Python errors.
- Verify `.env` formatting and that all imports succeed.

---

Common issues and fixes
-----------------------

- **AWS / S3 credential errors**  
  - Symptoms: `InvalidAccessKeyId`, `AccessDenied`, `ExpiredToken`, or `NoCredentialsError` when calling `dd.read_parquet`.  
  - Fixes:
    - Ensure `.env` is in the `HW4/` folder (or another searched path) and has **no** `export` prefixes:
      ```bash
      AWS_ACCESS_KEY_ID=...
      AWS_SECRET_ACCESS_KEY=...
      AWS_SESSION_TOKEN=...
      ```
    - Make sure there are **no spaces** inside `AWS_SESSION_TOKEN`.
    - If your credentials are **temporary** (common for course AWS access), they may **expire**:
      - Fetch/get a fresh set of AWS credentials from the source your course uses.
      - Update your `.env` with the new values.
      - Re-run the dotenv cell (or restart the kernel) so `AWS_*` env vars are reloaded with `override=True`.

- **Notebook crashes under `nbconvert` (exit code 134 / config issues)**  
  - Sometimes `jupyter nbconvert --execute` fails because Jupyter tries to write config into a non-writable location.  
  - Fix: set `IPYTHONDIR` and `JUPYTER_CONFIG_DIR` to a local, writable folder when running nbconvert, e.g.:
    ```bash
    export IPYTHONDIR=./.ipython
    export JUPYTER_CONFIG_DIR=./.jupyter
    jupyter nbconvert --to notebook --execute hw4.ipynb --output hw4_executed.ipynb
    ```

- **Out-of-memory / very slow runs when loading Part A data**  
  - Symptoms: kernel dies or hangs during Dask `read_parquet` on large trip files.  
  - Fixes:
    - Keep `MAX_ROWS_PER_CLASS` at a moderate value (e.g. `50_000` or lower).
    - Leave the **approximate Dask downsampling** (`frac=0.10`) enabled so only a subset is materialized to pandas.

- **XGBoost warnings about `use_label_encoder`**  
  - You may see a warning that `use_label_encoder` is ignored.  
  - This is expected with modern XGBoost; the notebook already sets `eval_metric` explicitly, so you can safely ignore the warning.

- **Plots not showing in some environments**  
  - Ensure `matplotlib` is installed and the notebook is run in a standard Jupyter / VSCode / Cursor environment.  
  - If running headless (e.g. CI), make sure the backend supports non-interactive rendering or run nbconvert with an appropriate profile.

---

Key Design Choices
------------------

- **Train-only PCA** to avoid data leakage from test into feature extraction.
- **Stratified splits** for both parts to preserve class balance properties.
- **Seeding all randomness** used in:
  - Sampling
  - Train/test splitting
  - XGBoost models
  - Extra-credit bootstrap.
- **Separation of concerns**:
  - Clear helpers for discovery, schema normalization, and loading.
  - Explicit feature-engineering cells.
  - Dedicated modeling and interpretation cells for each part.

