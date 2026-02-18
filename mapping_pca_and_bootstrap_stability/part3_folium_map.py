"""
Part 3: First and Second Coefficients on a Folium Map (HW2).

Visualize PC1 and PC2 scores on an interactive Folium map of NYC taxi zones:
- Aggregate PC scores by pickup_place; join with zone coordinates.
- Color zones by PC1; encode PC2 (size, opacity, or second map/layer).
- Save: pc1_pc2_folium_map.html

Uses the PCA model (pca_model.pkl) from this folder (Part 1).
"""

from __future__ import annotations

import logging
import pickle
import tempfile
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Align with Part 1
WIDE_PARQUET_PATH = "s3://291-s3-bucket/wide.parquet"
HOUR_COLS = [f"hour_{i}" for i in range(24)]

# Default: PCA model in this folder
_THIS_DIR = Path(__file__).resolve().parent
DEFAULT_PCA_MODEL = str(_THIS_DIR / "pca_model.pkl")

# Course S3 zone lookup (CSV has LocationID, Borough, Zone; no lat/lon — use sibling .shp for coords)
DEFAULT_ZONE_COORDS_S3 = "s3://dsc291-ucsd/taxi/Dataset/02.taxi_zones/taxi+_zone_lookup.csv"


def _infer_id_col(columns: list[str]) -> Optional[str]:
    candidates = [
        "pickup_place", "PULocationID", "pulocationid",
        "LocationID", "locationid", "zone_id", "zoneid",
    ]
    lower_map = {c: str(c).strip().lower() for c in columns}
    for cand in candidates:
        cand_l = cand.lower()
        for orig, low in lower_map.items():
            if low == cand_l:
                return orig
    return None


def _infer_lat_lon_cols(columns: list[str]) -> Tuple[Optional[str], Optional[str]]:
    lower_map = {c: str(c).strip().lower() for c in columns}
    lat_cands = ["lat", "latitude", "y", "centroid_lat", "center_lat"]
    lon_cands = ["lon", "lng", "longitude", "x", "centroid_lon", "centroid_lng", "center_lon", "center_lng"]
    lat_col = None
    lon_col = None
    for cand in lat_cands:
        for orig, low in lower_map.items():
            if low == cand:
                lat_col = orig
                break
        if lat_col:
            break
    for cand in lon_cands:
        for orig, low in lower_map.items():
            if low == cand:
                lon_col = orig
                break
        if lon_col:
            break
    return lat_col, lon_col


def _storage_options(anon: bool) -> Optional[Dict[str, Any]]:
    return {"anon": True} if anon else None


def load_pca_components(pca_model_path: str) -> np.ndarray:
    """Load components matrix (24, 24) from pca_model.pkl from Part 1."""
    with open(pca_model_path, "rb") as f:
        obj = pickle.load(f)
    if not isinstance(obj, dict) or "components" not in obj:
        raise ValueError(f"Expected dict with 'components' in {pca_model_path}")
    comps = np.asarray(obj["components"], dtype=np.float64)
    if comps.shape[0] != 24:
        raise ValueError(f"Expected components (24, 24); got {comps.shape}")
    return comps


def _sibling_shapefile_path(zone_coords_path: str) -> str:
    """Return path to taxi_zones.shp in the same directory as zone_coords_path."""
    if zone_coords_path.startswith("s3://"):
        parts = zone_coords_path.rsplit("/", 1)
        prefix = parts[0] if len(parts) == 2 else ""
        return f"{prefix}/taxi_zones.shp"
    p = Path(zone_coords_path).resolve().parent
    return str(p / "taxi_zones.shp")


def _load_zone_coords_from_shapefile(
    shp_path: str,
    storage_options: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Load zone id + centroid (lat, lon) from a shapefile. Requires geopandas."""
    try:
        import geopandas as gpd  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "geopandas is required to use zone coordinates from a shapefile. "
            "Install with: pip install geopandas"
        ) from None
    logger.info("Loading zone centroids from shapefile: %s", shp_path)
    if storage_options:
        gdf = gpd.read_file(shp_path, storage_options=storage_options)
    else:
        gdf = gpd.read_file(shp_path)
    gdf = gdf.to_crs("EPSG:4326")
    gdf["lat"] = gdf.geometry.centroid.y
    gdf["lon"] = gdf.geometry.centroid.x
    id_col = _infer_id_col([c for c in gdf.columns if c != "geometry"])
    if id_col is None:
        for c in gdf.columns:
            if c.lower() in ("locationid", "objectid", "zone_id", "id"):
                id_col = c
                break
        if id_col is None and len(gdf.columns) > 1:
            id_col = [c for c in gdf.columns if c not in ("geometry", "lat", "lon")][0]
    out = gdf[[id_col, "lat", "lon"]].copy()
    out = out.rename(columns={id_col: "pickup_place"})
    out["pickup_place"] = out["pickup_place"].astype(str)
    out = out.dropna(subset=["lat", "lon"])
    return out


def _resolve_zone_coords_path(zone_coords_path: str) -> str:
    """If path is a URL, download to a temp file and return local path."""
    s = zone_coords_path.strip().lower()
    if s.startswith("http://") or s.startswith("https://"):
        logger.info("Downloading zone coordinates from %s", zone_coords_path)
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            urllib.request.urlretrieve(zone_coords_path, f.name)
            return f.name
    return zone_coords_path


def load_zone_coordinates(
    zone_coords_path: str,
    storage_options: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Load zone→(lat, lon) table. Returns DataFrame with pickup_place, lat, lon.
    zone_coords_path can be a local .csv/.tsv/.parquet path, or a URL to a CSV.
    """
    path_lower = zone_coords_path.strip().lower()
    if path_lower.startswith("http://") or path_lower.startswith("https://"):
        zone_coords_path = _resolve_zone_coords_path(zone_coords_path)
        path_lower = zone_coords_path.lower()
        storage_options = None  # local file after download
    if path_lower.endswith(".shp"):
        return _load_zone_coords_from_shapefile(zone_coords_path, storage_options)
    if path_lower.endswith(".parquet"):
        z = pd.read_parquet(zone_coords_path, storage_options=storage_options)
    elif path_lower.endswith(".csv") or path_lower.endswith(".tsv"):
        sep = "\t" if path_lower.endswith(".tsv") else ","
        z = pd.read_csv(zone_coords_path, storage_options=storage_options, sep=sep)
    else:
        raise ValueError(
            "zone_coords_path must be .csv, .tsv, .parquet, .shp, or a URL to a CSV. "
            "Example (course S3): s3://dsc291-ucsd/taxi/Dataset/02.taxi_zones/taxi+_zone_lookup.csv"
        )

    id_col = _infer_id_col(list(z.columns))
    if id_col is None:
        raise ValueError(f"No zone id column in {zone_coords_path}. Columns: {list(z.columns)}")
    lat_col, lon_col = _infer_lat_lon_cols(list(z.columns))
    if lat_col is None or lon_col is None:
        shp_path = _sibling_shapefile_path(zone_coords_path)
        try:
            return _load_zone_coords_from_shapefile(shp_path, storage_options)
        except Exception as e:
            raise ValueError(
                f"No lat/lon in {zone_coords_path} and could not load {shp_path}: {e}. "
                "Use a file with zone id + lat + lon, or pass path to taxi_zones.shp."
            ) from e

    out = z[[id_col, lat_col, lon_col]].copy()
    out = out.rename(columns={id_col: "pickup_place", lat_col: "lat", lon_col: "lon"})
    out["pickup_place"] = out["pickup_place"].astype(str)
    out["lat"] = pd.to_numeric(out["lat"], errors="coerce")
    out["lon"] = pd.to_numeric(out["lon"], errors="coerce")
    out = out.dropna(subset=["lat", "lon"])
    return out


def aggregate_pc_scores_by_pickup_place(
    wide_parquet_path: str,
    components: np.ndarray,
    storage_options: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Aggregate PC scores by pickup_place.
    Uses same imputation/centering as Part 1: NaN → column mean, then center.
    mean(score | zone) = mean(X_centered | zone) @ components[:, :2].
    """
    logger.info("Loading wide parquet: %s", wide_parquet_path)
    try:
        df = pd.read_parquet(wide_parquet_path, storage_options=storage_options)
    except (PermissionError, OSError) as e:
        if "Forbidden" in str(e) or "403" in str(e) or "Access Denied" in str(e):
            raise PermissionError(
                "S3 access denied (403 Forbidden). Use a local wide table instead: "
                "--wide /path/to/wide.parquet"
            ) from e
        raise
    logger.info("Loaded wide parquet: %d rows", len(df))
    if "pickup_place" not in df.columns:
        df = df.reset_index()
    missing = [c for c in (["pickup_place"] + HOUR_COLS) if c not in df.columns]
    if missing:
        raise ValueError(f"Wide table missing: {missing}")

    df["pickup_place"] = df["pickup_place"].astype(str)
    X = df[HOUR_COLS].astype(np.float64)
    means = X.mean(skipna=True)
    X_centered = X.fillna(means) - means
    centered_with_zone = pd.concat([df[["pickup_place"]], X_centered], axis=1)
    zone_mean_centered = centered_with_zone.groupby("pickup_place")[HOUR_COLS].mean()

    comps2 = np.asarray(components[:, :2], dtype=np.float64)
    scores = zone_mean_centered.values @ comps2
    return pd.DataFrame({
        "pickup_place": zone_mean_centered.index.astype(str),
        "pc1": scores[:, 0],
        "pc2": scores[:, 1],
    })


def build_folium_map(
    zone_scores: pd.DataFrame,
    zone_coords: pd.DataFrame,
    out_html_path: str,
) -> None:
    """Color zones by PC1; encode PC2 as circle size (second layer). Save HTML."""
    import folium  # type: ignore[import-untyped]
    from branca.colormap import linear  # type: ignore[import-untyped]

    merged = zone_scores.merge(zone_coords, on="pickup_place", how="inner")
    if merged.empty:
        raise ValueError("No rows after joining scores with zone coordinates (check id types).")

    pc1 = merged["pc1"].astype(float)
    m = max(abs(pc1.min()), abs(pc1.max())) or 1.0
    colormap = linear.RdBu_11.scale(-m, m)
    colormap.caption = "PC1"

    pc2_abs = merged["pc2"].abs().astype(float)
    pc2_max = float(pc2_abs.max()) or 1.0

    center_lat = float(merged["lat"].mean())
    center_lon = float(merged["lon"].mean())
    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=10, tiles="cartodbpositron")

    fg_pc1 = folium.FeatureGroup(name="PC1 (color)", show=True)
    fg_pc2 = folium.FeatureGroup(name="PC2 (size)", show=False)

    for row in merged.itertuples(index=False):
        lat, lon = float(row.lat), float(row.lon)
        pc1_val, pc2_val = float(row.pc1), float(row.pc2)
        tooltip = f"pickup_place={row.pickup_place} | PC1={pc1_val:.4f} | PC2={pc2_val:.4f}"

        folium.CircleMarker(
            location=(lat, lon), radius=6,
            color=colormap(pc1_val), fill=True, fill_color=colormap(pc1_val),
            fill_opacity=0.8, opacity=0.9, tooltip=tooltip,
        ).add_to(fg_pc1)

        radius = 3.0 + 9.0 * (abs(pc2_val) / pc2_max)
        folium.CircleMarker(
            location=(lat, lon), radius=radius,
            color="#333", fill=True, fill_color="#333",
            fill_opacity=0.25, opacity=0.35, tooltip=tooltip,
        ).add_to(fg_pc2)

    fg_pc1.add_to(fmap)
    fg_pc2.add_to(fmap)
    colormap.add_to(fmap)
    folium.LayerControl(collapsed=False).add_to(fmap)

    out_path = Path(out_html_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fmap.save(str(out_path))
    logger.info("Saved %s", out_path)


def main(
    wide_parquet_path: str,
    pca_model_path: str,
    zone_coords_path: str,
    output_html: str | None = None,
    anon: bool = False,
) -> None:
    if output_html is None:
        output_html = str(_THIS_DIR / "pc1_pc2_folium_map.html")
    opts = _storage_options(anon)
    components = load_pca_components(pca_model_path)
    zone_scores = aggregate_pc_scores_by_pickup_place(wide_parquet_path, components, storage_options=opts)
    zone_coords = load_zone_coordinates(zone_coords_path, storage_options=opts)
    build_folium_map(zone_scores, zone_coords, out_html_path=output_html)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Part 3: PC1/PC2 Folium map by pickup_place")
    parser.add_argument("--wide", default=WIDE_PARQUET_PATH, help="HW1 wide parquet (local or S3)")
    parser.add_argument("--pca-model", default=DEFAULT_PCA_MODEL, help="pca_model.pkl from Part 1")
    parser.add_argument(
        "--zone-coords",
        default=DEFAULT_ZONE_COORDS_S3,
        help="Zone lookup CSV or .shp (default: course S3 taxi+_zone_lookup.csv; uses sibling taxi_zones.shp for lat/lon)",
    )
    parser.add_argument("--output", default=None, help="Output HTML path (default: same dir as script)")
    parser.add_argument("--anon", action="store_true", help="Use anonymous S3 access")
    args = parser.parse_args()
    main(
        wide_parquet_path=args.wide,
        pca_model_path=args.pca_model,
        zone_coords_path=args.zone_coords,
        output_html=args.output or None,
        anon=args.anon,
    )
