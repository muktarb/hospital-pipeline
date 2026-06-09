"""
Silver zone: Transform raw GeoJSON (bronze) into clean GeoParquet (silver).

Steps
-----
1. Load the bronze GeoJSON into a GeoDataFrame.
2. Normalise the schema — extract known OSM tags into typed columns,
   drop the raw 'properties' blob.
3. Apply data quality checks (see quality.py).
4. Write to GeoParquet, partitioned by ingestion date.

Design decisions
----------------
- We use EPSG:4326 (WGS84) as the coordinate reference system throughout.
  This is the native CRS of OSM and the most interoperable choice for
  downstream consumers including Trino's geospatial functions.
- GeoParquet is chosen over plain Parquet because it embeds CRS metadata
  and geometry encoding in a standard way, making the file self-describing
  and compatible with tools like QGIS, DuckDB, and Trino without extra
  configuration.
- String columns are cast to nullable string (pd.StringDtype) to
  distinguish genuinely missing values (pd.NA) from empty strings.
- The 'beds' column is coerced to nullable integer — OSM tags are always
  strings, so non-numeric values are set to NA rather than crashing.
"""

import json
import os
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, shape

from pipeline import config
from pipeline.logging_config import get_logger
from pipeline.quality import run_quality_checks

logger = get_logger(__name__)

# OSM tag → silver column name mapping.
# Only tags in this map are promoted to first-class columns.
# All other tags remain available via the raw bronze GeoJSON if needed.
TAG_MAP = {
    "name": "name",
    "operator": "operator",
    "emergency": "emergency",
    "beds": "beds",
    "phone": "phone",
    "website": "website",
    "addr:street": "addr_street",
    "addr:city": "addr_city",
    "addr:postcode": "addr_postcode",
    "addr:country": "addr_country",
}


def _load_bronze(bronze_path: str) -> list[dict]:
    """Load and validate the bronze GeoJSON file."""
    logger.info("Loading bronze file: %s", bronze_path)

    with open(bronze_path, encoding="utf-8") as f:
        data = json.load(f)

    if data.get("type") != "FeatureCollection":
        raise ValueError(f"Expected GeoJSON FeatureCollection, got: {data.get('type')}")

    features = data.get("features", [])
    logger.info("Loaded %d features from bronze", len(features))
    return features


def _features_to_geodataframe(features: list[dict]) -> gpd.GeoDataFrame:
    """
    Convert a list of GeoJSON features into a typed GeoDataFrame.

    Each feature's properties are unpacked; only tags in TAG_MAP are
    promoted to named columns. Geometry is converted to Shapely objects.
    """
    rows = []

    for feature in features:
        props = feature.get("properties", {})
        geom_dict = feature.get("geometry")

        # Build Shapely geometry from GeoJSON geometry dict
        try:
            geometry = shape(geom_dict) if geom_dict else None
        except Exception as exc:
            logger.debug("Could not parse geometry for osm_id=%s: %s", props.get("osm_id"), exc)
            geometry = None

        row = {
            "osm_id": props.get("osm_id"),
            "osm_type": props.get("osm_type"),
            "geometry": geometry,
        }

        # Extract coordinates for convenience columns
        if isinstance(geometry, Point):
            row["longitude"] = geometry.x
            row["latitude"] = geometry.y
        else:
            row["longitude"] = None
            row["latitude"] = None

        # Promote mapped OSM tags to named columns
        for osm_tag, col_name in TAG_MAP.items():
            row[col_name] = props.get(osm_tag)

        # Pipeline metadata
        row["ingested_at"] = datetime.now(timezone.utc).isoformat()
        row["pipeline_version"] = config.PIPELINE_VERSION

        rows.append(row)

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    logger.info("Created GeoDataFrame with %d rows and %d columns", len(gdf), len(gdf.columns))
    return gdf


def _apply_types(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Apply explicit dtypes to silver columns.

    Using nullable dtypes (pd.StringDtype, pd.Int64Dtype) means that
    missing values are represented as pd.NA rather than NaN or None,
    which round-trips correctly through Parquet.
    """
    string_cols = [
        "osm_type", "name", "operator", "emergency", "phone",
        "website", "addr_street", "addr_city", "addr_postcode", "addr_country",
    ]

    for col in string_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].astype(pd.StringDtype())

    # 'beds' is an OSM tag string — coerce to nullable integer
    if "beds" in gdf.columns:
        gdf["beds"] = pd.to_numeric(gdf["beds"], errors="coerce").astype(pd.Int64Dtype())

    # osm_id should be integer
    if "osm_id" in gdf.columns:
        gdf["osm_id"] = pd.to_numeric(gdf["osm_id"], errors="coerce").astype(pd.Int64Dtype())

    return gdf


def transform(bronze_path: str) -> str:
    """
    Transform the bronze GeoJSON into a clean GeoParquet file in the silver zone.

    Parameters
    ----------
    bronze_path : str
        Path to the bronze GeoJSON file produced by extract.py.

    Returns
    -------
    str
        Path to the silver GeoParquet file.
    """
    silver_path = config.SILVER_PATH

    # ── Load bronze ───────────────────────────────────────────────────────
    features = _load_bronze(bronze_path)

    # ── Convert to GeoDataFrame ───────────────────────────────────────────
    gdf = _features_to_geodataframe(features)

    # ── Apply types ───────────────────────────────────────────────────────
    gdf = _apply_types(gdf)

    # ── Data quality checks ───────────────────────────────────────────────
    gdf = run_quality_checks(gdf)

    # ── Write GeoParquet ──────────────────────────────────────────────────
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)

    # GeoParquet written with snappy compression — good balance of
    # compression ratio and read speed for columnar analytics workloads.
    gdf.to_parquet(silver_path, compression="snappy", index=False)

    size_kb = os.path.getsize(silver_path) / 1024
    logger.info(
        "Silver file written: %s (%.1f KB, %d records)",
        silver_path,
        size_kb,
        len(gdf),
    )

    return silver_path
