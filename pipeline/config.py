"""
Central configuration for the hospital pipeline.

Keeping all configuration here means that changing a path, query,
or constant requires editing exactly one file, not hunting through
the codebase. It also makes the pipeline easy to adapt to other
countries or amenity types in future.
"""

from datetime import date

# ── Paths ─────────────────────────────────────────────────────────────────────

# Top-level data directory — can be overridden by environment variable
# to point at an S3-compatible bucket mount or network share.
DATA_DIR = "data"

BRONZE_DIR = f"{DATA_DIR}/bronze"
SILVER_DIR = f"{DATA_DIR}/silver"
LOG_DIR = "logs"

# ── Partitioning ──────────────────────────────────────────────────────────────

# Partition key: ingestion date.
# Using today's date as the partition means each pipeline run is isolated.
# Re-running on the same day overwrites the existing partition (idempotent).
# Re-running on a new day creates a new partition, preserving history.
PARTITION_DATE: str = date.today().isoformat()  # e.g. "2026-06-08"

BRONZE_PATH = f"{BRONZE_DIR}/{PARTITION_DATE}/hospitals_raw.geojson"
SILVER_PATH = f"{SILVER_DIR}/{PARTITION_DATE}/hospitals.parquet"

# ── OpenStreetMap / Overpass API ──────────────────────────────────────────────

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Timeout in seconds for the Overpass API HTTP request.
# UK hospital data is large (~2,000+ features); 90s is conservative.
OVERPASS_TIMEOUT = 90

# Overpass QL query:
# - Scopes to the United Kingdom via ISO3166-1 country code
# - Fetches nodes, ways, and relations tagged amenity=hospital
# - "out center" returns a representative centroid for ways/relations
#   so every feature has a usable point geometry regardless of type
OVERPASS_QUERY = """
[out:json][timeout:{timeout}];
area["ISO3166-1"="GB"][admin_level=2]->.uk;
(
  node["amenity"="hospital"](area.uk);
  way["amenity"="hospital"](area.uk);
  relation["amenity"="hospital"](area.uk);
);
out center tags;
""".strip()

# ── Coordinate bounds — United Kingdom ────────────────────────────────────────
# Used in data quality checks to detect clearly erroneous coordinates.
UK_BOUNDS = {
    "min_lon": -8.65,
    "max_lon": 1.76,
    "min_lat": 49.88,
    "max_lat": 60.86,
}

# ── Pipeline metadata ─────────────────────────────────────────────────────────
PIPELINE_VERSION = "1.0.0"

# ── Silver zone schema ────────────────────────────────────────────────────────
# Canonical column names for the silver layer.
# Explicit schema makes downstream consumers predictable and simplifies
# any future migration to a schema registry.
SILVER_COLUMNS = [
    "osm_id",
    "osm_type",
    "name",
    "operator",
    "emergency",
    "beds",
    "phone",
    "website",
    "addr_street",
    "addr_city",
    "addr_postcode",
    "addr_country",
    "latitude",
    "longitude",
    "geometry",
    "ingested_at",
    "pipeline_version",
    "quality_flags",
]
