# hospital-pipeline

A Python data pipeline that extracts UK hospital locations from OpenStreetMap and stores them using a **medallion architecture** (Bronze → Silver).

[![CI](https://github.com/muktarb/hospital-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/muktarb/hospital-pipeline/actions/workflows/ci.yml)

---

## Architecture

```
OpenStreetMap (Overpass API)
         │
         ▼
┌─────────────────────────────┐
│        BRONZE ZONE          │
│  data/bronze/YYYY-MM-DD/    │
│  hospitals_raw.geojson      │
│                             │
│  Raw GeoJSON FeatureCollection
│  Saved exactly as received  │
│  Partitioned by date        │
│  Idempotent — cached        │
└────────────┬────────────────┘
             │  transform.py
             ▼
┌─────────────────────────────┐
│        SILVER ZONE          │
│  data/silver/YYYY-MM-DD/    │
│  hospitals.parquet          │
│                             │
│  GeoParquet (EPSG:4326)     │
│  Typed columns              │
│  Quality-checked & flagged  │
│  Snappy-compressed          │
└─────────────────────────────┘
```

---

## Pipeline Stages

### Bronze — Raw extraction

`pipeline/extract.py` queries the [Overpass API](https://overpass-api.de) for all OSM elements tagged `amenity=hospital` within the UK. The response is converted to a GeoJSON `FeatureCollection` and saved verbatim.

**Idempotency:** if a bronze file already exists for today's partition date, the API call is skipped and the cached file is used. Pass `--force` to override this.

**OSM element types handled:**
- `node` — point feature with explicit lat/lon
- `way` and `relation` — queried with `out center` to return a representative centroid

### Silver — Transformation and quality checks

`pipeline/transform.py` reads the bronze GeoJSON and:

1. Converts each feature to a typed GeoDataFrame row
2. Promotes known OSM tags to named columns (see schema below)
3. Coerces types — `beds` to nullable integer, string columns to `pd.StringDtype`
4. Runs data quality checks (see below)
5. Writes GeoParquet with Snappy compression

---

## Data Quality

`pipeline/quality.py` applies the following checks:

| Check | Action | Flag code |
|---|---|---|
| Null or empty geometry | **Remove** — cannot store in GeoParquet | — |
| Duplicate OSM ID | **Remove** — keep first occurrence | — |
| Missing `name` tag | **Flag** — hospital retained | `MISSING_NAME` |
| Coordinates outside UK bounding box | **Flag** — record retained | `OUT_OF_BOUNDS` |

Flagged records are retained in the silver layer with the `quality_flags` column populated. This allows downstream consumers to filter by quality level without discarding potentially useful data.

---

## Silver Schema

| Column | Type | Description |
|---|---|---|
| `osm_id` | `Int64` | OpenStreetMap element ID |
| `osm_type` | `string` | `node`, `way`, or `relation` |
| `name` | `string` | Hospital name |
| `operator` | `string` | Operating organisation |
| `emergency` | `string` | Emergency department (`yes`/`no`) |
| `beds` | `Int64` | Bed count (where tagged) |
| `phone` | `string` | Phone number |
| `website` | `string` | Website URL |
| `addr_street` | `string` | Street address |
| `addr_city` | `string` | City |
| `addr_postcode` | `string` | Postcode |
| `addr_country` | `string` | Country code |
| `latitude` | `float64` | Latitude (WGS84) |
| `longitude` | `float64` | Longitude (WGS84) |
| `geometry` | `geometry` | Point geometry (EPSG:4326) |
| `ingested_at` | `string` | ISO 8601 UTC timestamp of ingestion |
| `pipeline_version` | `string` | Pipeline version string |
| `quality_flags` | `string` | `\|`-separated quality flag codes, empty if clean |

---

## Setup

```bash
# Clone
git clone https://github.com/muktarb/hospital-pipeline.git
cd hospital-pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## Running the Pipeline

```bash
# Full run (extract + transform)
python -m pipeline.run

# Force re-extraction even if today's bronze file already exists
python -m pipeline.run --force

# Extract only — inspect raw GeoJSON before transforming
python -m pipeline.run --bronze-only
```

Output files are written to `data/bronze/YYYY-MM-DD/` and `data/silver/YYYY-MM-DD/`.
Logs are written to `logs/pipeline_YYYY-MM-DD.log`.

---

## Running Tests

```bash
pytest tests/ -v --cov=pipeline --cov-report=term-missing
```

Tests use `unittest.mock` to avoid real HTTP requests. No network access required.

---

## Design Decisions and Trade-offs

### Partitioning by ingestion date

Each pipeline run writes to a date-stamped subdirectory (`YYYY-MM-DD/`). This means:
- **Re-running on the same day** overwrites the existing partition (idempotent)
- **Re-running on a new day** creates a new partition, preserving history
- Partitions can be queried independently for point-in-time analysis

A trade-off: date partitioning means the silver layer accumulates snapshots over time rather than maintaining a single current state. For a production system with daily runs, a compaction strategy (or a gold zone deduplicated view) would manage this.

### GeoParquet over plain Parquet

GeoParquet embeds CRS metadata and geometry encoding in a standardised way, making the file self-describing. Tools including QGIS, DuckDB, and Apache Sedona can read GeoParquet without additional configuration. Plain Parquet with WKB-encoded geometry requires consumers to know the encoding convention — an implicit contract that breaks silently.

### EPSG:4326 (WGS84) as the CRS

OSM data is natively in WGS84. Reprojecting to a national grid (e.g. OSGB36/EPSG:27700) would improve metric distance calculations within the UK but would introduce a transformation step and break interoperability with global tools. WGS84 was chosen for simplicity and interoperability; reprojection can be added as a gold zone step if required.

### Orchestration: plain Python rather than Prefect/Airflow

The exercise scope is a two-stage pipeline with a single dependency (bronze → silver). Introducing Prefect or Airflow would add significant infrastructure overhead for marginal benefit at this scale. The `run.py` orchestrator provides timing, error handling, and a structured summary log. Migrating to Prefect would require wrapping `extract()` and `transform()` as `@flow` and `@task` decorators — a straightforward change that does not require restructuring the core logic.

### Quality: flag rather than drop

Flagging rather than silently dropping records preserves data for audit and downstream filtering. A record with a missing name or slightly out-of-bounds coordinate may still be a valid hospital — the flag surfaces the issue without destroying information. Only records that are genuinely unusable (null geometry, duplicate ID) are removed.

---

## Bonus: Exposing Silver Zone Data via Trino

To expose the silver GeoParquet files for querying via Trino using S3-compatible object storage:

1. **Store silver files in MinIO** (or any S3-compatible service) instead of the local filesystem — change `SILVER_PATH` in `config.py` to an `s3://` URI and configure `geopandas`/`pyarrow` with the appropriate S3 credentials.

2. **Register the table in Hive Metastore** — create an external Hive table pointing at the MinIO bucket prefix:
   ```sql
   CREATE EXTERNAL TABLE hospitals (
     osm_id BIGINT,
     name STRING,
     latitude DOUBLE,
     longitude DOUBLE,
     geometry BINARY,
     quality_flags STRING,
     ...
   )
   STORED AS PARQUET
   LOCATION 's3://your-bucket/silver/'
   TBLPROPERTIES ('parquet.compress'='SNAPPY');
   ```

3. **Configure the Trino Hive connector** to point at the Metastore and MinIO endpoint.

4. **Query via Trino SQL** — with the geospatial plugin enabled, geometry columns can be queried using `ST_*` functions:
   ```sql
   SELECT name, ST_AsText(ST_GeomFromBinary(geometry)) AS location
   FROM hive.default.hospitals
   WHERE quality_flags = ''
     AND ST_Distance(
           ST_GeomFromBinary(geometry),
           ST_Point(-0.1276, 51.5074)  -- central London
         ) < 0.1
   ORDER BY name;
   ```

This approach separates storage (MinIO) from compute (Trino), enabling multiple engines to query the same data without duplication — the core value proposition of a Data Lakehouse architecture.

---

## Gold Zone

Although not implemented in this exercise, the gold zone would typically contain:

- **Aggregated / business-ready views** — e.g. hospital count by postcode district, emergency department coverage by region
- **Deduplicated current-state table** — merging daily silver snapshots into a single authoritative record per OSM ID
- **Joined datasets** — hospitals enriched with population data, deprivation indices, or clinical outcome statistics
- **Materialised outputs** — pre-computed for dashboard consumption, reducing query time for end users

The gold zone serves analysts and decision-makers directly; the bronze and silver zones serve engineers and data scientists who need raw or lightly-transformed data.

---

## Repository Structure

```
hospital-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── .github/
│   └── workflows/
│       └── ci.yml              # Lint + test on every push
├── pipeline/
│   ├── __init__.py
│   ├── config.py               # All configuration in one place
│   ├── logging_config.py       # Structured logging
│   ├── extract.py              # Bronze: OSM → GeoJSON
│   ├── transform.py            # Silver: GeoJSON → GeoParquet
│   ├── quality.py              # Data quality checks
│   └── run.py                  # Orchestrator
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_quality.py
└── data/                       # Created at runtime (gitignored)
    ├── bronze/
    └── silver/
```
