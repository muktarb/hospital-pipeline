"""
Bronze zone: Extract hospital data from OpenStreetMap via the Overpass API
and persist the raw GeoJSON to the bronze partition.

Design decisions
----------------
- Raw data is saved exactly as received (no transformation) so that the
  bronze layer is a faithful, replayable record of the source.
- The function is idempotent: if a bronze file already exists for today's
  partition, extraction is skipped and the cached file is used. This avoids
  unnecessary load on the public Overpass API and makes the pipeline safe
  to re-run without side effects.
- OSM elements (nodes, ways, relations) are normalised into a consistent
  GeoJSON FeatureCollection so that downstream consumers see a single format
  regardless of OSM element type.
"""

import json
import os
from datetime import datetime, timezone
from typing import Optional

import requests

from pipeline import config
from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def _build_feature(element: dict) -> Optional[dict]:
    """
    Convert a single Overpass API element into a GeoJSON Feature.

    Nodes carry explicit lat/lon. Ways and relations carry a 'center'
    object when queried with 'out center', giving a representative point.
    Elements without any usable coordinate are discarded here — they will
    be counted in the quality report.

    Parameters
    ----------
    element : dict
        Raw Overpass API element dict.

    Returns
    -------
    dict | None
        A GeoJSON Feature, or None if no coordinate could be extracted.
    """
    osm_type = element.get("type")

    if osm_type == "node":
        lat = element.get("lat")
        lon = element.get("lon")
    else:
        # ways and relations use the centroid returned by 'out center'
        center = element.get("center", {})
        lat = center.get("lat")
        lon = center.get("lon")

    if lat is None or lon is None:
        logger.debug(
            "Skipping %s/%s — no coordinate available",
            osm_type,
            element.get("id"),
        )
        return None

    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [lon, lat],  # GeoJSON is [lon, lat]
        },
        "properties": {
            "osm_id": element.get("id"),
            "osm_type": osm_type,
            **element.get("tags", {}),
        },
    }


def _elements_to_geojson(elements: list[dict]) -> dict:
    """
    Convert a list of Overpass API elements into a GeoJSON FeatureCollection.

    Parameters
    ----------
    elements : list[dict]
        Raw elements list from the Overpass JSON response.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection with pipeline metadata.
    """
    features = []
    skipped = 0

    for element in elements:
        feature = _build_feature(element)
        if feature is not None:
            features.append(feature)
        else:
            skipped += 1

    logger.info(
        "Converted %d elements to GeoJSON features (%d skipped — no coordinate)",
        len(features),
        skipped,
    )

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "source": "OpenStreetMap via Overpass API",
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_version": config.PIPELINE_VERSION,
            "feature_count": len(features),
            "skipped_count": skipped,
            "partition_date": config.PARTITION_DATE,
        },
    }


def extract(force: bool = False) -> str:
    """
    Extract UK hospital data from OpenStreetMap and save raw GeoJSON
    to the bronze zone.

    Parameters
    ----------
    force : bool
        If True, re-extract even if a bronze file already exists for
        today's partition. Useful for testing or manual refresh.

    Returns
    -------
    str
        Path to the bronze GeoJSON file.
    """
    bronze_path = config.BRONZE_PATH

    # ── Idempotency check ──────────────────────────────────────────────────
    if os.path.exists(bronze_path) and not force:
        logger.info(
            "Bronze file already exists for partition %s — skipping extraction. "
            "Pass force=True to re-extract.",
            config.PARTITION_DATE,
        )
        return bronze_path

    # ── Query Overpass API ─────────────────────────────────────────────────
    query = config.OVERPASS_QUERY.format(timeout=config.OVERPASS_TIMEOUT)
    logger.info("Querying Overpass API for UK hospitals (timeout=%ds)...", config.OVERPASS_TIMEOUT)

    try:
        response = requests.post(
            config.OVERPASS_URL,
            data={"data": query},
            timeout=config.OVERPASS_TIMEOUT + 10,  # HTTP timeout > query timeout
            headers={"User-Agent": f"hospital-pipeline/{config.PIPELINE_VERSION}"},
        )
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Overpass API request timed out after %ds", config.OVERPASS_TIMEOUT + 10)
        raise
    except requests.exceptions.HTTPError as exc:
        logger.error("Overpass API returned HTTP %s: %s", response.status_code, response.text[:200])
        raise RuntimeError(f"Overpass API error: HTTP {response.status_code}") from exc
    except requests.exceptions.RequestException as exc:
        logger.error("Network error querying Overpass API: %s", exc)
        raise

    data = response.json()
    elements = data.get("elements", [])
    logger.info("Received %d elements from Overpass API", len(elements))

    if not elements:
        raise ValueError(
            "Overpass API returned zero elements. "
            "The query may be malformed or the API may be rate-limiting."
        )

    # ── Convert to GeoJSON ────────────────────────────────────────────────
    geojson = _elements_to_geojson(elements)

    # ── Persist to bronze zone ────────────────────────────────────────────
    os.makedirs(os.path.dirname(bronze_path), exist_ok=True)

    with open(bronze_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    size_kb = os.path.getsize(bronze_path) / 1024
    logger.info(
        "Bronze file written: %s (%.1f KB, %d features)",
        bronze_path,
        size_kb,
        geojson["metadata"]["feature_count"],
    )

    return bronze_path
