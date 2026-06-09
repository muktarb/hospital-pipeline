"""
Data quality checks for the silver zone.

Design decisions
----------------
- Quality issues are flagged rather than silently dropped wherever possible.
  This preserves data that may still be useful while making issues visible.
- Each check appends a short code to a 'quality_flags' column so that
  downstream consumers can filter by quality level and analysts can audit
  what was flagged and why.
- Only genuinely unusable records (missing geometry, duplicate OSM ID) are
  removed entirely.

Quality flag codes
------------------
MISSING_NAME      : 'name' tag absent — hospital may be unnamed in OSM
OUT_OF_BOUNDS     : coordinates fall outside the UK bounding box
"""

from pipeline import config
from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def run_quality_checks(gdf):
    """
    Apply data quality checks to the silver GeoDataFrame.

    Checks applied (in order):
    1. Remove records with null geometry — cannot be stored as GeoParquet.
    2. Remove duplicate OSM IDs — true duplicates with no additional information.
    3. Flag records with missing hospital name.
    4. Flag records with coordinates outside the UK bounding box.

    Parameters
    ----------
    gdf : GeoDataFrame
        Input GeoDataFrame from the transform step.

    Returns
    -------
    GeoDataFrame
        Cleaned GeoDataFrame with a populated 'quality_flags' column.
    """
    initial_count = len(gdf)
    gdf = gdf.copy()
    gdf["quality_flags"] = ""

    # ── 1. Drop null geometry ──────────────────────────────────────────────
    null_geom_mask = gdf.geometry.isna() | gdf.geometry.is_empty
    null_geom_count = null_geom_mask.sum()
    if null_geom_count:
        logger.warning("Dropping %d records with null/empty geometry", null_geom_count)
        gdf = gdf[~null_geom_mask].copy()

    # ── 2. Drop duplicate OSM IDs ─────────────────────────────────────────
    dupe_mask = gdf["osm_id"].duplicated(keep="first")
    dupe_count = dupe_mask.sum()
    if dupe_count:
        logger.warning("Dropping %d duplicate OSM IDs", dupe_count)
        gdf = gdf[~dupe_mask].copy()

    # ── 3. Flag missing name ───────────────────────────────────────────────
    missing_name_mask = gdf["name"].isna() | (gdf["name"].str.strip() == "")
    missing_name_count = missing_name_mask.sum()
    if missing_name_count:
        logger.info(
            "Flagging %d records with missing name (MISSING_NAME)", missing_name_count
        )
        gdf.loc[missing_name_mask, "quality_flags"] = (
            gdf.loc[missing_name_mask, "quality_flags"]
            .apply(lambda f: (f + "|" if f else "") + "MISSING_NAME")
        )

    # ── 4. Flag out-of-bounds coordinates ────────────────────────────────
    b = config.UK_BOUNDS
    out_of_bounds_mask = (
        (gdf["longitude"] < b["min_lon"])
        | (gdf["longitude"] > b["max_lon"])
        | (gdf["latitude"] < b["min_lat"])
        | (gdf["latitude"] > b["max_lat"])
    )
    out_of_bounds_count = out_of_bounds_mask.sum()
    if out_of_bounds_count:
        logger.warning(
            "Flagging %d records outside UK bounding box (OUT_OF_BOUNDS)",
            out_of_bounds_count,
        )
        gdf.loc[out_of_bounds_mask, "quality_flags"] = (
            gdf.loc[out_of_bounds_mask, "quality_flags"]
            .apply(lambda f: (f + "|" if f else "") + "OUT_OF_BOUNDS")
        )

    # ── Summary ──────────────────────────────────────────────────────────
    final_count = len(gdf)
    flagged_count = (gdf["quality_flags"] != "").sum()

    logger.info(
        "Quality checks complete | initial=%d | removed=%d | flagged=%d | clean=%d",
        initial_count,
        initial_count - final_count,
        flagged_count,
        final_count - flagged_count,
    )

    return gdf
