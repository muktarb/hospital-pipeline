"""Tests for data quality checks."""

import geopandas as gpd
from shapely.geometry import Point

from pipeline.quality import run_quality_checks


def make_gdf(rows):
    """Helper: build a minimal GeoDataFrame for quality check testing."""
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


class TestQualityChecks:
    def test_clean_record_has_no_flags(self):
        gdf = make_gdf([{
            "osm_id": 1, "name": "Good Hospital",
            "latitude": 51.5, "longitude": -0.1,
            "geometry": Point(-0.1, 51.5),
        }])
        result = run_quality_checks(gdf)
        assert result["quality_flags"].iloc[0] == ""

    def test_null_geometry_removed(self):
        gdf = make_gdf([
            {"osm_id": 1, "name": "Valid", "latitude": 51.5, "longitude": -0.1, "geometry": Point(-0.1, 51.5)},
            {"osm_id": 2, "name": "No Geom", "latitude": None, "longitude": None, "geometry": None},
        ])
        result = run_quality_checks(gdf)
        assert len(result) == 1
        assert result["osm_id"].iloc[0] == 1

    def test_duplicate_osm_id_removed(self):
        gdf = make_gdf([
            {"osm_id": 1, "name": "Hospital A", "latitude": 51.5, "longitude": -0.1, "geometry": Point(-0.1, 51.5)},
            {"osm_id": 1, "name": "Hospital A duplicate", "latitude": 51.5, "longitude": -0.1, "geometry": Point(-0.1, 51.5)},
        ])
        result = run_quality_checks(gdf)
        assert len(result) == 1

    def test_missing_name_flagged(self):
        gdf = make_gdf([{
            "osm_id": 1, "name": None,
            "latitude": 51.5, "longitude": -0.1,
            "geometry": Point(-0.1, 51.5),
        }])
        result = run_quality_checks(gdf)
        assert "MISSING_NAME" in result["quality_flags"].iloc[0]

    def test_out_of_bounds_flagged(self):
        gdf = make_gdf([{
            "osm_id": 1, "name": "Foreign Hospital",
            "latitude": 48.8, "longitude": 2.3,  # Paris — outside UK bounds
            "geometry": Point(2.3, 48.8),
        }])
        result = run_quality_checks(gdf)
        assert "OUT_OF_BOUNDS" in result["quality_flags"].iloc[0]

    def test_multiple_flags_combined(self):
        gdf = make_gdf([{
            "osm_id": 1, "name": None,
            "latitude": 48.8, "longitude": 2.3,
            "geometry": Point(2.3, 48.8),
        }])
        result = run_quality_checks(gdf)
        flags = result["quality_flags"].iloc[0]
        assert "MISSING_NAME" in flags
        assert "OUT_OF_BOUNDS" in flags
