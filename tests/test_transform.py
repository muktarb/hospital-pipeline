"""Tests for the silver transformation stage."""

import json

import geopandas as gpd
import pandas as pd
import pytest

from pipeline import config
from pipeline.transform import _apply_types, _features_to_geodataframe, _load_bronze


SAMPLE_FEATURES = [
    {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-0.1276, 51.5074]},
        "properties": {
            "osm_id": 1001,
            "osm_type": "node",
            "name": "St Thomas' Hospital",
            "beds": "900",
            "emergency": "yes",
            "addr:city": "London",
            "addr:postcode": "SE1 7EH",
        },
    },
    {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-1.8904, 52.4862]},
        "properties": {
            "osm_id": 1002,
            "osm_type": "way",
            "name": "Queen Elizabeth Hospital Birmingham",
            "beds": "1200",
            "addr:city": "Birmingham",
        },
    },
]


class TestLoadBronze:
    def test_loads_valid_geojson(self, tmp_path):
        path = tmp_path / "hospitals_raw.geojson"
        data = {"type": "FeatureCollection", "features": SAMPLE_FEATURES}
        path.write_text(json.dumps(data))
        features = _load_bronze(str(path))
        assert len(features) == 2

    def test_raises_on_wrong_type(self, tmp_path):
        path = tmp_path / "bad.geojson"
        path.write_text(json.dumps({"type": "Feature"}))
        with pytest.raises(ValueError, match="FeatureCollection"):
            _load_bronze(str(path))


class TestFeaturesToGeoDataFrame:
    def test_basic_conversion(self):
        gdf = _features_to_geodataframe(SAMPLE_FEATURES)
        assert isinstance(gdf, gpd.GeoDataFrame)
        assert len(gdf) == 2
        assert gdf.crs.to_epsg() == 4326

    def test_coordinates_extracted(self):
        gdf = _features_to_geodataframe(SAMPLE_FEATURES)
        assert gdf["longitude"].iloc[0] == pytest.approx(-0.1276)
        assert gdf["latitude"].iloc[0] == pytest.approx(51.5074)

    def test_osm_tags_mapped(self):
        gdf = _features_to_geodataframe(SAMPLE_FEATURES)
        assert gdf["name"].iloc[0] == "St Thomas' Hospital"
        assert gdf["addr_city"].iloc[1] == "Birmingham"

    def test_pipeline_metadata_added(self):
        gdf = _features_to_geodataframe(SAMPLE_FEATURES)
        assert "ingested_at" in gdf.columns
        assert "pipeline_version" in gdf.columns
        assert gdf["pipeline_version"].iloc[0] == config.PIPELINE_VERSION

    def test_null_geometry_handled(self):
        features = [
            {
                "type": "Feature",
                "geometry": None,
                "properties": {"osm_id": 999, "osm_type": "node"},
            }
        ]
        gdf = _features_to_geodataframe(features)
        assert gdf["geometry"].iloc[0] is None or gdf["geometry"].isna().iloc[0]


class TestApplyTypes:
    def test_beds_converted_to_integer(self):
        gdf = _features_to_geodataframe(SAMPLE_FEATURES)
        gdf = _apply_types(gdf)
        assert gdf["beds"].dtype == pd.Int64Dtype()
        assert gdf["beds"].iloc[0] == 900

    def test_non_numeric_beds_become_na(self):
        features = [{
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 51]},
            "properties": {"osm_id": 1, "osm_type": "node", "beds": "unknown"},
        }]
        gdf = _features_to_geodataframe(features)
        gdf = _apply_types(gdf)
        assert pd.isna(gdf["beds"].iloc[0])

    def test_string_columns_use_string_dtype(self):
        gdf = _features_to_geodataframe(SAMPLE_FEATURES)
        gdf = _apply_types(gdf)
        assert gdf["name"].dtype == pd.StringDtype()
