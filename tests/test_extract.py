"""
Tests for the bronze extraction stage.

Uses unittest.mock to avoid making real HTTP requests during testing.
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from pipeline import config
from pipeline.extract import _build_feature, _elements_to_geojson, extract


# ── Unit tests: _build_feature ────────────────────────────────────────────────

class TestBuildFeature:
    def test_node_returns_feature(self):
        element = {"type": "node", "id": 123, "lat": 51.5, "lon": -0.1, "tags": {"name": "St Thomas'"}}
        feature = _build_feature(element)
        assert feature is not None
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] == "Point"
        assert feature["geometry"]["coordinates"] == [-0.1, 51.5]
        assert feature["properties"]["osm_id"] == 123
        assert feature["properties"]["name"] == "St Thomas'"

    def test_way_uses_center(self):
        element = {
            "type": "way",
            "id": 456,
            "center": {"lat": 52.0, "lon": -1.5},
            "tags": {"name": "Walsgrave Hospital"},
        }
        feature = _build_feature(element)
        assert feature is not None
        assert feature["geometry"]["coordinates"] == [-1.5, 52.0]

    def test_missing_coordinates_returns_none(self):
        element = {"type": "way", "id": 789, "tags": {}}
        feature = _build_feature(element)
        assert feature is None

    def test_node_missing_lat_returns_none(self):
        element = {"type": "node", "id": 101, "lon": -0.1, "tags": {}}
        feature = _build_feature(element)
        assert feature is None

    def test_tags_included_in_properties(self):
        element = {
            "type": "node", "id": 1,
            "lat": 51.0, "lon": -0.5,
            "tags": {"name": "Hospital A", "beds": "200", "emergency": "yes"},
        }
        feature = _build_feature(element)
        assert feature["properties"]["beds"] == "200"
        assert feature["properties"]["emergency"] == "yes"


# ── Unit tests: _elements_to_geojson ─────────────────────────────────────────

class TestElementsToGeoJson:
    def test_valid_elements_produce_feature_collection(self):
        elements = [
            {"type": "node", "id": 1, "lat": 51.5, "lon": -0.1, "tags": {"name": "Hospital A"}},
            {"type": "node", "id": 2, "lat": 52.0, "lon": -1.0, "tags": {"name": "Hospital B"}},
        ]
        result = _elements_to_geojson(elements)
        assert result["type"] == "FeatureCollection"
        assert len(result["features"]) == 2
        assert result["metadata"]["feature_count"] == 2
        assert result["metadata"]["skipped_count"] == 0

    def test_elements_without_coords_are_skipped(self):
        elements = [
            {"type": "node", "id": 1, "lat": 51.5, "lon": -0.1, "tags": {}},
            {"type": "way", "id": 2, "tags": {}},  # no center
        ]
        result = _elements_to_geojson(elements)
        assert len(result["features"]) == 1
        assert result["metadata"]["skipped_count"] == 1

    def test_empty_elements_returns_empty_collection(self):
        result = _elements_to_geojson([])
        assert result["type"] == "FeatureCollection"
        assert result["features"] == []
        assert result["metadata"]["feature_count"] == 0


# ── Integration test: extract() with mocked HTTP ──────────────────────────────

class TestExtract:
    def test_extract_writes_bronze_file(self, tmp_path, monkeypatch):
        """extract() should write a valid GeoJSON file to the bronze path."""
        monkeypatch.setattr(config, "BRONZE_PATH", str(tmp_path / "hospitals_raw.geojson"))
        monkeypatch.setattr(config, "PARTITION_DATE", "2026-06-08")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "elements": [
                {"type": "node", "id": 1, "lat": 51.5, "lon": -0.1, "tags": {"name": "Test Hospital"}},
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("pipeline.extract.requests.post", return_value=mock_response):
            result_path = extract(force=True)

        assert os.path.exists(result_path)
        with open(result_path) as f:
            data = json.load(f)
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 1

    def test_extract_idempotent_skips_existing(self, tmp_path, monkeypatch):
        """extract() should skip the API call if a bronze file already exists."""
        bronze_path = str(tmp_path / "hospitals_raw.geojson")
        monkeypatch.setattr(config, "BRONZE_PATH", bronze_path)

        # Pre-create the bronze file
        with open(bronze_path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": []}, f)

        with patch("pipeline.extract.requests.post") as mock_post:
            result = extract(force=False)
            mock_post.assert_not_called()

        assert result == bronze_path

    def test_extract_raises_on_empty_elements(self, tmp_path, monkeypatch):
        """extract() should raise ValueError when API returns zero elements."""
        monkeypatch.setattr(config, "BRONZE_PATH", str(tmp_path / "hospitals_raw.geojson"))

        mock_response = MagicMock()
        mock_response.json.return_value = {"elements": []}
        mock_response.raise_for_status = MagicMock()

        with patch("pipeline.extract.requests.post", return_value=mock_response):
            with pytest.raises(ValueError, match="zero elements"):
                extract(force=True)
