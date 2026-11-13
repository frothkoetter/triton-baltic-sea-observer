"""Offline tests: WKT validity and SQL artifacts (no Trino required)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _bornholm_wkt() -> str:
    sql = (ROOT / "sql" / "01_schema_spatial_core.sql").read_text(encoding="utf-8")
    m = re.search(r"'Bornholm Basin',\s*'(POLYGON\(\([^']+\)\))'", sql, re.DOTALL)
    assert m, "Bornholm WKT not found in DDL"
    return m.group(1)


def test_wkt_polygon_closed_ring():
    wkt = _bornholm_wkt()
    assert wkt.startswith("POLYGON((")
    inner = wkt[len("POLYGON((") : -2]
    coords = [c.strip() for c in inner.split(",")]
    assert coords[0] == coords[-1], "WKT ring must close (first point equals last)"


def test_wkt_coords_in_baltic_bbox():
    wkt = _bornholm_wkt()
    pairs = re.findall(r"([0-9.]+)\s+([0-9.]+)", wkt)
    for lon_s, lat_s in pairs:
        lon, lat = float(lon_s), float(lat_s)
        assert 14 <= lon <= 21, f"lon {lon} outside Baltic rough bbox"
        assert 54 <= lat <= 60, f"lat {lat} outside Baltic rough bbox"


@pytest.mark.parametrize(
    "rel",
    [
        "sql/01_schema_spatial_core.sql",
        "sql/02_view_area_violation.sql",
        "sql/03_ostsee_infrastruktur.sql",
        "sql/04_geojson_staging.sql",
        "sql/05_ostsee_pipelines_segmented.sql",
        "sql/queries/03_ais_within_aoi.sql",
        "sql/queries/04_ais_near_harbour_km.sql",
        "sql/queries/05_pipeline_mehrfach_ueberquerungen.sql",
        "sql/queries/06_pipeline_korridor_langsamfahrt.sql",
        "sql/queries/07_korridor_segmented_performance.sql",
        "sql/queries/08_ais_blackout_pipelines.sql",
        "data/sample_ais_blackout.csv",
        "data/mock_ostsee_featurecollection.geojson",
        "data/sample_ais_positions.csv",
        "data/sample_ais_pipeline_track.csv",
        "docs/implementation-draft-de.md",
    ],
)
def test_project_artifacts_exist(rel: str):
    assert (ROOT / rel).is_file()


def test_view_uses_ogc_pipeline():
    view = (ROOT / "sql" / "02_view_area_violation.sql").read_text(encoding="utf-8")
    assert "ST_GeometryFromText" in view
    assert "ST_Within" in view
    assert "ST_Point(e.longitude, e.latitude)" in view


def test_sample_ais_has_bornholm_point():
    """At least one CSV row should fall inside Bornholm Basin for E2E."""
    text = (ROOT / "data" / "sample_ais_positions.csv").read_text(encoding="utf-8")
    # 55.98, 17.85 from first data row — inside Bornholm polygon in DDL
    assert "55.98" in text and "17.85" in text


def test_infrastructure_uses_linestring_wkt():
    ddl = (ROOT / "sql" / "03_ostsee_infrastruktur.sql").read_text(encoding="utf-8")
    assert "LINESTRING(" in ddl
    assert "geometrie_wkt" in ddl


def test_pipeline_queries_use_lead_and_intersects():
    q = (ROOT / "sql/queries/05_pipeline_mehrfach_ueberquerungen.sql").read_text(encoding="utf-8")
    assert "LEAD(" in q and "ST_Intersects" in q and "ST_LineFromText" in q


def test_draft_doc_mentions_wkt_standard():
    doc = (ROOT / "docs/implementation-draft-de.md").read_text(encoding="utf-8")
    assert "Well-Known Text" in doc or "WKT" in doc
    assert "ST_Intersects" in doc


def test_geojson_staging_has_raw_and_pipelines():
    sql = (ROOT / "sql" / "04_geojson_staging.sql").read_text(encoding="utf-8")
    assert "raw_ostsee_geojson" in sql
    assert "ostsee_pipelines" in sql
    assert "json_extract" in sql or "ST_GeomFromGeoJSON" in sql


def test_segmented_sql_has_bbox_columns():
    sql = (ROOT / "sql" / "05_ostsee_pipelines_segmented.sql").read_text(encoding="utf-8")
    assert "ostsee_pipelines_segmented" in sql
    assert "min_lon" in sql and "seg_wkt" in sql


def test_ais_daten_view_parses_multiple_timestamp_formats():
    sql = (ROOT / "sql" / "03a_refresh_ais_daten_view.sql").read_text(encoding="utf-8")
    assert "date_parse" in sql
    assert "from_iso8601_timestamp" in sql


def test_blackout_query_uses_gap_and_spherical():
    q = (ROOT / "sql/queries/08_ais_blackout_pipelines.sql").read_text(encoding="utf-8")
    assert "luecke_minuten" in q
    assert "to_spherical_geography" in q
    assert "ostsee_pipelines_segmented" in q
