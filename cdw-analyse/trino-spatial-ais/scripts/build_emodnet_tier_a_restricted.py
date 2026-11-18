#!/usr/bin/env python3
"""Curate Tier-A military restriction polygons (EMODnet HA) for Triton observation_areas.

Source: emodnet:militaryareaspoly WFS (Baltic bbox).
Tier A = tactically relevant subset (~24): DE/DK firing ranges, PL defence zones,
Gulf-of-Finland choke, Swedish Baltic exercise/firing near Gotland corridor.

Outputs:
  ../data/emodnet_tier_a_restricted_areas.json
  ../sql/06_tier_a_observation_areas.sql  (regenerated INSERT block)
"""

from __future__ import annotations

import json
import textwrap
import urllib.parse
import urllib.request
from pathlib import Path

from shapely.geometry import shape

ROOT = Path(__file__).resolve().parent.parent
OUT_JSON = ROOT / "data" / "emodnet_tier_a_restricted_areas.json"
OUT_SQL = ROOT / "sql" / "06_tier_a_observation_areas.sql"
BASE = "https://ows.emodnet-humanactivities.eu/wfs"
BBOX = "9.0,53.5,30.5,66.0,EPSG:4326"
TIER = "A"
SOURCE = "EMODnet emodnet:militaryareaspoly (Tier A curated)"


def fetch() -> list[dict]:
    q = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": "emodnet:militaryareaspoly",
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "bbox": BBOX,
        "count": 500,
    }
    url = BASE + "?" + urllib.parse.urlencode(q)
    with urllib.request.urlopen(url, timeout=120) as resp:
        return json.load(resp).get("features", [])


def largest_polygon(geom):
    if geom.geom_type == "Polygon":
        return geom
    if geom.geom_type == "MultiPolygon":
        return max(geom.geoms, key=lambda g: g.area)
    return None


def de_firing_label(lat: float, lon: float) -> str:
    if lat > 55.0 and lon > 14.5:
        return "DE-Firing-Bornholm-Bight"
    if lon < 11.5:
        return "DE-Firing-Putlos-Todendorf"
    if lon < 14.0:
        return "DE-Firing-Bay-Kiel"
    if lon < 15.0:
        return "DE-Firing-Ostsee-Central"
    return "DE-Firing-Schleswig"


def dk_firing_label(lat: float, lon: float) -> str:
    return "DK-Firing-Skagerrak" if lon < 10.0 else "DK-Firing-Kattegat"


def pl_label(lat: float, lon: float) -> str:
    if lon > 18.5:
        return "PL-Defence-Gdansk-Bight"
    if lon > 16.5:
        return "PL-Defence-Slupsk-Bank"
    if lon > 15.0:
        return "PL-Defence-Kolobrzeg"
    return "PL-Defence-Swinoujscie"


def gof_label(country: str, lat: float, lon: float) -> str:
    if country == "Estonia":
        return "EE-Defence-Gulf-Finland" if lon > 25.5 else "EE-Defence-West-Estonia"
    if lon < 24.0:
        return "FI-Defence-West-Gulf"
    if lon < 25.8:
        return "FI-Defence-Helsinki-Approaches"
    return "FI-Defence-Eastern-Gulf"


def se_baltic_label(lat: float, lon: float) -> str:
    if lat < 56.5:
        return "SE-Firing-Southern-Baltic"
    if lon < 17.5:
        return "SE-Firing-Gotland-West"
    return "SE-Firing-Gotland-East"


def pick_tier_a(features: list[dict]) -> list[dict]:
    selected: list[dict] = []
    seen_ids: set[str] = set()
    se_centroids: list[tuple[float, float]] = []

    def add(f: dict, area_id: str, country: str, area_type: str) -> None:
        if area_id in seen_ids:
            return
        poly = largest_polygon(shape(f["geometry"]))
        if poly is None:
            return
        poly = poly.simplify(0.008, preserve_topology=True)
        c = poly.centroid
        selected.append(
            {
                "area_id": area_id,
                "country": country,
                "area_type": area_type,
                "tier": TIER,
                "source": SOURCE,
                "polygon": poly.wkt,
                "center_latitude": round(c.y, 6),
                "center_longitude": round(c.x, 6),
            }
        )
        seen_ids.add(area_id)

    for f in features:
        p = f.get("properties") or {}
        country = p.get("country") or ""
        area_type = p.get("type_1") or ""
        c = shape(f["geometry"]).centroid
        lat, lon = c.y, c.x

        if country == "Germany" and area_type == "Firing Area":
            add(f, de_firing_label(lat, lon), country, area_type)
        elif country == "Denmark" and area_type == "Firing Area":
            add(f, dk_firing_label(lat, lon), country, area_type)
        elif country == "Poland" and area_type == "National Defence Area":
            add(f, pl_label(lat, lon), country, area_type)
        elif area_type == "Firing Area" and 55.0 <= lat <= 58.9 and 15.5 <= lon <= 21.0:
            too_close = any(abs(lat - a) < 0.06 and abs(lon - b) < 0.06 for a, b in se_centroids)
            if too_close or len(se_centroids) >= 5:
                continue
            se_centroids.append((lat, lon))
            add(f, se_baltic_label(lat, lon), "Sweden", area_type)
        elif area_type in ("National Defence Area", "Underwater Exercise") and 59.0 <= lat <= 60.45 and 23.0 <= lon <= 27.5:
            label = gof_label(country, lat, lon)
            if label in seen_ids:
                label = f"{label}-{round(lon, 1)}"
            add(f, label, country, area_type)

    # Cap Gulf of Finland to 6 most EEZ-central (avoid clutter)
    gof = [a for a in selected if "Gulf" in a["area_id"] or "Helsinki" in a["area_id"] or "Estonia" in a["area_id"] or a["area_id"].startswith("EE-") or a["area_id"].startswith("FI-")]
    if len(gof) > 6:
        gof.sort(key=lambda a: abs(a["center_longitude"] - 24.5) + abs(a["center_latitude"] - 59.85) * 0.5)
        keep = {a["area_id"] for a in gof[:6]}
        selected = [a for a in selected if a not in gof or a["area_id"] in keep]

    selected.sort(key=lambda a: (a["country"], a["area_id"]))
    return selected


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def write_sql(areas: list[dict]) -> None:
    values = []
    for a in areas:
        values.append(
            "('{aid}', '{poly}', {lat}, {lon}, '{country}', '{atype}', '{tier}', '{src}')".format(
                aid=sql_escape(a["area_id"]),
                poly=sql_escape(a["polygon"]),
                lat=a["center_latitude"],
                lon=a["center_longitude"],
                country=sql_escape(a["country"]),
                atype=sql_escape(a["area_type"]),
                tier=a["tier"],
                src=sql_escape(a["source"]),
            )
        )
    body = ",\n".join(values)
    sql = textwrap.dedent(
        f"""\
        -- Tier A military / restricted areas (EMODnet). Regenerate via scripts/build_emodnet_tier_a_restricted.py
        USE iceberg.defense;

        DROP TABLE IF EXISTS observation_areas;
        CREATE TABLE observation_areas (
          area_id varchar COMMENT 'Stable id for map + area_violation joins.',
          polygon varchar COMMENT 'OGC WKT POLYGON (lon lat).',
          center_latitude double,
          center_longitude double,
          country varchar COMMENT 'Reporting state from EMODnet.',
          area_type varchar COMMENT 'Firing Area, National Defence Area, …',
          tier varchar COMMENT 'A = tactically curated subset.',
          source varchar COMMENT 'Provenance.'
        );

        INSERT INTO observation_areas (
          area_id, polygon, center_latitude, center_longitude,
          country, area_type, tier, source
        ) VALUES
        {body};
        """
    )
    OUT_SQL.write_text(sql, encoding="utf-8")


def main() -> int:
    feats = fetch()
    areas = pick_tier_a(feats)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(areas, indent=2, ensure_ascii=False), encoding="utf-8")
    write_sql(areas)
    print(f"Wrote {len(areas)} Tier A areas to {OUT_JSON}")
    print(f"Wrote SQL to {OUT_SQL}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())