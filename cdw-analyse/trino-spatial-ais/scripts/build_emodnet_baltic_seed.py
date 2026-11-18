#!/usr/bin/env python3
"""Fetch Baltic subsea routes from EMODnet HA WFS and write seed JSON + optional SQL snippet.

Sources (public):
  - https://ows.emodnet-humanactivities.eu/wfs  emodnet:pipelines
  - emodnet:pcablesbshcontis, emodnet:pcablesnve

Output: ../data/emodnet_baltic_seed_assets.json
Regenerate after EMODnet updates; commit JSON and refresh create_db_tables_trino.sql INSERT block.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path

from shapely.geometry import LineString, shape
from shapely.ops import linemerge

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "emodnet_baltic_seed_assets.json"
BASE = "https://ows.emodnet-humanactivities.eu/wfs"
BBOX = "9.0,53.5,30.5,66.0,EPSG:4326"


def fetch(layer: str) -> dict:
    q = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": layer,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "bbox": BBOX,
    }
    url = BASE + "?" + urllib.parse.urlencode(q)
    with urllib.request.urlopen(url, timeout=120) as resp:
        return json.load(resp)


def to_linestring(geom):
    if geom.geom_type == "LineString":
        return geom
    if geom.geom_type == "MultiLineString":
        merged = linemerge(geom)
        if merged.geom_type == "LineString":
            return merged
        parts = list(merged.geoms) if merged.geom_type == "MultiLineString" else [merged]
        return max(parts, key=lambda g: g.length)
    return None


def simplify_wkt(geom, tol: float = 0.01, max_pts: int = 100) -> str | None:
    ls = to_linestring(geom)
    if ls is None:
        return None
    g = ls.simplify(tol, preserve_topology=True)
    coords = list(g.coords)
    if len(coords) > max_pts:
        step = max(1, len(coords) // max_pts)
        coords = coords[::step]
    if coords[0] != ls.coords[0]:
        coords = [ls.coords[0]] + coords
    if coords[-1] != ls.coords[-1]:
        coords = coords + [ls.coords[-1]]
    return LineString(coords).wkt.replace("LINESTRING (", "LINESTRING(")


def find_name(features, *needles: str):
    for f in features:
        p = f.get("properties") or {}
        name = (p.get("name") or "").strip()
        if all(n in name.lower() for n in needles):
            return name, shape(f["geometry"])
    return None, None


def main() -> int:
    pipes = fetch("emodnet:pipelines")
    pc_bsh = fetch("emodnet:pcablesbshcontis")
    pc_nve = fetch("emodnet:pcablesnve")

    assets: list[dict] = []
    for needles, eid, typ, medium in [
        (("nord stream", "1"), "EMODNET_NS1", "Gaspipeline", "Gas"),
        (("balticconnector",), "EMODNET_BALTICCONNECTOR", "Gaspipeline", "Gas"),
        (("nord stream", "2"), "EMODNET_NS2", "Gaspipeline", "Gas"),
    ]:
        name, geom = find_name(pipes.get("features", []), *needles)
        if not geom:
            continue
        wkt = simplify_wkt(geom, tol=0.012)
        if not wkt:
            continue
        assets.append(
            {
                "id": eid,
                "name": name,
                "typ": typ,
                "medium": medium,
                "status": "In use",
                "geometrie_wkt": wkt,
                "source": "EMODnet emodnet:pipelines WFS",
            }
        )

    cables: list[tuple] = []
    for f in pc_bsh.get("features", []):
        g = shape(f["geometry"])
        if g.length * 111 < 70:
            continue
        c = g.centroid
        if 54.15 < c.y < 54.7 and 11.5 < c.x < 15.0:
            cables.append((g.length, g))
    cables.sort(reverse=True)
    seen: set[str] = set()
    idx = 0
    for _, g in cables:
        wkt = simplify_wkt(g, tol=0.005, max_pts=80)
        if not wkt or wkt in seen:
            continue
        seen.add(wkt)
        idx += 1
        if idx > 4:
            break
        assets.append(
            {
                "id": f"EMODNET_PCABLE_BSH_{idx}",
                "name": f"Offshore power cable (BSH/CONTIS) segment {idx}",
                "typ": "Seekabel",
                "medium": "Power",
                "status": "In use",
                "geometrie_wkt": wkt,
                "source": "EMODnet emodnet:pcablesbshcontis WFS",
            }
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(assets, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {len(assets)} assets to {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())