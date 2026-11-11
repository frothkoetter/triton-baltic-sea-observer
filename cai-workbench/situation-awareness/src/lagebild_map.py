"""Static Baltic Sea situation map (all harbours / events; cartopy or bundled coast)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .harbour_geo import canonical_harbour_name, coords_for_harbour, harbour_points
from .lagebild_viz_prep import normalize_lagebild_frame

_SEVERITY_RANK = {"low": 1, "medium": 2, "high": 3, "critical": 4}
_STATUS_PREFIX = re.compile(r"^\s*(NEW|ESCALATED)\s*\|", re.I)

# Full Baltic basin (German coast through Gulf of Finland / St. Petersburg)
_MAP_EXTENT = (9.0, 32.0, 53.2, 66.2)  # lon_min, lon_max, lat_min, lat_max
_SEA_COLOR = "#e8f0f8"
_LAND_COLOR = "#d4dcd4"
_COASTLINE_COLOR = "#6a7a6a"
_BORDER_COLOR = "#9aa89a"
_DATA_DIR = Path(__file__).resolve().parents[1] / "data"

_last_basemap_backend: str | None = None
_last_basemap_scale: str | None = None

# Harbour-scale extent needs 10m coast; 110m looks like the bundled fallback polygons.
_CARTOPY_PHYSICAL_SCALE = "10m"


def basemap_backend() -> str | None:
    """Backend used by the last ``render_lagebild_map`` call (``cartopy`` or ``fallback``)."""
    return _last_basemap_backend


def basemap_detail() -> str | None:
    """Resolution label for the last map (e.g. ``cartopy:10m``, ``fallback:bundled``)."""
    if _last_basemap_backend is None:
        return None
    if _last_basemap_backend == "cartopy" and _last_basemap_scale:
        return f"cartopy:{_last_basemap_scale}"
    return "fallback:bundled"


def _max_severity(values: pd.Series) -> str:
    best = "low"
    best_rank = 0
    for v in values.dropna().astype(str):
        key = v.strip().lower()
        rank = _SEVERITY_RANK.get(key, 1)
        if rank > best_rank:
            best_rank = rank
            best = key if key in _SEVERITY_RANK else "medium"
    return best


def _coords_from_chunk(
    name: str, chunk: pd.DataFrame
) -> tuple[float, float] | None:
    coords = coords_for_harbour(name)
    if coords:
        return coords
    if chunk.empty:
        return None
    if "latitude" not in chunk.columns or "longitude" not in chunk.columns:
        return None
    pts = chunk.dropna(subset=["latitude", "longitude"])
    if pts.empty:
        return None
    lat = float(pts["latitude"].astype(float).mean())
    lon = float(pts["longitude"].astype(float).mean())
    if not (_MAP_EXTENT[2] - 2 <= lat <= _MAP_EXTENT[3] + 2):
        return None
    if not (_MAP_EXTENT[0] - 2 <= lon <= _MAP_EXTENT[1] + 2):
        return None
    return lat, lon


def _harbour_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "harbour_name" not in df.columns:
        return pd.DataFrame(
            columns=[
                "harbour_name",
                "count",
                "severity",
                "latitude",
                "longitude",
            ]
        )

    grouped = df.groupby("harbour_name", dropna=True)
    rows: list[dict[str, object]] = []
    for harbour, chunk in grouped:
        raw = str(harbour).strip()
        if not raw:
            continue
        name = canonical_harbour_name(raw) or raw
        if _coords_from_chunk(name, chunk) is None:
            continue
        severity = (
            _max_severity(chunk["severity"]) if "severity" in chunk.columns else "medium"
        )
        lat, lon = _coords_from_chunk(name, chunk)
        assert lat is not None and lon is not None
        rows.append(
            {
                "harbour_name": name,
                "count": len(chunk),
                "severity": severity,
                "latitude": lat,
                "longitude": lon,
            }
        )
    return pd.DataFrame(rows)


def _changed_harbours(
    comparison_text: str | None,
    dataset_summary: str,
    counts: pd.DataFrame,
) -> set[str]:
    highlighted: set[str] = set()
    if comparison_text:
        for line in comparison_text.splitlines():
            if not _STATUS_PREFIX.match(line):
                continue
            upper = line.upper()
            for harbour in counts["harbour_name"].tolist():
                if harbour.upper() in upper:
                    highlighted.add(harbour)

    if highlighted:
        return highlighted

    m = re.search(r"busiest harbours\s*[—–-]\s*([^;(]+)", dataset_summary, re.I)
    if m:
        token = m.group(1).strip().split("(")[0].strip()
        if coords_for_harbour(token):
            return {token}

    if not counts.empty:
        top = counts.sort_values("count", ascending=False).iloc[0]
        return {str(top["harbour_name"])}

    return set()


def _severity_color(severity: str) -> tuple[float, float, float, float]:
    key = (severity or "medium").lower()
    palette = {
        "low": (0.35, 0.65, 0.45, 0.85),
        "medium": (0.95, 0.55, 0.15, 0.9),
        "high": (0.85, 0.15, 0.12, 0.95),
        "critical": (0.55, 0.05, 0.08, 0.95),
    }
    return palette.get(key, palette["medium"])


def _plot_geojson_features(ax, geojson_path: Path, *, land: bool) -> None:
    if not geojson_path.is_file():
        return
    with geojson_path.open(encoding="utf-8") as f:
        collection = json.load(f)

    def _iter_poly_coords(geom: dict) -> list:
        gtype = geom.get("type")
        coords = geom.get("coordinates", [])
        if gtype == "Polygon":
            return [coords]
        if gtype == "MultiPolygon":
            return coords
        return []

    def _iter_line_coords(geom: dict) -> list:
        gtype = geom.get("type")
        coords = geom.get("coordinates", [])
        if gtype == "LineString":
            return [coords]
        if gtype == "MultiLineString":
            return coords
        if gtype == "Polygon":
            return [ring for ring in coords]
        if gtype == "MultiPolygon":
            rings = []
            for poly in coords:
                rings.extend(poly)
            return rings
        return []

    for feature in collection.get("features", []):
        geom = feature.get("geometry") or {}
        if land:
            for poly in _iter_poly_coords(geom):
                if not poly:
                    continue
                ring = poly[0]
                lons = [pt[0] for pt in ring]
                lats = [pt[1] for pt in ring]
                ax.fill(lons, lats, facecolor=_LAND_COLOR, edgecolor="none", zorder=1)
        else:
            for line in _iter_line_coords(geom):
                if len(line) < 2:
                    continue
                lons = [pt[0] for pt in line]
                lats = [pt[1] for pt in line]
                ax.plot(
                    lons,
                    lats,
                    color=_COASTLINE_COLOR,
                    linewidth=0.9,
                    solid_capstyle="round",
                    zorder=2,
                )


def _add_cartopy_physical(ax, scale: str) -> None:
    import cartopy.feature as cfeature

    ax.add_feature(
        cfeature.OCEAN.with_scale(scale),
        facecolor=_SEA_COLOR,
        edgecolor="none",
        zorder=0,
    )
    ax.add_feature(
        cfeature.LAND.with_scale(scale),
        facecolor=_LAND_COLOR,
        edgecolor="none",
        zorder=1,
    )
    ax.add_feature(
        cfeature.COASTLINE.with_scale(scale),
        edgecolor=_COASTLINE_COLOR,
        linewidth=0.8,
        zorder=2,
    )
    ax.add_feature(
        cfeature.BORDERS.with_scale(scale),
        edgecolor=_BORDER_COLOR,
        linewidth=0.35,
        linestyle="-",
        zorder=2,
    )


def _setup_cartopy_axes(fig, *, scale: str = _CARTOPY_PHYSICAL_SCALE):
    import cartopy.crs as ccrs

    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    ax.set_extent(_MAP_EXTENT, crs=ccrs.PlateCarree())
    _add_cartopy_physical(ax, scale)
    gl = ax.gridlines(draw_labels=False, linewidth=0.35, color="#a8b4c0", alpha=0.55, linestyle=":")
    gl.xlocator = plt.FixedLocator(range(10, 33, 4))
    gl.ylocator = plt.FixedLocator([54, 58, 62, 66])
    return ax


def _setup_fallback_axes(fig):
    ax = fig.add_subplot(1, 1, 1)
    lon_min, lon_max, lat_min, lat_max = _MAP_EXTENT
    ax.set_xlim(lon_min, lon_max)
    ax.set_ylim(lat_min, lat_max)
    ax.set_facecolor(_SEA_COLOR)
    _plot_geojson_features(ax, _DATA_DIR / "baltic_land_simplified.geojson", land=True)
    _plot_geojson_features(ax, _DATA_DIR / "baltic_coast_simplified.geojson", land=False)
    ax.set_aspect("equal", adjustable="box")
    ax.grid(True, linestyle=":", alpha=0.35, color="#a8b4c0")
    ax.set_xlabel("Longitude °E")
    ax.set_ylabel("Latitude °N")
    return ax


def _setup_map_axes(fig) -> tuple[object, str, str]:
    scales = ("50m", "110m", _CARTOPY_PHYSICAL_SCALE)
    try:
        import cartopy  # noqa: F401
    except ImportError:
        return _setup_fallback_axes(fig), "fallback", "bundled"

    for idx, scale in enumerate(scales):
        try:
            if idx > 0:
                fig.clf()
            ax = _setup_cartopy_axes(fig, scale=scale)
            return ax, "cartopy", scale
        except Exception:  # pragma: no cover - network / cache failures on CML
            fig.clf()
            continue

    return _setup_fallback_axes(fig), "fallback", "bundled"


def _overlay_gps_jammers(ax, df: pd.DataFrame, *, transform) -> None:
    """Plot GPS jammer events with lat/lon when present."""
    if df.empty or "data_source" not in df.columns:
        return
    jam = df[df["data_source"] == "GPS Jammer"]
    if jam.empty:
        return
    if "latitude" not in jam.columns or "longitude" not in jam.columns:
        return
    pts = jam.dropna(subset=["latitude", "longitude"])
    if pts.empty:
        return
    kw = {"transform": transform} if transform is not None else {}
    ax.scatter(
        pts["longitude"].astype(float),
        pts["latitude"].astype(float),
        marker="x",
        s=42,
        c="#b71c1c",
        linewidths=1.2,
        zorder=6,
        label="GPS Jammer",
        **kw,
    )


def _overlay_harbours(
    ax,
    counts: pd.DataFrame,
    changed: set[str],
    *,
    transform=None,
) -> None:
    scatter_kw = {"transform": transform} if transform is not None else {}

    if counts.empty:
        for hp in harbour_points():
            ax.scatter(
                hp.longitude,
                hp.latitude,
                s=18,
                c="#888888",
                alpha=0.5,
                zorder=3,
                **scatter_kw,
            )
            ax.text(
                hp.longitude + 0.03,
                hp.latitude + 0.02,
                hp.name,
                fontsize=7,
                zorder=5,
                **scatter_kw,
            )
        return

    max_count = max(int(c) for c in counts["count"]) or 1
    for _, row in counts.iterrows():
        name = str(row["harbour_name"])
        if "latitude" in row and "longitude" in row:
            lat = float(row["latitude"])
            lon = float(row["longitude"])
        else:
            coords = coords_for_harbour(name)
            if not coords:
                continue
            lat, lon = coords
        n = int(row["count"])
        size = 80 + 220 * (n / max_count)
        color = _severity_color(str(row.get("severity", "medium")))
        ax.scatter(
            lon, lat, s=size, c=[color], edgecolors="none", zorder=4, **scatter_kw
        )
        if name in changed:
            ax.scatter(
                lon,
                lat,
                s=size * 1.55,
                facecolors="none",
                edgecolors="#c9a227",
                linewidths=2.4,
                zorder=5,
                **scatter_kw,
            )
            ax.scatter(
                lon,
                lat,
                s=size * 1.72,
                facecolors="none",
                edgecolors="#b22222",
                linewidths=1.0,
                linestyle="--",
                zorder=5,
                **scatter_kw,
            )
        ax.text(
            lon + 0.12,
            lat + 0.06,
            f"{name} ({n})",
            fontsize=6.0,
            zorder=6,
            **scatter_kw,
        )

    plotted = set(counts["harbour_name"].astype(str))
    for hp in harbour_points():
        if hp.name in plotted:
            continue
        ax.scatter(
            hp.longitude,
            hp.latitude,
            s=10,
            c="#aaaaaa",
            alpha=0.25,
            zorder=3,
            **scatter_kw,
        )


def _overlay_offshore_events(ax, df: pd.DataFrame, *, transform) -> None:
    """Events without a mapped harbour name but with coordinates."""
    if df.empty or "harbour_name" not in df.columns:
        return
    mask = df["harbour_name"].isna() | (
        df["harbour_name"].astype(str).str.strip() == ""
    )
    if "latitude" not in df.columns or "longitude" not in df.columns:
        return
    off = df[mask].dropna(subset=["latitude", "longitude"])
    if off.empty:
        return
    kw = {"transform": transform} if transform is not None else {}
    ax.scatter(
        off["longitude"].astype(float),
        off["latitude"].astype(float),
        s=28,
        c="#5c6bc0",
        alpha=0.75,
        edgecolors="white",
        linewidths=0.4,
        zorder=5,
        label="Offshore / area",
        **kw,
    )


def render_lagebild_map(
    df: pd.DataFrame,
    out_path: Path,
    *,
    comparison_text: str | None = None,
    dataset_summary: str = "",
) -> Path:
    """Write PNG map; circle size = event count, ring = NEW/ESCALATED harbour."""
    global _last_basemap_backend, _last_basemap_scale

    out_path.parent.mkdir(parents=True, exist_ok=True)
    work = normalize_lagebild_frame(df) if not df.empty else df
    counts = _harbour_counts(work)
    changed = _changed_harbours(comparison_text, dataset_summary, counts)

    fig = plt.figure(figsize=(8.4, 5.8), dpi=120)
    ax, backend, scale = _setup_map_axes(fig)
    _last_basemap_backend = backend
    _last_basemap_scale = scale if backend == "cartopy" else None

    if backend == "cartopy":
        import cartopy.crs as ccrs

        transform = ccrs.PlateCarree()
        ax.set_title(
            "Baltic Sea — all harbours & events (Lagebild)",
            fontsize=11,
            pad=10,
        )
    else:
        transform = None
        ax.set_title(
            "Baltic Sea — all harbours & events (Lagebild)",
            fontsize=11,
            pad=10,
        )

    _overlay_harbours(ax, counts, changed, transform=transform)
    _overlay_offshore_events(ax, work, transform=transform)
    _overlay_gps_jammers(ax, work, transform=transform)

    if changed:
        lon_min, _, lat_min, _ = _MAP_EXTENT
        legend_kw = {"transform": transform} if transform is not None else {}
        ax.text(
            lon_min + 0.05,
            lat_min + 0.08,
            "Gold/red ring: NEW or ESCALATED harbour",
            fontsize=7,
            style="italic",
            zorder=7,
            **legend_kw,
        )

    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path
