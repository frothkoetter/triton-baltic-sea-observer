import type { AisRow } from "../pages/Lagebild";
import type { FleetPosition } from "../components/TacticalMap";

export type LagebildFilters = {
  flag: string;
  vesselType: string;
  shipName: string;
};

export function matchesAisFilters(row: AisRow, filters: LagebildFilters): boolean {
  if (!Number.isFinite(row.latitude) || !Number.isFinite(row.longitude)) return false;
  if (filters.flag && row.flag !== filters.flag) return false;
  if (filters.vesselType && row.vessel_type !== filters.vesselType) return false;
  if (filters.shipName && row.ship_name !== filters.shipName) return false;
  return true;
}

export function filterAisRows(rows: AisRow[], filters: LagebildFilters): AisRow[] {
  return rows.filter((r) => matchesAisFilters(r, filters));
}

/** When a specific AIS ship is selected, narrow fleet/outlier overlays to that MMSI if known. */
export function filterFleetBySelection(
  fleet: FleetPosition[],
  filters: LagebildFilters,
  aisRows: AisRow[]
): FleetPosition[] {
  if (!filters.shipName) return fleet;
  const hit = aisRows.find((r) => r.ship_name === filters.shipName);
  if (hit) {
    return fleet.filter(
      (f) => f.unit_name === filters.shipName || String(f.mmsi) === String(hit.mmsi)
    );
  }
  return fleet.filter((f) => f.unit_name === filters.shipName);
}

/** Hide NATO / RU fleet markers when Filter Rail flag targets a third country. */
export function fleetLayersAllowed(filters: LagebildFilters): { german: boolean; russian: boolean } {
  if (!filters.flag) return { german: true, russian: true };
  if (filters.flag === "Germany") return { german: true, russian: false };
  if (filters.flag === "Russia") return { german: false, russian: true };
  return { german: false, russian: false };
}

export function filterOutliersByAis<T extends { mmsi: number }>(
  outliers: T[],
  filteredAis: AisRow[],
  filters: LagebildFilters
): T[] {
  if (!filters.flag && !filters.vesselType && !filters.shipName) return outliers;
  const mmsiSet = new Set(filteredAis.map((r) => r.mmsi));
  return outliers.filter((o) => mmsiSet.has(o.mmsi));
}