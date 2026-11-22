import type { AisRow } from "../pages/Lagebild";
import type { FleetPosition } from "../components/TacticalMap";

export type LagebildFilters = {
  flags: string[];
  vesselTypes: string[];
  shipNames: string[];
  germanUnits: string[];
  russianUnits: string[];
};

export const EMPTY_LAGEBILD_FILTERS: LagebildFilters = {
  flags: [],
  vesselTypes: [],
  shipNames: [],
  germanUnits: [],
  russianUnits: [],
};

export function filtersActive(filters: LagebildFilters): boolean {
  return (
    filters.flags.length > 0 ||
    filters.vesselTypes.length > 0 ||
    filters.shipNames.length > 0 ||
    filters.germanUnits.length > 0 ||
    filters.russianUnits.length > 0
  );
}

function inList(value: string | undefined, selected: string[]): boolean {
  if (selected.length === 0) return true;
  return selected.includes(String(value ?? ""));
}

export function matchesAisFilters(row: AisRow, filters: LagebildFilters): boolean {
  if (!Number.isFinite(row.latitude) || !Number.isFinite(row.longitude)) return false;
  if (!inList(row.flag, filters.flags)) return false;
  if (!inList(row.vessel_type, filters.vesselTypes)) return false;
  if (!inList(row.ship_name, filters.shipNames)) return false;
  return true;
}

export function filterAisRows(rows: AisRow[], filters: LagebildFilters): AisRow[] {
  return rows.filter((r) => matchesAisFilters(r, filters));
}

export function fleetUnitLabel(row: FleetPosition): string {
  return String(row.unit_name ?? row.mmsi ?? "").trim();
}

export function filterFleetUnits(fleet: FleetPosition[], units: string[]): FleetPosition[] {
  if (units.length === 0) return fleet;
  const set = new Set(units);
  return fleet.filter((f) => set.has(fleetUnitLabel(f)));
}

export function filterOutliersByAis<T extends { mmsi: number }>(
  outliers: T[],
  filteredAis: AisRow[],
  filters: LagebildFilters
): T[] {
  if (!filtersActive(filters)) return outliers;
  const mmsiSet = new Set(filteredAis.map((r) => r.mmsi));
  return outliers.filter((o) => mmsiSet.has(o.mmsi));
}

export function serializeFilterQueryParams(filters: LagebildFilters): URLSearchParams {
  const params = new URLSearchParams();
  if (filters.flags.length) params.set("flags", filters.flags.join(","));
  if (filters.vesselTypes.length) params.set("vessel_types", filters.vesselTypes.join(","));
  if (filters.shipNames.length) params.set("ship_names", filters.shipNames.join(","));
  if (filters.germanUnits.length) params.set("german_units", filters.germanUnits.join(","));
  if (filters.russianUnits.length) params.set("russian_units", filters.russianUnits.join(","));
  return params;
}