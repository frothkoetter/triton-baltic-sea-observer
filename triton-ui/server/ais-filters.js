/** Parse comma-separated or repeated query params into unique trimmed strings. */
export function parseMultiQuery(...parts) {
  const raw = parts
    .flat()
    .filter((p) => p != null && p !== "")
    .map(String)
    .join(",");
  return [...new Set(raw.split(",").map((s) => s.trim()).filter(Boolean))];
}

export function parseAisFiltersFromQuery(q = {}) {
  return {
    flags: parseMultiQuery(q.flags, q.flag),
    vesselTypes: parseMultiQuery(q.vessel_types, q.vessel_type, q.vesselType),
    shipNames: parseMultiQuery(q.ship_names, q.ship_name, q.shipName),
    germanUnits: parseMultiQuery(q.german_units, q.germanUnits),
    russianUnits: parseMultiQuery(q.russian_units, q.russianUnits),
  };
}

function asList(value) {
  if (Array.isArray(value)) return value.map(String).map((s) => s.trim()).filter(Boolean);
  const s = String(value ?? "").trim();
  return s ? [s] : [];
}

/** Normalise API filter object (arrays or legacy single strings). */
export function normalizeAisFilterObject(filters = {}) {
  return {
    flags: filters.flags?.length ? asList(filters.flags) : asList(filters.flag),
    vesselTypes: filters.vesselTypes?.length
      ? asList(filters.vesselTypes)
      : asList(filters.vesselType),
    shipNames: filters.shipNames?.length ? asList(filters.shipNames) : asList(filters.shipName),
    germanUnits: asList(filters.germanUnits),
    russianUnits: asList(filters.russianUnits),
  };
}

export function sqlInLiterals(column, values) {
  const list = asList(values);
  if (list.length === 0) return null;
  const literals = list.map((v) => `'${String(v).replace(/'/g, "''")}'`).join(", ");
  return `${column} IN (${literals})`;
}