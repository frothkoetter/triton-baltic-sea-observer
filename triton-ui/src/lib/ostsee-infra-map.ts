import { wktToLineStrings, type LatLng } from "./wkt-geo";

export type OstseeInfraRow = {
  id: string;
  name: string;
  typ: string;
  geometrie_wkt: string;
  status?: string;
  medium?: string;
};

export type OstseePipelineRow = {
  pipeline_name: string;
  medium?: string;
  status?: string;
  geo_typ?: string;
  geometrie_wkt: string;
  quelle?: string;
};

export type InfraMapLine = {
  key: string;
  lines: LatLng[][];
  label: string;
  meta: string;
  style: { color: string; weight: number; dashArray: string; opacity: number };
};

export function normalizeOstseeInfra(row: Record<string, unknown>): OstseeInfraRow | null {
  const wkt = String(row.geometrie_wkt ?? row.Geometrie_Wkt ?? "").trim();
  if (!wkt) return null;
  return {
    id: String(row.id ?? row.Id ?? ""),
    name: String(row.name ?? row.Name ?? ""),
    typ: String(row.typ ?? row.Typ ?? ""),
    geometrie_wkt: wkt,
    status: row.status != null ? String(row.status) : undefined,
    medium: row.medium != null ? String(row.medium) : undefined,
  };
}

export function normalizeOstseePipeline(row: Record<string, unknown>): OstseePipelineRow | null {
  const wkt = String(row.geometrie_wkt ?? row.Geometrie_Wkt ?? "").trim();
  if (!wkt) return null;
  return {
    pipeline_name: String(row.pipeline_name ?? row.Pipeline_Name ?? ""),
    medium: row.medium != null ? String(row.medium) : undefined,
    status: row.status != null ? String(row.status) : undefined,
    geo_typ: row.geo_typ != null ? String(row.geo_typ) : undefined,
    geometrie_wkt: wkt,
    quelle: row.quelle != null ? String(row.quelle) : undefined,
  };
}

function infraLineStyle(row: OstseeInfraRow | OstseePipelineRow) {
  const medium = String(
    "medium" in row ? row.medium : (row as OstseeInfraRow).typ ?? ""
  ).toLowerCase();
  const isCable =
    medium.includes("telecom") ||
    medium.includes("power") ||
    medium.includes("kabel") ||
    String((row as OstseeInfraRow).typ ?? "").toLowerCase().includes("seekabel");
  if (isCable) {
    return { color: "#26C6DA", weight: 3, dashArray: "6 4", opacity: 0.88 };
  }
  return { color: "#FF6F00", weight: 4, dashArray: "10 6", opacity: 0.9 };
}

/** Cable / pipeline polylines for Leaflet (same rules as Lagebild TacticalMap). */
export function buildOstseeInfraMapLines(
  infrastruktur: OstseeInfraRow[],
  pipelines: OstseePipelineRow[]
): InfraMapLine[] {
  const out: InfraMapLine[] = [];
  for (const row of infrastruktur) {
    const lines = wktToLineStrings(row.geometrie_wkt);
    if (lines.length === 0) continue;
    out.push({
      key: `infra-${row.id}`,
      lines,
      label: row.name,
      meta: `${row.typ} · ${row.medium ?? "—"} · ${row.status ?? "—"}`,
      style: infraLineStyle(row),
    });
  }
  const seenWkt = new Set(infrastruktur.map((r) => r.geometrie_wkt.trim()));
  for (const row of pipelines) {
    if (seenWkt.has(row.geometrie_wkt.trim())) continue;
    const lines = wktToLineStrings(row.geometrie_wkt);
    if (lines.length === 0) continue;
    out.push({
      key: `pipe-${row.pipeline_name}-${row.quelle ?? "wkt"}`,
      lines,
      label: row.pipeline_name,
      meta: `${row.medium ?? "—"} · ${row.status ?? "—"} · ${row.quelle ?? ""}`,
      style: infraLineStyle(row),
    });
  }
  return out;
}

export function ostseeCableFilterOptions(
  infrastruktur: OstseeInfraRow[],
  pipelines: OstseePipelineRow[],
  segmentAssets: string[]
): string[] {
  const names = new Set<string>();
  for (const a of segmentAssets) {
    if (a) names.add(a);
  }
  for (const p of pipelines) {
    if (p.pipeline_name) names.add(p.pipeline_name);
  }
  for (const r of infrastruktur) {
    const typ = r.typ.toLowerCase();
    const med = String(r.medium ?? "").toLowerCase();
    if (typ.includes("kabel") || med.includes("telecom") || med.includes("power") || typ.includes("seekabel")) {
      if (r.name) names.add(r.name);
    }
  }
  return [...names].sort((a, b) => a.localeCompare(b));
}