/**
 * Talk-to-your-Data: resolve NL → KDM mapping plan, compare KG vs Trino evidence SQL.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildSemanticaKgGraph, getKgSqlQuery, listKgSqlQueries } from "./agent-kg.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = process.env.TRITON_REPO_ROOT
  ? path.resolve(process.env.TRITON_REPO_ROOT)
  : path.resolve(__dirname, "..", "..");
const KDM_FILE = path.join(REPO_ROOT, "cai-workbench", "next-best-action", "data", "kdm_defense_classes.json");

function readKdm() {
  if (!fs.existsSync(KDM_FILE)) {
    return { version: "missing", classes: [], golden_examples: [], thresholds: {} };
  }
  return JSON.parse(fs.readFileSync(KDM_FILE, "utf8"));
}

function norm(s) {
  return String(s ?? "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/\p{M}/gu, "");
}

/** Score golden example by keyword overlap. */
export function resolveGoldenExample(text) {
  const kdm = readKdm();
  const hay = norm(text);
  let best = null;
  let bestScore = 0;
  for (const ex of kdm.golden_examples || []) {
    let score = 0;
    for (const p of ex.prompts || []) {
      if (hay.includes(norm(p))) score += 2;
    }
    if (ex.id && hay.includes(norm(ex.id))) score += 3;
    for (const c of ex.classes || []) {
      if (hay.includes(norm(c))) score += 1;
    }
    if (score > bestScore) {
      bestScore = score;
      best = ex;
    }
  }
  if (!best || bestScore < 2) {
    return { ok: false, error: "No golden example matched (score < 2)", score: bestScore };
  }
  return { ok: true, example: best, score: bestScore };
}

export function buildMappingPlan(example) {
  const kdm = readKdm();
  const byId = new Map((kdm.classes || []).map((c) => [c.class_id, c]));
  const classes = (example.classes || []).map((id) => byId.get(id)).filter(Boolean);
  const tables = new Set();
  const views = new Set();
  for (const c of classes) {
    if (c.primary_table) tables.add(c.primary_table);
    if (c.primary_view) views.add(c.primary_view);
    for (const t of c.primary_tables || []) tables.add(t);
    if (c.position_via?.table) tables.add(c.position_via.table);
    if (c.preferred_view) views.add(c.preferred_view);
  }
  return {
    schema: "triton-mapping-plan/1",
    example_id: example.id,
    description_de: example.description_de,
    kdm_classes: example.classes,
    warehouse: kdm.warehouse ?? { catalog: "iceberg", schema: "defense" },
    tables: [...tables],
    views: [...views],
    kg_query_id: example.kg_query_id,
    thresholds: kdm.thresholds,
    class_details: classes.map((c) => ({
      class_id: c.class_id,
      primary_table: c.primary_table ?? null,
      primary_view: c.primary_view ?? null,
      note: c.note ?? c.position_via?.note ?? null,
    })),
  };
}

const KDM_TO_ENTITY = {
  SanctionedMaritimePlatform: "SanctionedVessel",
  AISTrackReport: "AISObservation",
  MaritimePlatform: "MaritimeUnit",
  NavalUnit: "MaritimeUnit",
  GPSElectronicWarfareIndicator: "MaritimeAnomaly",
  SituationPictureEntry: "SituationPicture",
  VesselAnomalyEvent: "MaritimeAnomaly",
  AreaViolationEvent: "RestrictedOperatingArea",
  FormattedTacticalMessage: "SituationPicture",
  PortFacility: "SituationPicture",
  SituationAwarenessSummary: "SituationPicture",
};

const KDM_TO_CONCEPT = {
  GPSElectronicWarfareIndicator: "gps_jammer",
  AreaViolationEvent: "ais_pipeline_blackout",
  VesselAnomalyEvent: "ais_pipeline_blackout",
};

/** Tables the Semantica KG links to given KDM class ids. */
export function kgTablesForClasses(classIds) {
  const g = buildSemanticaKgGraph().graph;
  const nodes = g?.nodes ?? [];
  const edges = g?.edges ?? [];
  const entityIds = new Set();
  for (const cid of classIds) {
    const entLabel = KDM_TO_ENTITY[cid] ?? cid;
    for (const n of nodes) {
      if (n.kind === "SemanticEntity" && (n.label === entLabel || norm(n.label) === norm(entLabel))) {
        entityIds.add(n.id);
      }
    }
  }
  const conceptIds = new Set();
  for (const cid of classIds) {
    const concept = KDM_TO_CONCEPT[cid];
    if (concept) conceptIds.add(`concept:${concept}`);
  }
  const tables = new Set();
  for (const e of edges) {
    if (entityIds.has(e.from) || conceptIds.has(e.from)) {
      const to = nodes.find((n) => n.id === e.to);
      if (to?.table) tables.add(to.table);
    }
  }
  return [...tables];
}

export function compareKgVsPlan(mappingPlan, kgTables) {
  const planTables = new Set([...mappingPlan.tables, ...mappingPlan.views]);
  const kgSet = new Set(kgTables);
  const inBoth = [...planTables].filter((t) => kgSet.has(t));
  const planOnly = [...planTables].filter((t) => !kgSet.has(t));
  const kgOnly = [...kgSet].filter((t) => !planTables.has(t));
  return {
    consistent: planOnly.length === 0 || inBoth.length >= Math.min(planTables.size, 1),
    overlap_count: inBoth.length,
    tables_in_both: inBoth,
    tables_plan_only: planOnly,
    tables_kg_only: kgOnly,
  };
}

export function listTtydCatalog() {
  const kdm = readKdm();
  const sql = listKgSqlQueries();
  const goldenIds = new Set((kdm.golden_examples || []).map((e) => e.kg_query_id));
  return {
    ok: true,
    kdm_version: kdm.version,
    namespace: kdm.namespace,
    class_count: (kdm.classes || []).length,
    golden_examples: kdm.golden_examples,
    sql_queries: (sql.queries || []).filter((q) => goldenIds.has(q.id) || q.id.startsWith("ttyd_")),
  };
}

export async function runTtydE2e({ text, exampleId, queryWarehouse }) {
  const kdm = readKdm();
  let example = null;
  if (exampleId) {
    example = (kdm.golden_examples || []).find((e) => e.id === exampleId);
    if (!example) return { ok: false, error: `Unknown example_id: ${exampleId}` };
  } else if (text) {
    const resolved = resolveGoldenExample(text);
    if (!resolved.ok) return resolved;
    example = resolved.example;
  } else {
    return { ok: false, error: "Provide text or example_id" };
  }

  const mappingPlan = buildMappingPlan(example);
  const kgTables = kgTablesForClasses(example.classes || []);
  const compare = compareKgVsPlan(mappingPlan, kgTables);

  const resolved = getKgSqlQuery(example.kg_query_id);
  if (!resolved.ok) {
    return { ok: false, error: resolved.error, mapping_plan: mappingPlan, kg_compare: compare };
  }

  const sqlResult = await queryWarehouse(resolved.sql);

  return {
    ok: true,
    example_id: example.id,
    user_text: text ?? null,
    mapping_plan: mappingPlan,
    kg: {
      graph_schema: "triton-semantica-kg/1",
      tables_from_entity_edges: kgTables,
    },
    kg_compare: compare,
    sql: {
      query_id: example.kg_query_id,
      label: resolved.query.label,
      sql: resolved.sql,
      source: sqlResult.source,
      ok: sqlResult.ok,
      row_count: sqlResult.rows?.length ?? 0,
      columns: sqlResult.columns,
      rows: (sqlResult.rows ?? []).slice(0, 15),
      error: sqlResult.error ?? null,
    },
    synthesis: {
      summary_de: sqlResult.ok
        ? `${example.description_de}: ${sqlResult.rows?.length ?? 0} Zeilen (${sqlResult.source}). KG↔Plan overlap: ${compare.overlap_count}.`
        : `${example.description_de}: SQL fehlgeschlagen (${sqlResult.error ?? "unknown"}). Mapping plan weiterhin gültig.`,
      gaps: compare.tables_plan_only.length
        ? [`KG graph missing edges for: ${compare.tables_plan_only.join(", ")}`]
        : [],
    },
  };
}