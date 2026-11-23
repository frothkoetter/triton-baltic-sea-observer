/**
 * Ontology → table → column registry (Semantica MCP-style grounding).
 * Validates allowlisted SQL against known defense schema mappings.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = process.env.TRITON_REPO_ROOT
  ? path.resolve(process.env.TRITON_REPO_ROOT)
  : path.resolve(__dirname, "..", "..");

const MAPPING_FILE = path.join(REPO_ROOT, "cai-workbench", "next-best-action", "data", "baltic_trino_mapping.json");
const KDM_FILE = path.join(REPO_ROOT, "cai-workbench", "next-best-action", "data", "kdm_defense_classes.json");

/** Trino / DDL extras not always in mapping key_columns */
const EXTRA_COLUMNS = {
  lagebild: ["Data_Source", "harbour_name", "event_time", "latitude", "longitude", "description"],
  sanctioned_vessels: ["name", "mmsi", "sanction_reason", "imo", "flag", "type"],
  gps_jammer_events: ["geohash", "ts", "latitude", "longitude", "adsb_nic", "jamming_indicator"],
  area_violation: ["mmsi", "area_id", "transition", "event_timestamp"],
  marine_messages: ["message_dtg", "message_subject", "message_timestamp", "message_latitude", "message_longitude"],
  observation_areas: ["area_id", "polygon", "country", "area_type"],
};

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function buildColumnRegistry() {
  const mapping = fs.existsSync(MAPPING_FILE) ? readJson(MAPPING_FILE) : { semantic_entities: [] };
  const registry = {};
  for (const ent of mapping.semantic_entities || []) {
    for (const t of ent.tables || []) {
      const table = t.table;
      const cols = new Set([...(t.key_columns || []), ...(EXTRA_COLUMNS[table] || [])]);
      if (!registry[table]) registry[table] = { columns: new Set(), entities: [] };
      for (const c of cols) registry[table].columns.add(c);
      registry[table].entities.push(ent.entity_id);
    }
  }
  for (const [table, cols] of Object.entries(EXTRA_COLUMNS)) {
    if (!registry[table]) registry[table] = { columns: new Set(), entities: [] };
    for (const c of cols) registry[table].columns.add(c);
  }
  const out = {};
  for (const [table, meta] of Object.entries(registry)) {
    out[table] = {
      columns: [...meta.columns].sort(),
      semantic_entities: [...new Set(meta.entities)],
    };
  }
  return { version: mapping.version, warehouse: mapping.warehouse, tables: out };
}

export function semanticMappingForClasses(classIds) {
  const kdm = fs.existsSync(KDM_FILE) ? readJson(KDM_FILE) : { classes: [] };
  const registry = buildColumnRegistry().tables;
  const byId = Object.fromEntries((kdm.classes || []).map((c) => [c.class_id, c]));
  const bindings = [];
  for (const cid of classIds || []) {
    const c = byId[cid];
    if (!c) continue;
    const tables = [
      c.primary_table,
      c.primary_view,
      ...(c.primary_tables || []),
      c.position_via?.table,
      c.preferred_view,
    ].filter(Boolean);
    for (const table of [...new Set(tables)]) {
      bindings.push({
        ontology_class: cid,
        table,
        columns: registry[table]?.columns ?? c.key_columns ?? [],
        iri: `${kdm.namespace ?? ""}${cid}`,
      });
    }
  }
  return bindings;
}

/** Light SQL column/table check for curated defense.* queries */
export function validateSqlMapping(sql, mappingPlan) {
  const registry = buildColumnRegistry().tables;
  const allowedTables = new Set([
    ...(mappingPlan?.tables || []),
    ...(mappingPlan?.views || []),
  ]);
  const warnings = [];
  const refs = [];

  const qualRe = /\b(?:iceberg\.defense|defense)\.([a-z_][a-z0-9_]*)\b/gi;
  let m;
  while ((m = qualRe.exec(sql)) !== null) {
    const table = m[1];
    refs.push({ table, qualified: true });
    if (allowedTables.size && !allowedTables.has(table)) {
      warnings.push(`Tabelle ${table} nicht im Mapping-Plan`);
    }
    if (!registry[table]) {
      warnings.push(`Tabelle ${table} nicht in Ontologie-Registry`);
    }
  }

  const aliasColRe = /\b([a-z_][a-z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
  while ((m = aliasColRe.exec(sql)) !== null) {
    const alias = m[1];
    const col = m[2];
    if (["try", "cast", "row_number", "partition", "order", "over", "select", "from", "where"].includes(alias)) continue;
    refs.push({ alias, column: col });
  }

  return {
    ok: warnings.length === 0,
    warnings,
    allowed_tables: [...allowedTables],
    registry_tables: Object.keys(registry).length,
    refs_sample: refs.slice(0, 24),
  };
}

export function readOntologyMapApi() {
  const reg = buildColumnRegistry();
  const mapping = fs.existsSync(MAPPING_FILE) ? readJson(MAPPING_FILE) : {};
  return {
    ok: true,
    schema: "triton-ontology-column-map/1",
    warehouse: reg.warehouse,
    semantic_entities: (mapping.semantic_entities || []).map((e) => ({
      entity_id: e.entity_id,
      iri: e.iri,
      tables: e.tables,
    })),
    column_registry: reg.tables,
  };
}