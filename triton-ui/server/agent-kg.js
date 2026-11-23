/**
 * Semantica KG view: ontology concepts + Trino entity bindings + curated evidence SQL.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { validateReadOnlySql } from "./sql-validator.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = process.env.TRITON_REPO_ROOT
  ? path.resolve(process.env.TRITON_REPO_ROOT)
  : path.resolve(__dirname, "..", "..");
const DATA_DIR = path.join(REPO_ROOT, "cai-workbench", "next-best-action", "data");
const ONTOLOGY_FILE = path.join(DATA_DIR, "baltic_ontology.json");
const TRINO_MAPPING_FILE = path.join(DATA_DIR, "baltic_trino_mapping.json");
const SEMANTICA_SAMPLE = path.join(__dirname, "data", "nba-semantica-record-sample.json");

const DEFENSE = "iceberg.defense";

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function qualify(sql) {
  return sql.replaceAll("{defense}", DEFENSE);
}

/** Curated read-only SQL for Semantica / agent KG evidence (IDs are allowlist keys). */
export const KG_SQL_QUERIES = [
  {
    id: "kg_ships_registry_gap",
    label: "MaritimeUnit · MMSI ohne ships-Eintrag",
    entity_id: "MaritimeUnit",
    alert_types: ["demo_greifswald", "rus_fleet_proximity"],
    description: "Evidence für AIS–Stammdaten-Lücke (Semantica ↔ Trino).",
    sql: qualify(`
      WITH latest AS (
        SELECT a.mmsi,
          row_number() OVER (PARTITION BY a.mmsi ORDER BY coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
          ) DESC) AS rn
        FROM {defense}.ais_events_ice a
        WHERE a.latitude IS NOT NULL
      )
      SELECT
        COUNT(*) AS distinct_mmsi,
        SUM(CASE WHEN s.mmsi IS NULL THEN 1 ELSE 0 END) AS ohne_ships,
        SUM(CASE WHEN s.mmsi IS NOT NULL THEN 1 ELSE 0 END) AS mit_ships
      FROM latest l
      LEFT JOIN {defense}.ships s ON l.mmsi = s.mmsi
      WHERE l.rn = 1
    `),
  },
  {
    id: "kg_das_threat_gt",
    label: "CriticalInfrastructure · DAS threat_class",
    entity_id: "CriticalInfrastructure",
    alert_types: ["das_cable_threat"],
    description: "Ground-truth Verteilung DAS-Segmente (NiFi Simulator).",
    sql: qualify(`
      SELECT threat_class AS label, COUNT(*) AS n
      FROM {defense}.das_cable_spectrogram_metadata
      WHERE threat_class IS NOT NULL
      GROUP BY 1
      ORDER BY n DESC
      LIMIT 20
    `),
  },
  {
    id: "kg_das_threat_ml",
    label: "CriticalInfrastructure · predicted_threat_type",
    entity_id: "CriticalInfrastructure",
    alert_types: ["das_cable_threat"],
    description: "CML-Klassifier (NiFi InvokeHTTP).",
    sql: qualify(`
      SELECT predicted_threat_type AS label, COUNT(*) AS n
      FROM {defense}.das_cable_spectrogram_metadata
      WHERE predicted_threat_type IS NOT NULL
      GROUP BY 1
      ORDER BY n DESC
      LIMIT 20
    `),
  },
  {
    id: "kg_das_threat_top1",
    label: "NBA das_cable_threat · Top segment 24h",
    entity_id: "CriticalInfrastructure",
    alert_types: ["das_cable_threat"],
    description: "Entspricht alerts/das_cable_threat_top1.sql.",
    sql: qualify(`
      SELECT
        cable_asset,
        threat_class,
        predicted_threat_type,
        model_confidence,
        segment_lat,
        segment_lon,
        ingested_at
      FROM {defense}.das_cable_spectrogram_metadata
      WHERE ingested_at >= current_timestamp - interval '24' hour
        AND segment_lat IS NOT NULL
        AND (
          threat_class IN ('cui_sabotage_tap', 'anchor_drag', 'cable_strain_anomaly')
          OR predicted_threat_type IN ('cui_sabotage_tap', 'anchor_drag', 'cable_strain_anomaly')
        )
      ORDER BY
        CASE COALESCE(predicted_threat_type, threat_class)
          WHEN 'cui_sabotage_tap' THEN 1
          WHEN 'anchor_drag' THEN 2
          WHEN 'cable_strain_anomaly' THEN 3
          ELSE 9
        END,
        COALESCE(model_confidence, 0) DESC,
        ingested_at DESC
      LIMIT 5
    `),
  },
  {
    id: "kg_outliers_by_type",
    label: "MaritimeAnomaly · vessel_outliers",
    entity_id: "MaritimeAnomaly",
    alert_types: ["ais_pipeline_blackout", "gps_jammer"],
    description: "Outlier-Feed für Lagebild / NBA.",
    sql: qualify(`
      SELECT outlier_type AS label, COUNT(*) AS n
      FROM {defense}.vessel_outliers
      GROUP BY 1
      ORDER BY n DESC
      LIMIT 16
    `),
  },
  {
    id: "kg_lagebild_sources",
    label: "SituationPicture · lagebild Data_Source",
    entity_id: "SituationPicture",
    alert_types: ["lagebild_flash", "sitrep_escalation"],
    description: "Semantica: SituationPicture aus aggregiertem Lagebild.",
    sql: qualify(`
      SELECT "Data_Source" AS source, COUNT(*) AS n
      FROM {defense}.lagebild
      GROUP BY 1
      ORDER BY n DESC
      LIMIT 20
    `),
  },
  {
    id: "kg_nba_open_incidents",
    label: "NbaDecision · recent incidents",
    entity_id: "NbaDecision",
    alert_types: ["lagebild_flash", "das_cable_threat", "gps_jammer"],
    description: "HITL-Incidents mit Ontology alert_type.",
    sql: qualify(`
      SELECT flow_uuid, alert_type, priority, status, theatre, best_coa_id, updated_at
      FROM {defense}.nba_incidents
      ORDER BY updated_at DESC
      LIMIT 10
    `),
  },
  {
    id: "kg_socmint_trust",
    label: "SOCMINT · trust_tag counts",
    entity_id: "SituationPicture",
    alert_types: ["sitrep_escalation"],
    description: "Social-media trust tags (UC1 Lagebild).",
    sql: qualify(`
      SELECT trust_tag AS label, COUNT(*) AS n
      FROM {defense}.social_media_messages
      GROUP BY 1
      ORDER BY n DESC
      LIMIT 12
    `),
  },
];

const queriesById = new Map(KG_SQL_QUERIES.map((q) => [q.id, q]));

export function listKgSqlQueries() {
  return {
    ok: true,
    catalog: DEFENSE,
    schema: "defense",
    queries: KG_SQL_QUERIES.map(({ id, label, entity_id, alert_types, description }) => ({
      id,
      label,
      entity_id,
      alert_types,
      description,
    })),
  };
}

export function getKgSqlQuery(id) {
  const q = queriesById.get(String(id));
  if (!q) return { ok: false, error: `Unknown kg query id: ${id}` };
  const check = validateReadOnlySql(q.sql);
  if (!check.ok) return { ok: false, error: check.error };
  return { ok: true, query: q, sql: check.sql };
}

export function buildSemanticaKgGraph() {
  const onto = fs.existsSync(ONTOLOGY_FILE)
    ? readJson(ONTOLOGY_FILE)
    : { alert_types: {}, rulebook_refs: {} };
  const mapping = fs.existsSync(TRINO_MAPPING_FILE)
    ? readJson(TRINO_MAPPING_FILE)
    : { semantic_entities: [], alert_type_bindings: {} };

  const nodes = [];
  const edges = [];

  for (const [conceptId, meta] of Object.entries(onto.alert_types || {})) {
    nodes.push({
      id: `concept:${conceptId}`,
      kind: "IncidentConcept",
      label: meta.label ?? conceptId,
      iri: meta.iri,
    });
    const bind = mapping.alert_type_bindings?.[conceptId];
    if (bind?.primary_tables) {
      for (const table of bind.primary_tables) {
        const tableId = `table:${table}`;
        if (!nodes.find((n) => n.id === tableId)) {
          nodes.push({
            id: tableId,
            kind: "TrinoTable",
            label: `${DEFENSE}.${table}`,
            table,
          });
        }
        edges.push({
          from: `concept:${conceptId}`,
          to: tableId,
          relation: "evidenceIn",
        });
      }
    }
  }

  for (const ent of mapping.semantic_entities || []) {
    const entityId = `entity:${ent.entity_id}`;
    if (!nodes.find((n) => n.id === entityId)) {
      nodes.push({
        id: entityId,
        kind: "SemanticEntity",
        label: ent.entity_id,
        iri: ent.iri,
      });
    }
    for (const t of ent.tables || []) {
      const tableId = `table:${t.table}`;
      if (!nodes.find((n) => n.id === tableId)) {
        nodes.push({
          id: tableId,
          kind: "TrinoTable",
          label: `${DEFENSE}.${t.table}`,
          table: t.table,
          role: t.role,
        });
      }
      edges.push({
        from: entityId,
        to: tableId,
        relation: "mapsTo",
      });
    }
  }

  for (const [ruleId, meta] of Object.entries(onto.rulebook_refs || {})) {
    nodes.push({
      id: `rule:${ruleId}`,
      kind: "RulebookRef",
      label: meta.label ?? ruleId,
    });
  }

  let semanticaBridge = null;
  if (fs.existsSync(SEMANTICA_SAMPLE)) {
    semanticaBridge = readJson(SEMANTICA_SAMPLE);
  }

  return {
    ok: true,
    graph: {
      schema: "triton-semantica-kg/1",
      namespace: onto.namespace,
      ontology_version: onto.version,
      warehouse: mapping.warehouse,
      nodes,
      edges,
      mcp: {
        record_decision: "Semantica MCP — fields from semantica_record.json",
        kg_seed: "cai-workbench/next-best-action/data/baltic_ontology.json",
        shacl: "cai-workbench/next-best-action/data/decision_record.ttl",
      },
      sample_bridge: semanticaBridge
        ? {
            schema: semanticaBridge.schema,
            flow_uuid: semanticaBridge.flow_uuid,
            snapshots: semanticaBridge.snapshots,
          }
        : null,
    },
  };
}