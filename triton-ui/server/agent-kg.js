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

/** Session SQL uses defense.* — qualifyDefenseSql() → iceberg.defense.* */
const DEFENSE = "defense";
const DEFENSE_LABEL = "iceberg.defense";

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
    id: "ttyd_sanctioned_latest_ais",
    label: "TTYD · SanctionedMaritimePlatform + latest AIS",
    entity_id: "SanctionedMaritimePlatform",
    alert_types: ["rus_fleet_proximity"],
    description: "Golden GQ-03 — sanction_reason + last position (ais_events_ice).",
    sql: qualify(`
      WITH sanctioned_list AS (
        SELECT name, mmsi, sanction_reason
        FROM {defense}.sanctioned_vessels
      ),
      latest_ais AS (
        SELECT a.mmsi, a.latitude, a.longitude, a.event_timestamp,
          row_number() OVER (PARTITION BY a.mmsi ORDER BY coalesce(
            try(from_iso8601_timestamp(replace(a.event_timestamp, ' ', 'T'))),
            try_cast(a.event_timestamp AS timestamp)
          ) DESC) AS rn
        FROM {defense}.ais_events_ice a
        WHERE a.mmsi IN (SELECT mmsi FROM sanctioned_list WHERE mmsi IS NOT NULL)
      )
      SELECT sl.mmsi, sl.name, sl.sanction_reason,
        la.latitude, la.longitude, la.event_timestamp
      FROM sanctioned_list sl
      LEFT JOIN latest_ais la ON sl.mmsi = la.mmsi AND la.rn = 1
      ORDER BY sl.name
      LIMIT 25
    `),
  },
  {
    id: "ttyd_gps_jamming_nic",
    label: "TTYD · ProbableGPSJamming (adsb_nic < 5)",
    entity_id: "GPSElectronicWarfareIndicator",
    alert_types: ["gps_jammer"],
    description: "Golden GQ-05 — NATO NIC threshold.",
    sql: qualify(`
      SELECT geohash, ts AS event_timestamp, latitude, longitude, adsb_nic, jamming_indicator
      FROM {defense}.gps_jammer_events
      WHERE adsb_nic < 5
      ORDER BY ts DESC
      LIMIT 50
    `),
  },
  {
    id: "ttyd_area_entry_sample",
    label: "TTYD · AreaViolationEvent (enter) sample",
    entity_id: "MaritimeAnomaly",
    alert_types: ["ais_pipeline_blackout"],
    description: "Golden GQ-04 — area_violation VIEW + area_id.",
    sql: qualify(`
      SELECT av.mmsi, av.area_id, av.transition, av.event_timestamp
      FROM {defense}.area_violation av
      WHERE av.transition = 'enter'
      ORDER BY av.event_timestamp DESC
      LIMIT 20
    `),
  },
  {
    id: "ttyd_lagebild_gdansk",
    label: "TTYD · SituationPicture Gdańsk 24h",
    entity_id: "SituationPicture",
    alert_types: ["lagebild_flash"],
    description: "Lagebild slices near PortFacility Gdańsk.",
    sql: qualify(`
      SELECT "Data_Source" AS source, harbour_name, COUNT(*) AS n
      FROM {defense}.lagebild
      WHERE harbour_name = 'Gdańsk'
      GROUP BY 1, 2
      ORDER BY n DESC
      LIMIT 20
    `),
  },
  {
    id: "ttyd_marine_messages_baltic",
    label: "TTYD · FormattedTacticalMessage Baltic",
    entity_id: "SituationPicture",
    alert_types: ["sitrep_escalation"],
    description: "marine_messages DTG + subject (Baltic bbox).",
    sql: qualify(`
      SELECT message_dtg, message_subject
      FROM {defense}.marine_messages
      WHERE message_latitude BETWEEN 53.0 AND 60.5
        AND message_longitude BETWEEN 9.0 AND 30.5
      ORDER BY message_timestamp DESC
      LIMIT 40
    `),
  },
  {
    id: "ttyd_fishing_near_kiel",
    label: "TTYD · Fishing vessels near Kiel",
    entity_id: "MaritimeUnit",
    alert_types: ["demo_greifswald"],
    description: "AIS positions + ships registry (not ships.lat).",
    sql: qualify(`
      WITH port AS (
        SELECT latitude, longitude FROM {defense}.baltic_sea_harbours WHERE name = 'Kiel' LIMIT 1
      ),
      latest AS (
        SELECT a.mmsi, a.latitude, a.longitude, a.event_timestamp,
          row_number() OVER (PARTITION BY a.mmsi ORDER BY coalesce(
            try(from_iso8601_timestamp(replace(a.event_timestamp, ' ', 'T'))),
            try_cast(a.event_timestamp AS timestamp)
          ) DESC) AS rn
        FROM {defense}.ais_events_ice a
        WHERE a.latitude IS NOT NULL
      )
      SELECT s.ship_name, s.vessel_type, l.mmsi, l.latitude, l.longitude,
        great_circle_distance(l.latitude, l.longitude, p.latitude, p.longitude) * 1000 AS distance_m
      FROM latest l
      JOIN {defense}.ships s ON l.mmsi = s.mmsi
      CROSS JOIN port p
      WHERE l.rn = 1
        AND lower(s.vessel_type) LIKE '%fish%'
        AND great_circle_distance(l.latitude, l.longitude, p.latitude, p.longitude) * 1000 <= 30000
      ORDER BY distance_m
      LIMIT 25
    `),
  },
  {
    id: "ttyd_nba_mda_history",
    label: "TTYD · Prior NBA / MDA-style incidents",
    entity_id: "NbaDecision",
    alert_types: ["lagebild_flash", "rus_fleet_proximity"],
    description: "Replaces phantom mda_decision table.",
    sql: qualify(`
      SELECT flow_uuid, alert_type, priority, status, theatre, updated_at
      FROM {defense}.nba_incidents
      WHERE alert_type IN ('rus_fleet_proximity', 'lagebild_flash', 'gps_jammer', 'das_cable_threat')
      ORDER BY updated_at DESC
      LIMIT 15
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
    catalog: DEFENSE_LABEL,
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
            label: `${DEFENSE_LABEL}.${table}`,
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