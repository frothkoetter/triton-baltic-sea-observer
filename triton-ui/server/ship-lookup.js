import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { queryWarehouse } from "./impala.js";
import * as sampleEmbedded from "./sample-data.js";
import { getResolvedSampleData } from "./sample-snapshot-loader.js";

function getSample() {
  return getResolvedSampleData(sampleEmbedded);
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));

let staticRegistry = null;

function loadStaticRegistry() {
  if (staticRegistry) return staticRegistry;
  const file = path.join(__dirname, "data", "ships-mmsi-registry.json");
  try {
    staticRegistry = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    staticRegistry = {};
  }
  return staticRegistry;
}

function mergeSampleRegistries() {
  const map = { ...loadStaticRegistry() };
  for (const row of getSample().aisShips) {
    map[String(row.mmsi)] = {
      ship_name: row.ship_name,
      vessel_type: row.vessel_type,
    };
  }
  for (const row of getSample().sanctions) {
    const mmsi = String(row.MMSI ?? row.mmsi ?? "");
    if (!mmsi) continue;
    map[mmsi] = {
      ship_name: row.Name ?? row.name ?? mmsi,
      vessel_type: row.Type ?? row.type ?? "—",
    };
  }
  for (const row of getSample().germanFleetPositions) {
    map[String(row.mmsi)] = {
      ship_name: row.unit_name,
      vessel_type: row.unit_class,
    };
  }
  for (const row of getSample().russianFleetPositions) {
    map[String(row.mmsi)] = {
      ship_name: row.unit_name,
      vessel_type: row.unit_class,
    };
  }
  return map;
}

function sqlLiteral(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

export async function lookupShipsByMmsi(mmsiList) {
  const unique = [...new Set(mmsiList.map((m) => String(m).trim()).filter(Boolean))];
  const map = {};
  if (!unique.length) {
    return { map, source: "none" };
  }

  const inList = unique.map(sqlLiteral).join(", ");
  const sql = `
    SELECT CAST(mmsi AS varchar) AS mmsi, ship_name, vessel_type
    FROM defense.ships
    WHERE CAST(mmsi AS varchar) IN (${inList})
  `;

  const live = await queryWarehouse(sql);
  if (live.ok && live.rows?.length) {
    for (const row of live.rows) {
      const key = String(row.mmsi ?? "").trim();
      if (!key) continue;
      map[key] = {
        ship_name: String(row.ship_name ?? key),
        vessel_type: String(row.vessel_type ?? "—"),
      };
    }
    return { map, source: "live" };
  }

  const fallback = mergeSampleRegistries();
  for (const mmsi of unique) {
    if (fallback[mmsi]) map[mmsi] = fallback[mmsi];
  }
  return {
    map,
    source: "sample",
    fallbackReason: live.error ?? (Object.keys(map).length ? undefined : "No registry rows for MMSIs"),
  };
}

export function enrichProximityEvent(event, registry) {
  const a = registry[String(event.ship_a)] ?? {};
  const b = registry[String(event.ship_b)] ?? {};
  return {
    ...event,
    ship_a_mmsi: event.ship_a,
    ship_b_mmsi: event.ship_b,
    ship_a_name: a.ship_name ?? null,
    ship_a_type: a.vessel_type ?? null,
    ship_b_name: b.ship_name ?? null,
    ship_b_type: b.vessel_type ?? null,
  };
}