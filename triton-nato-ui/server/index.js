import cors from "cors";
import dotenv from "dotenv";
import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getAis,
  getAreaViolations,
  getConnectionState,
  getDatasets,
  getFleet,
  getFleetPositions,
  getKpis,
  getLidarScans,
  getObservationAreas,
  getPositionTracks,
  getVesselOutliers,
  getSanctions,
  getSurveillance,
  listTables,
  queryWarehouse,
  runQueryOrSample,
} from "./impala.js";
import { DATASET_STARTER_SQL } from "./queries.js";
import { fetchProximityAlertsLive } from "./proximity-alerts.js";
import {
  approveNbaIncident,
  getNbaCoaRuns,
  getNbaTrinoState,
  listNbaIncidents,
} from "./nba.js";
import { fetchLidarPointsFromS3, s3Configured } from "./lidar.js";
import {
  fetchSitrepPdfBase64,
  listSitrepReports,
  sitrepConfigured,
} from "./sitrep.js";
import { validateReadOnlySql } from "./sql-validator.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "..", ".env.local") });
dotenv.config({ path: path.join(__dirname, "..", ".env") });

const app = express();
const PORT = Number(
  process.env.CDSW_APP_PORT || process.env.API_PORT || 3001
);

// Cloudera AI readiness probes (fast; no Impala/Trino).
// Set CDSW_APP_POLLING_ENDPOINT=/healthz or /healthcheck on the Application.
const healthProbe = (_req, res) => {
  res.status(200).type("text/plain").send("ok");
};
app.get("/healthz", healthProbe);
app.get("/healthcheck", healthProbe);

app.use(cors());
app.use(express.json({ limit: "1mb" }));

app.get("/api/s3/diagnose", async (req, res) => {
  const { spawn } = await import("node:child_process");
  const prefix =
    req.query.prefix ||
    process.env.SITREP_S3_PREFIX ||
    process.env.LAGEBILD_S3_PREFIX ||
  "";
  const script = path.join(__dirname, "s3_boto.py");
  await new Promise((resolve) => {
    const child = spawn("python3", [script, String(prefix)], {
      env: { ...process.env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    child.stdout.on("data", (d) => {
      out += d.toString();
    });
    child.on("close", () => {
      try {
        res.json(JSON.parse(out || "{}"));
      } catch {
        res.status(500).json({ ok: false, error: out || "diagnose failed" });
      }
      resolve();
    });
  });
});

app.get("/api/map-config", (_req, res) => {
  const mapboxToken =
    process.env.MAPBOX_ACCESS_TOKEN || process.env.VITE_MAPBOX_TOKEN || "";
  res.json({ mapboxToken });
});

app.get("/api/health", async (_req, res) => {
  const state = getConnectionState();
  const nbaTrino = getNbaTrinoState();
  const liveEngine = state.live ? state.engine || "impala" : "sample";
  res.json({
    ok: true,
    database:
      process.env.TRINO_SCHEMA ||
      process.env.IMPALA_DATABASE ||
      "defense",
    catalog: process.env.TRINO_CATALOG || "iceberg",
    dataSource: liveEngine,
    impala: state,
    nba: {
      trinoConfigured: Boolean(process.env.TRINO_USER && process.env.TRINO_PASSWORD),
      trinoLive: nbaTrino.live,
      lastError: nbaTrino.lastError,
    },
    lidar: {
      s3Configured: s3Configured(),
    },
    sitrep: {
      configured: sitrepConfigured(),
    },
  });
});

app.get("/api/nba/incidents", async (_req, res) => {
  res.json(await listNbaIncidents());
});

app.get("/api/nba/incidents/:flowUuid/coa-runs", async (req, res) => {
  try {
    const data = await getNbaCoaRuns(req.params.flowUuid);
    res.json(data);
  } catch (err) {
    res.status(400).json({ ok: false, error: String(err.message || err) });
  }
});

app.post("/api/nba/incidents/:flowUuid/approve", async (req, res) => {
  const { coaId, approvedBy } = req.body ?? {};
  if (!coaId) {
    return res.status(400).json({ ok: false, error: "coaId required" });
  }
  try {
    const result = await approveNbaIncident(
      req.params.flowUuid,
      coaId,
      approvedBy || "nato-ui-operator"
    );
    if (!result.ok) {
      return res.status(503).json(result);
    }
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(400).json({ ok: false, error: String(err.message || err) });
  }
});

app.get("/api/tables", async (_req, res) => {
  const result = await listTables();
  res.json(result);
});

app.get("/api/datasets", (_req, res) => {
  res.json({ ok: true, datasets: getDatasets(), starters: DATASET_STARTER_SQL });
});

app.get("/api/kpis", async (_req, res) => {
  res.json(await getKpis());
});

app.get("/api/ais", async (_req, res) => {
  res.json(await getAis());
});

app.get("/api/vessel-outliers", async (_req, res) => {
  res.json(await getVesselOutliers());
});

app.get("/api/fleet-positions", async (_req, res) => {
  res.json(await getFleetPositions());
});

app.get("/api/position-tracks", async (req, res) => {
  const q = req.query ?? {};
  res.json(
    await getPositionTracks({
      hours: q.hours,
      flag: q.flag,
      vesselType: q.vessel_type,
      shipName: q.ship_name,
      ais: q.ais !== "0" && q.ais !== "false",
      german: q.german !== "0" && q.german !== "false",
      russian: q.russian !== "0" && q.russian !== "false",
    })
  );
});

app.get("/api/analyses/fleet", async (_req, res) => {
  res.json(await getFleet());
});

app.get("/api/analyses/sanctions", async (_req, res) => {
  res.json(await getSanctions());
});

app.get("/api/analyses/violations", async (_req, res) => {
  res.json(await getAreaViolations());
});

app.get("/api/analyses/surveillance", async (_req, res) => {
  res.json(await getSurveillance());
});

app.get("/api/observation-areas", async (_req, res) => {
  res.json(await getObservationAreas());
});

app.get("/api/lidar/scans", async (_req, res) => {
  res.json(await getLidarScans());
});

app.get("/api/sitrep/reports", async (_req, res) => {
  const result = await listSitrepReports();
  if (!result.ok) {
    return res.status(result.error?.includes("Set") ? 503 : 502).json(result);
  }
  res.json(result);
});

app.get("/api/sitrep/reports/:pdfName/pdf", async (req, res) => {
  const name = req.params.pdfName;
  const result = await fetchSitrepPdfBase64(name);
  if (!result.ok) {
    return res.status(404).json(result);
  }
  const buf = Buffer.from(result.content_base64, "base64");
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `inline; filename="${name}"`);
  res.send(buf);
});

app.get("/api/lidar/points", async (req, res) => {
  const uri = String(req.query.hadoop_file_url ?? req.query.uri ?? "").trim();
  if (!uri) {
    return res.status(400).json({ ok: false, error: "hadoop_file_url query param required" });
  }
  const result = await fetchLidarPointsFromS3(uri);
  if (!result.ok) {
    return res.status(result.error?.includes("not configured") ? 503 : 502).json(result);
  }
  res.json(result);
});

app.get("/api/proximity-alerts", async (_req, res) => {
  const result = await fetchProximityAlertsLive();
  res.json(result);
});

app.post("/api/query", async (req, res) => {
  const { sql } = req.body ?? {};
  const check = validateReadOnlySql(sql);
  if (!check.ok) {
    return res.status(400).json({ ok: false, error: check.error });
  }
  const result = await queryWarehouse(check.sql);
  if (result.ok) {
    return res.json(result);
  }
  return res.status(503).json({
    ok: false,
    error: result.error,
    hint: "Set TRINO_USER/TRINO_PASSWORD (preferred) or IMPALA_* in Application env",
  });
});

const distPath = path.join(__dirname, "..", "dist");
app.use(express.static(distPath));
app.get("*", (req, res, next) => {
  if (req.path.startsWith("/api")) return next();
  if (process.env.NODE_ENV === "production") {
    res.sendFile(path.join(distPath, "index.html"));
  } else {
    res.status(404).json({ error: "Use Vite dev server on :5173" });
  }
});

// Cloudera docs: bind CDSW_APP_PORT / CDSW_READONLY_PORT on 127.0.0.1 (in-engine proxy).
const HOST =
  process.env.CDSW_APP_PORT || process.env.CDSW_READONLY_PORT
    ? "127.0.0.1"
    : "0.0.0.0";

app.listen(PORT, HOST, () => {
  console.log(`Triton API listening on http://${HOST}:${PORT}`);
});
