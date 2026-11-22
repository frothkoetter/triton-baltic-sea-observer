import cors from "cors";
import dotenv from "dotenv";
import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getAis,
  getAisAnalytics,
  getAreaViolations,
  getConnectionState,
  getDatasets,
  getFleet,
  getFleetAnalytics,
  getRusFleetAnalytics,
  getBuoyAnalytics,
  getFleetPositions,
  getKpis,
  getLidarScans,
  getDasCableSegments,
  getObservationAreas,
  getOstseeInfrastructure,
  getPositionTracks,
  getVesselOutliers,
  getSanctions,
  getSanctionedAnalytics,
  getRestrictedAnalytics,
  getSurveillance,
  listTables,
  queryWarehouse,
  runQueryOrSample,
} from "./impala.js";
import { DATASET_STARTER_SQL } from "./queries.js";
import { fetchProximityAlertsLive } from "./proximity-alerts.js";
import { demoMeta } from "./demo-fallback.js";
import {
  approveNbaIncident,
  getNbaCoaRuns,
  getNbaDecisionRecordFromTrino,
  getNbaTrinoState,
  listNbaIncidents,
} from "./nba.js";
import {
  loadDecisionRecord,
  loadGovernanceBundle,
  loadPlaybook,
  loadSemanticaRecord,
} from "./nba-artifacts.js";
import { fetchLidarPointsFromS3, s3Configured, s3ConfigHint } from "./lidar.js";
import { fetchDasSegmentPreview, dasS3Configured } from "./das.js";
import {
  fetchSitrepPdfBase64,
  listSitrepReports,
  sitrepConfigured,
} from "./sitrep.js";
import { loadBuildStamp } from "./build-stamp.js";
import { distDasPreviewBundled } from "./ui-dist-check.js";
import { validateReadOnlySql } from "./sql-validator.js";
import {
  AGENT_APPS,
  postAgentChat,
  readBusinessRulesSummary,
  readOntologySummary,
} from "./agent-chat.js";
import {
  COOKIE_NAME,
  dataModeMeta,
  getSampleReason,
  runWithDataMode,
  shouldUseSampleData,
} from "./data-mode.js";
import { runTrino, trinoConfigured, trinoEnvDiagnostics } from "./trino-client.js";

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

app.use((req, res, next) => {
  runWithDataMode(req, () => next());
});

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

app.post("/api/data-mode", (req, res) => {
  const mode = String(req.body?.mode ?? "").toLowerCase();
  if (!["live", "sample", "auto"].includes(mode)) {
    return res.status(400).json({ ok: false, error: "mode must be live, sample, or auto" });
  }
  if (process.env.TRITON_FORCE_SAMPLE_DATA === "true") {
    return res.status(409).json({
      ok: false,
      error: "TRITON_FORCE_SAMPLE_DATA=true — toggle disabled",
      dataMode: "sample",
    });
  }
  res.setHeader(
    "Set-Cookie",
    `${COOKIE_NAME}=${mode}; Path=/; Max-Age=31536000; SameSite=Lax`
  );
  res.json({
    ok: true,
    mode,
    hint: mode === "live" ? "Requires TRINO_USER/TRINO_PASSWORD on the Application" : null,
  });
});

app.get("/api/map-config", (_req, res) => {
  const mapboxToken =
    process.env.MAPBOX_ACCESS_TOKEN || process.env.VITE_MAPBOX_TOKEN || "";
  res.json({ mapboxToken });
});

app.get("/api/version", (_req, res) => {
  const stamp = loadBuildStamp();
  const distPath = path.join(__dirname, "..", "dist");
  const distUi = distDasPreviewBundled(distPath);
  res.json({
    ok: true,
    app: "triton-ui",
    version: stamp.version,
    build: stamp.build,
    features: stamp.features ?? { dataModeFooterToggle: true, agentConsole: true },
    distDasPreview: distUi.ok,
    distAsset: distUi.asset ?? null,
    distError: distUi.ok ? null : distUi.error,
  });
});

app.get("/api/health", async (req, res) => {
  const state = getConnectionState();
  const nbaTrino = getNbaTrinoState();
  const modeMeta = dataModeMeta();
  let dataSource = "sample";
  let warehouseProbeError = null;
  if (shouldUseSampleData()) {
    warehouseProbeError = getSampleReason();
  } else if (trinoConfigured()) {
    const probe = await runTrino("SELECT 1 AS ok");
    if (probe.ok) {
      dataSource = "trino";
    } else {
      warehouseProbeError = probe.error || "Trino probe failed";
    }
  } else {
    warehouseProbeError = "TRINO_USER/TRINO_PASSWORD not set on Application";
  }
  if (!warehouseProbeError && state.live && state.engine) {
    dataSource = state.engine;
  }
  res.json({
    ok: true,
    database:
      process.env.TRINO_SCHEMA ||
      process.env.IMPALA_DATABASE ||
      "defense",
    catalog: process.env.TRINO_CATALOG || "iceberg",
    dataSource,
    warehouseProbeError,
    ...modeMeta,
    impala: state,
    nba: {
      trinoConfigured: Boolean(process.env.TRINO_USER && process.env.TRINO_PASSWORD),
      trinoLive: nbaTrino.live,
      lastError: nbaTrino.lastError,
      ...demoMeta(),
    },
    lidar: {
      s3Configured: s3Configured(),
    },
    das: {
      s3Configured: dasS3Configured(),
      s3Hint: s3ConfigHint(),
      playMaxSec: Number(process.env.DAS_PLAY_MAX_SEC || 10),
      distPreviewBundled: distDasPreviewBundled(path.join(__dirname, "..", "dist")).ok,
    },
    sitrep: {
      configured: sitrepConfigured(),
    },
    trino: {
      configured: trinoConfigured(),
      ...trinoEnvDiagnostics(),
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

app.get("/api/nba/incidents/:flowUuid/governance-bundle", (req, res) => {
  try {
    res.json(loadGovernanceBundle(req.params.flowUuid));
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.get("/api/nba/incidents/:flowUuid/semantica-record", (req, res) => {
  try {
    res.json(loadSemanticaRecord(req.params.flowUuid));
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.get("/api/nba/rules-playbook", (_req, res) => {
  const { source, playbook } = loadPlaybook();
  res.json({ ok: true, source, playbook });
});

app.get("/api/nba/incidents/:flowUuid/decision-record", async (req, res) => {
  try {
    const data = await loadDecisionRecord(
      req.params.flowUuid,
      getNbaDecisionRecordFromTrino
    );
    res.json(data);
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) });
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

app.get("/api/ais", async (req, res) => {
  const q = req.query ?? {};
  res.json(
    await getAis({
      flag: String(q.flag ?? "").trim(),
      vesselType: String(q.vessel_type ?? q.vesselType ?? "").trim(),
      shipName: String(q.ship_name ?? q.shipName ?? "").trim(),
    })
  );
});

app.get("/api/analyses/ais-analytics", async (req, res) => {
  const q = req.query ?? {};
  res.json(
    await getAisAnalytics({
      flag: String(q.flag ?? "").trim(),
      vesselType: String(q.vessel_type ?? q.vesselType ?? "").trim(),
      shipName: String(q.ship_name ?? q.shipName ?? "").trim(),
    })
  );
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

app.get("/api/analyses/fleet-analytics", async (_req, res) => {
  res.json(await getFleetAnalytics());
});

app.get("/api/analyses/rus-fleet-analytics", async (_req, res) => {
  res.json(await getRusFleetAnalytics());
});

app.get("/api/analyses/buoy-analytics", async (_req, res) => {
  res.json(await getBuoyAnalytics());
});

app.get("/api/analyses/sanctions", async (_req, res) => {
  res.json(await getSanctions());
});

app.get("/api/analyses/sanctioned-analytics", async (_req, res) => {
  res.json(await getSanctionedAnalytics());
});

app.get("/api/analyses/violations", async (_req, res) => {
  res.json(await getAreaViolations());
});

app.get("/api/analyses/restricted-analytics", async (_req, res) => {
  res.json(await getRestrictedAnalytics());
});

app.get("/api/analyses/surveillance", async (_req, res) => {
  res.json(await getSurveillance());
});

app.get("/api/observation-areas", async (_req, res) => {
  res.json(await getObservationAreas());
});

app.get("/api/ostsee-infrastructure", async (_req, res) => {
  res.json(await getOstseeInfrastructure());
});

app.get("/api/lidar/scans", async (_req, res) => {
  res.json(await getLidarScans());
});

app.get("/api/das-cable/segments", async (_req, res) => {
  res.json(await getDasCableSegments());
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

app.get("/api/das-cable/preview", async (req, res) => {
  const uri = String(req.query.hadoop_file_url ?? req.query.uri ?? "").trim();
  const segmentIndex = Number(req.query.segment_index ?? 0);
  if (!uri) {
    return res.status(400).json({ ok: false, error: "hadoop_file_url query param required" });
  }
  const result = await fetchDasSegmentPreview(uri, segmentIndex);
  if (!result.ok) {
    const err = String(result.error ?? "");
    const status = err.includes("not configured") || err.includes("Configure project")
      ? 503
      : err.includes("not allowed") || err.includes("URI not allowed")
        ? 403
        : 502;
    if (!result.hint && (!dasS3Configured() || status === 503)) {
      result.hint = s3ConfigHint();
    }
    return res.status(status).json(result);
  }
  res.json(result);
});

app.get("/api/proximity-alerts", async (_req, res) => {
  const result = await fetchProximityAlertsLive();
  res.json(result);
});

app.get("/api/agents/apps", (_req, res) => {
  res.json({ ok: true, apps: AGENT_APPS });
});

app.get("/api/agents/ontology", (_req, res) => {
  res.json(readOntologySummary());
});

app.get("/api/agents/business-rules", (_req, res) => {
  res.json(readBusinessRulesSummary());
});

app.post("/api/agents/chat", async (req, res) => {
  try {
    const result = await postAgentChat(req.body);
    if (!result.ok) {
      return res.status(400).json(result);
    }
    res.json(result);
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
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
