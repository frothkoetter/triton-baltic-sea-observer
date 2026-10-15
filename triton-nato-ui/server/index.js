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
  getKpis,
  getObservationAreas,
  getSanctions,
  getSurveillance,
  listTables,
  queryImpala,
  runQueryOrSample,
} from "./impala.js";
import { DATASET_STARTER_SQL } from "./queries.js";
import { validateReadOnlySql } from "./sql-validator.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "..", ".env.local") });
dotenv.config({ path: path.join(__dirname, "..", ".env") });

const app = express();
const PORT = Number(process.env.API_PORT || 3001);

app.use(cors());
app.use(express.json({ limit: "1mb" }));

app.get("/api/health", async (_req, res) => {
  const state = getConnectionState();
  res.json({
    ok: true,
    database: process.env.IMPALA_DATABASE || "defense",
    dataSource: state.live ? "impala" : "sample",
    impala: state,
  });
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

app.post("/api/query", async (req, res) => {
  const { sql } = req.body ?? {};
  const check = validateReadOnlySql(sql);
  if (!check.ok) {
    return res.status(400).json({ ok: false, error: check.error });
  }
  const impala = await queryImpala(check.sql);
  if (impala.ok) {
    return res.json(impala);
  }
  return res.status(503).json({
    ok: false,
    error: impala.error,
    hint: "Set IMPALA_* in .env.local or use TRITON_FORCE_SAMPLE_DATA=true for demo panels",
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

app.listen(PORT, () => {
  console.log(`Triton API listening on http://127.0.0.1:${PORT}`);
});
