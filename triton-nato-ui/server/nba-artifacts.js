import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import decisionSample from "./data/nba-decision-record-sample.json" with { type: "json" };
import playbookBundled from "./data/nba-rules-playbook.json" with { type: "json" };

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../..");

function artifactsRoot() {
  if (process.env.NBA_ARTIFACTS_DIR) {
    return path.resolve(process.env.NBA_ARTIFACTS_DIR);
  }
  return path.join(REPO_ROOT, "cai-workbench/next-best-action/artifacts/nba");
}

function playbookPath() {
  if (process.env.NBA_PLAYBOOK_PATH) {
    return path.resolve(process.env.NBA_PLAYBOOK_PATH);
  }
  return path.join(REPO_ROOT, "cai-workbench/next-best-action/data/rules_playbook.json");
}

export function loadPlaybook() {
  const custom = playbookPath();
  if (fs.existsSync(custom)) {
    return { source: "repo", playbook: JSON.parse(fs.readFileSync(custom, "utf8")) };
  }
  return { source: "bundled", playbook: playbookBundled };
}

export async function loadDecisionRecord(flowUuid, trinoLoader) {
  if (trinoLoader) {
    const fromTrino = await trinoLoader(flowUuid);
    if (fromTrino?.ok && fromTrino.record) {
      return fromTrino;
    }
  }
  const file = path.join(artifactsRoot(), flowUuid, "decision_record.json");
  if (fs.existsSync(file)) {
    return {
      ok: true,
      source: "artifacts",
      record: JSON.parse(fs.readFileSync(file, "utf8")),
    };
  }
  const sample = { ...decisionSample, flow_uuid: flowUuid };
  return { ok: true, source: "sample", record: sample };
}

export function loadGovernanceBundle(flowUuid) {
  const file = path.join(artifactsRoot(), flowUuid, "governance_bundle.json");
  if (fs.existsSync(file)) {
    return {
      ok: true,
      source: "artifacts",
      bundle: JSON.parse(fs.readFileSync(file, "utf8")),
    };
  }
  return { ok: false, source: "missing" };
}

export function loadSemanticaRecord(flowUuid) {
  const file = path.join(artifactsRoot(), flowUuid, "semantica_record.json");
  if (fs.existsSync(file)) {
    return {
      ok: true,
      source: "artifacts",
      record: JSON.parse(fs.readFileSync(file, "utf8")),
    };
  }
  return { ok: false, source: "missing" };
}

export function hasSemanticaRecord(flowUuid) {
  return fs.existsSync(
    path.join(artifactsRoot(), flowUuid, "semantica_record.json"),
  );
}

export function loadCoaMatrixArtifact(flowUuid) {
  const file = path.join(artifactsRoot(), flowUuid, "coa_matrix.json");
  if (!fs.existsSync(file)) {
    return { ok: false, source: "missing" };
  }
  const rows = JSON.parse(fs.readFileSync(file, "utf8"));
  return { ok: true, source: "artifacts", rows: Array.isArray(rows) ? rows : [] };
}