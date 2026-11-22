import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const REPO_ROOT = process.env.TRITON_REPO_ROOT
  ? path.resolve(process.env.TRITON_REPO_ROOT)
  : path.resolve(__dirname, "..", "..");
const CONSOLE_DIR = path.join(REPO_ROOT, "cai-workbench", "agent-console");
const BRIDGE = path.join(CONSOLE_DIR, "chat_bridge.py");
const DATA_DIR = path.join(REPO_ROOT, "cai-workbench", "next-best-action", "data");
const ONTOLOGY_FILE = path.join(DATA_DIR, "baltic_ontology.json");
const BUSINESS_RULES_FILE = path.join(DATA_DIR, "baltic_business_rules.json");
const TRINO_MAPPING_FILE = path.join(DATA_DIR, "baltic_trino_mapping.json");

function pythonEnv() {
  const nba = path.join(REPO_ROOT, "cai-workbench", "next-best-action");
  const prev = process.env.PYTHONPATH || "";
  const parts = [CONSOLE_DIR, nba].filter((p) => fs.existsSync(p));
  return { ...process.env, PYTHONPATH: [...parts, prev].filter(Boolean).join(":") };
}

function runBridge(payload) {
  return new Promise((resolve) => {
    if (!fs.existsSync(BRIDGE)) {
      resolve({ ok: false, error: `Missing chat bridge: ${BRIDGE}` });
      return;
    }
    const child = spawn("python3", [BRIDGE], {
      env: pythonEnv(),
      stdio: ["pipe", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => {
      out += d.toString();
    });
    child.stderr.on("data", (d) => {
      err += d.toString();
    });
    child.stdin.write(JSON.stringify(payload));
    child.stdin.end();
    child.on("close", (code) => {
      try {
        const parsed = JSON.parse(out || "{}");
        if (!parsed.ok && err) parsed.stderr = err.slice(0, 500);
        resolve(parsed);
      } catch {
        resolve({
          ok: false,
          error: err || out || `chat_bridge exit ${code}`,
        });
      }
    });
  });
}

export function readOntologySummary() {
  try {
    if (fs.existsSync(ONTOLOGY_FILE)) {
      const raw = JSON.parse(fs.readFileSync(ONTOLOGY_FILE, "utf8"));
      const concepts = Object.entries(raw.alert_types || {}).map(([id, meta]) => ({
        id,
        iri: meta.iri,
        label: meta.label,
        emodnet_hint: meta.emodnet_hint,
      }));
      const rules = Object.entries(raw.rulebook_refs || {}).map(([id, meta]) => ({
        id,
        ...meta,
      }));
      return {
        ok: true,
        ontology: {
          version: raw.version,
          namespace: raw.namespace,
          concepts,
          rules,
          semantica: {
            kg_seed_path: ONTOLOGY_FILE,
            mcp_hints: [
              "record_decision / query_decisions (Semantica MCP)",
              "export_graph from baltic_ontology.json",
              "SHACL: decision_record.ttl",
            ],
          },
        },
      };
    }
  } catch (e) {
    return { ok: false, error: String(e.message || e) };
  }
  return { ok: false, error: "Ontology file not found" };
}

export function readBusinessRulesSummary() {
  try {
    if (!fs.existsSync(BUSINESS_RULES_FILE) || !fs.existsSync(TRINO_MAPPING_FILE)) {
      return { ok: false, error: "Business rules or Trino mapping file missing" };
    }
    const br = JSON.parse(fs.readFileSync(BUSINESS_RULES_FILE, "utf8"));
    const tm = JSON.parse(fs.readFileSync(TRINO_MAPPING_FILE, "utf8"));
    const rules = (br.business_rules || []).map((r) => ({
      rule_id: r.rule_id,
      title: r.title,
      source_ids: r.source_ids,
      ontology_alert_types: r.ontology_alert_types,
      playbook_rule_id: r.playbook_rule_id,
      tables: r.trino_bindings?.tables,
      entities: r.trino_bindings?.entities,
      evidence_template: r.trino_bindings?.evidence_template,
    }));
    return {
      ok: true,
      version: br.version,
      classification_note: br.classification_note,
      nato_public_sources: br.nato_public_sources,
      business_rules: rules,
      trino_mapping: {
        version: tm.version,
        warehouse: tm.warehouse,
        semantic_entities: (tm.semantic_entities || []).map((e) => ({
          entity_id: e.entity_id,
          iri: e.iri,
          tables: e.tables,
          api_routes: e.api_routes,
        })),
        alert_type_bindings: tm.alert_type_bindings,
      },
    };
  } catch (e) {
    return { ok: false, error: String(e.message || e) };
  }
}

export const AGENT_APPS = [
  {
    app_id: "lage-analyst",
    title: "Lage-Analyst",
    subtitle: "Ostsee-Lage, Alerts, Ontologie-Begriffe",
    starter_prompt: "Was bedeutet ais_pipeline_blackout im Ontologie-Kontext?",
  },
  {
    app_id: "coa-advisor",
    title: "COA-Berater",
    subtitle: "Handlungsoptionen, Playbook, OPSEC",
    starter_prompt: "Welche OPSEC-Regeln gelten bei GPS jamming?",
  },
  {
    app_id: "semantica-curator",
    title: "Semantica-Kurator",
    subtitle: "KG, SHACL, MCP-Brücke",
    starter_prompt: "Wie spiegle ich einen NBA-Freigabe-Entschluss im Semantica KG?",
  },
];

export async function postAgentChat(body) {
  const app_id = String(body?.app_id || "lage-analyst");
  const messages = Array.isArray(body?.messages) ? body.messages : [];
  const last = messages[messages.length - 1];
  if (!last || last.role !== "user") {
    return { ok: false, error: "Last message must be user role" };
  }
  return runBridge({ action: "chat", app_id, messages });
}