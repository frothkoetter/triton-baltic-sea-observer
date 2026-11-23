/**
 * Bridge to frothkoetter/semantica MCP (ontology load, graph summary, business rules → SQL).
 * Falls back to bundled KG snapshot + YAML when Python semantica is not installed.
 */

import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = process.env.TRITON_REPO_ROOT
  ? path.resolve(process.env.TRITON_REPO_ROOT)
  : path.resolve(__dirname, "..", "..");

const ONTOLOGY_DIR = path.join(REPO_ROOT, "cdw-analyse", "ontology");
const DEFAULT_PATHS = {
  kg: path.join(ONTOLOGY_DIR, "data", "triton_defense_graph.json"),
  mapping: path.join(ONTOLOGY_DIR, "defense_r2rml_db_mapping.yaml"),
  business_rules: path.join(ONTOLOGY_DIR, "defense_business_rules.yaml"),
  invoke: path.join(ONTOLOGY_DIR, "semantica_mcp_invoke.py"),
};

export function semanticaEnvPaths() {
  return {
    SEMANTICA_KG_PATH: process.env.SEMANTICA_KG_PATH || DEFAULT_PATHS.kg,
    SEMANTICA_MAPPING_CONFIG: process.env.SEMANTICA_MAPPING_CONFIG || DEFAULT_PATHS.mapping,
    SEMANTICA_BUSINESS_RULES: process.env.SEMANTICA_BUSINESS_RULES || DEFAULT_PATHS.business_rules,
    semantica_repo: "https://github.com/frothkoetter/semantica.git",
  };
}

function localName(uri) {
  const s = String(uri || "");
  if (s.includes("#")) return s.split("#").pop();
  return s.split("/").pop();
}

/** Same shape as semantica.mcp_server.kg_snapshot.snapshot_kg_file */
export function snapshotKgFile(kgPath) {
  if (!fs.existsSync(kgPath)) {
    return { graph_ready: false, error: `KG not found: ${kgPath}` };
  }
  const data = JSON.parse(fs.readFileSync(kgPath, "utf8"));
  const nodes = data.nodes || [];
  const typeCounts = {};
  for (const n of nodes) {
    const t = n.type || "entity";
    typeCounts[t] = (typeCounts[t] || 0) + 1;
  }
  const ontologyNodes = nodes.filter((n) => n.type === "OntologyClass");
  const dbTables = nodes.filter((n) => n.type === "DatabaseTable");
  const label = (n) => {
    const p = n.properties || {};
    return p.label || p.content || p.table_name || localName(p.uri || n.id);
  };
  return {
    node_count: nodes.length,
    edge_count: (data.edges || []).length,
    ontology_class_count: typeCounts.OntologyClass || 0,
    database_table_count: typeCounts.DatabaseTable || 0,
    ontology_classes: ontologyNodes.slice(0, 50).map(label),
    database_tables: dbTables.slice(0, 50).map(label),
    graph_ready: nodes.length > 0,
    source: "kg_file_snapshot",
  };
}

function parseSimpleYamlBusinessRules(yamlPath) {
  if (!fs.existsSync(yamlPath)) return null;
  const text = fs.readFileSync(yamlPath, "utf8");
  const sqlHints = [];
  let inHints = false;
  for (const line of text.split("\n")) {
    if (line.startsWith("sql_agent_hints:")) {
      inHints = true;
      continue;
    }
    if (inHints) {
      if (line.match(/^\S/) && !line.startsWith(" ")) break;
      const m = line.match(/^\s+(\w+):\s*(.*)$/);
      if (m) sqlHints.push({ key: m[1], value: m[2].trim() });
    }
  }
  return {
    rules_path: yamlPath,
    rules_path_exists: true,
    sql_agent_hints_parsed: sqlHints,
    source: "yaml_fallback",
  };
}

function invokePythonTool(tool, args = {}) {
  const script = DEFAULT_PATHS.invoke;
  if (!fs.existsSync(script)) {
    return { ok: false, error: "semantica_mcp_invoke.py missing" };
  }
  const paths = semanticaEnvPaths();
  const env = {
    ...process.env,
    SEMANTICA_KG_PATH: paths.SEMANTICA_KG_PATH,
    SEMANTICA_MAPPING_CONFIG: paths.SEMANTICA_MAPPING_CONFIG,
    SEMANTICA_BUSINESS_RULES: paths.SEMANTICA_BUSINESS_RULES,
    SEMANTICA_MCP_DISABLE_ML: process.env.SEMANTICA_MCP_DISABLE_ML || "1",
  };
  const proc = spawnSync("python3", [script, tool, JSON.stringify(args)], {
    encoding: "utf8",
    env,
    timeout: 120_000,
  });
  if (proc.error) return { ok: false, error: proc.error.message, backend: "python" };
  const stdout = (proc.stdout || "").trim();
  if (!stdout) {
    return { ok: false, error: proc.stderr || "empty stdout", backend: "python" };
  }
  try {
    const parsed = JSON.parse(stdout);
    return { ...parsed, backend: "semantica-mcp-python" };
  } catch {
    return { ok: false, error: "invalid json from semantica invoke", raw: stdout.slice(0, 500) };
  }
}

export function getSemanticaGraphSummary() {
  const paths = semanticaEnvPaths();
  const py = invokePythonTool("get_graph_summary", {});
  if (py.node_count != null || py.graph_ready != null) {
    return {
      ok: true,
      ...py,
      paths,
      tool: "get_graph_summary",
    };
  }
  const snapshot = snapshotKgFile(paths.SEMANTICA_KG_PATH);
  const mappingExists = fs.existsSync(paths.SEMANTICA_MAPPING_CONFIG);
  const rulesExists = fs.existsSync(paths.SEMANTICA_BUSINESS_RULES);
  snapshot.graph_ready_for_sql = Boolean(
    snapshot.graph_ready && snapshot.node_count >= 20 && mappingExists && rulesExists
  );
  snapshot.mapping_config_exists = mappingExists;
  snapshot.business_rules_path_exists = rulesExists;
  snapshot.python_fallback_reason = py.error;
  return {
    ok: snapshot.graph_ready,
    ...snapshot,
    paths,
    backend: "node-kg-snapshot",
    tool: "get_graph_summary",
  };
}

export function getSemanticaBusinessRules() {
  const paths = semanticaEnvPaths();
  const py = invokePythonTool("get_business_rules", {});
  if (py.sql_agent_hints || py.rules_path_exists) {
    return { ok: true, ...py, paths, tool: "get_business_rules" };
  }
  const fallback = parseSimpleYamlBusinessRules(paths.SEMANTICA_BUSINESS_RULES);
  return {
    ok: Boolean(fallback),
    ...fallback,
    paths,
    backend: "node-yaml-fallback",
    python_fallback_reason: py.error,
    tool: "get_business_rules",
  };
}

/** Attach Semantica context to TTYD mapper (MCP-style grounding). */
export function semanticaGroundingForClasses(classIds) {
  const summary = getSemanticaGraphSummary();
  const rules = getSemanticaBusinessRules();
  const hints = rules.sql_agent_hints?.summary || rules.sql_agent_hints_parsed?.[0]?.value;
  return {
    schema: "triton-semantica-mcp-bridge/1",
    graph_summary: {
      graph_ready_for_sql: summary.graph_ready_for_sql,
      ontology_class_count: summary.ontology_class_count,
      database_table_count: summary.database_table_count,
      backend: summary.backend,
    },
    sql_agent_hints: hints || null,
    queried_classes: classIds,
    mcp_tools: ["get_graph_summary", "get_business_rules", "map_db_schema_to_ontology"],
    repo: DEFAULT_PATHS.invoke ? "https://github.com/frothkoetter/semantica.git" : null,
  };
}