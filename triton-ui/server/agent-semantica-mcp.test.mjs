import test from "node:test";
import assert from "node:assert/strict";
import {
  getSemanticaGraphSummary,
  snapshotKgFile,
  semanticaEnvPaths,
} from "./agent-semantica-mcp.js";

test("semantica paths point at defense bundle", () => {
  const p = semanticaEnvPaths();
  assert.match(p.SEMANTICA_KG_PATH, /triton_defense_graph\.json$/);
  assert.match(p.SEMANTICA_MAPPING_CONFIG, /defense_r2rml_db_mapping\.yaml$/);
});

test("kg snapshot has ontology classes", () => {
  const p = semanticaEnvPaths();
  const snap = snapshotKgFile(p.SEMANTICA_KG_PATH);
  assert.ok(snap.ontology_class_count >= 10);
  assert.ok(snap.database_table_count >= 5);
});

test("graph summary fallback ok", () => {
  const s = getSemanticaGraphSummary();
  assert.equal(s.ok, true);
  assert.ok(s.node_count > 40);
});