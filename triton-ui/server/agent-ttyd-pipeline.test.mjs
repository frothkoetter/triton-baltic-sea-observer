import test from "node:test";
import assert from "node:assert/strict";
import { runTtydAsk } from "./agent-ttyd-pipeline.js";

test("full pipeline fishing kiel mock", async () => {
  const mock = async () => ({
    ok: true,
    source: "mock",
    rows: [{ ship_name: "FV Test" }],
    columns: ["ship_name"],
  });
  const out = await runTtydAsk({
    text: "fishing vessels in vicinity of kiel port",
    queryWarehouse: mock,
  });
  assert.equal(out.ok, true);
  assert.equal(out.agents.ontology_mapper.example_id, "gq_fishing_kiel");
  assert.equal(out.agents.sql_executor.row_count, 1);
});