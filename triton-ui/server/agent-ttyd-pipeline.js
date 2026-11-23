/**
 * 3-agent TTYD pipeline: Mapper → Analytic → Synthesizer (deterministic orchestrator).
 */

import {
  resolveGoldenExample,
  buildMappingPlan,
  kgTablesForClasses,
  compareKgVsPlan,
} from "./agent-ttyd.js";
import { getKgSqlQuery } from "./agent-kg.js";
import { semanticMappingForClasses, validateSqlMapping } from "./agent-ontology-schema.js";

function agentMapper(text, example) {
  const resolved = example ? { ok: true, example, score: 99 } : resolveGoldenExample(text);
  if (!resolved.ok) {
    return {
      agent: "ontology_mapper",
      ok: false,
      error: resolved.error,
      score: resolved.score ?? 0,
    };
  }
  const ex = resolved.example;
  const mappingPlan = buildMappingPlan(ex);
  return {
    agent: "ontology_mapper",
    ok: true,
    example_id: ex.id,
    score: resolved.score,
    kdm_classes: ex.classes,
    mapping_plan: mappingPlan,
    warehouse: mappingPlan.warehouse,
    kg_query_id: ex.kg_query_id,
    reasoning_de: `Frage → Golden Example „${ex.id}“; Tabellen: ${[...mappingPlan.tables, ...mappingPlan.views].join(", ")}`,
  };
}

async function agentAnalytic(mapperOut, queryWarehouse) {
  if (!mapperOut.ok) return { agent: "sql_executor", ok: false, error: "mapper failed" };
  const resolved = getKgSqlQuery(mapperOut.kg_query_id);
  if (!resolved.ok) {
    return { agent: "sql_executor", ok: false, error: resolved.error };
  }
  const started = Date.now();
  const sqlResult = await queryWarehouse(resolved.sql);
  return {
    agent: "sql_executor",
    ok: sqlResult.ok,
    elapsed_ms: Date.now() - started,
    query_id: mapperOut.kg_query_id,
    label: resolved.query.label,
    sql: resolved.sql,
    source: sqlResult.source,
    columns: sqlResult.columns,
    row_count: sqlResult.rows?.length ?? 0,
    rows: (sqlResult.rows ?? []).slice(0, 20),
    error: sqlResult.error ?? null,
  };
}

function agentSynthesizer(text, mapperOut, analyticOut, kgCompare) {
  const artifacts = [
    "cai-workbench/next-best-action/data/kdm_defense_classes.json",
    "cai-workbench/next-best-action/data/baltic_trino_mapping.json",
    "cai-workbench/next-best-action/data/baltic_ontology.json",
  ];
  if (!mapperOut.ok) {
    return {
      agent: "answer_synthesizer",
      ok: false,
      summary_de: `Keine Ontologie-Zuordnung für: „${text}“. Nutzen Sie einen der Golden-Prompts oder #agents TTYD-Buttons.`,
      markdown_de: "",
      map_layers: [],
      artifacts,
      gaps: ["intent_unmapped"],
    };
  }
  const rows = analyticOut.row_count ?? 0;
  const src = analyticOut.source ?? "—";
  let md = `## Talk to your Data\n\n**Frage:** ${text}\n\n`;
  md += `**Ontologie:** ${mapperOut.kdm_classes?.join(", ")}\n\n`;
  if (analyticOut.ok) {
    md += `**Evidence:** ${rows} Zeilen (${src}), Query \`${mapperOut.kg_query_id}\`.\n\n`;
    if (analyticOut.rows?.length) {
      md += "| Zeile (JSON) |\n|---|\n";
      for (const row of analyticOut.rows.slice(0, 8)) {
        md += `| ${JSON.stringify(row)} |\n`;
      }
    }
  } else {
    md += `**SQL:** fehlgeschlagen — ${analyticOut.error ?? "unknown"}\n`;
  }
  md += `\n**KG↔Plan overlap:** ${kgCompare?.overlap_count ?? 0}\n`;
  const gaps = [...(kgCompare?.tables_plan_only ?? []).map((t) => `KG-Kante fehlt: ${t}`)];
  return {
    agent: "answer_synthesizer",
    ok: true,
    summary_de: analyticOut.ok
      ? `${mapperOut.mapping_plan.description_de}: ${rows} Zeilen (${src}).`
      : `Mapping OK, SQL fehlgeschlagen.`,
    markdown_de: md,
    tactical_stub: analyticOut.ok
      ? { dtg: "AUTO", subject: "TTYD EVIDENCE", ref: mapperOut.kg_query_id }
      : null,
    map_layers: analyticOut.ok
      ? [{ layer_id: "ttyd-evidence", query_id: mapperOut.kg_query_id, hint: "Overlay aus SQL-Ergebnis" }]
      : [],
    artifacts,
    gaps,
    nba_hook: mapperOut.example_id?.includes("sanction")
      ? "/nba-hitl#sanctions"
      : mapperOut.example_id === "gq_gps_jamming"
        ? "/lagebild#gps"
        : null,
  };
}

export async function runTtydAsk({ text, queryWarehouse }) {
  const trimmed = String(text ?? "").trim();
  if (!trimmed) return { ok: false, error: "text required" };

  const mapper = agentMapper(trimmed, null);
  const kgTables = mapper.ok ? kgTablesForClasses(mapper.kdm_classes ?? []) : [];
  const kgCompare = mapper.ok
    ? compareKgVsPlan(mapper.mapping_plan, kgTables)
    : { overlap_count: 0, tables_in_both: [], tables_plan_only: [] };

  const ontology_bindings = mapper.ok
    ? semanticMappingForClasses(mapper.kdm_classes)
    : [];
  const analytic = await agentAnalytic(mapper, queryWarehouse);
  const sql_validation =
    mapper.ok && analytic.sql
      ? validateSqlMapping(analytic.sql, mapper.mapping_plan)
      : { ok: false, warnings: ["no sql"] };
  const synthesizer = agentSynthesizer(trimmed, mapper, analytic, kgCompare);

  return {
    ok: mapper.ok && synthesizer.ok,
    schema: "triton-ttyd-ask/2",
    user_text: trimmed,
    ontology_bindings,
    sql_validation,
    agents: {
      ontology_mapper: mapper,
      sql_executor: analytic,
      answer_synthesizer: synthesizer,
    },
    kg_compare: kgCompare,
    reply: synthesizer.markdown_de,
    summary_de: synthesizer.summary_de,
  };
}