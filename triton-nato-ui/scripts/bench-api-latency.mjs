#!/usr/bin/env node
/** Measure Express API latency (used by Triton UI). */
const BASE = process.env.BENCH_BASE || "http://127.0.0.1:3001";

const ENDPOINTS = [
  { name: "health", path: "/api/health" },
  { name: "kpis", path: "/api/kpis", group: "lagebild" },
  { name: "ais", path: "/api/ais", group: "lagebild" },
  { name: "observation-areas", path: "/api/observation-areas", group: "lagebild" },
  { name: "ostsee-infrastructure", path: "/api/ostsee-infrastructure", group: "lagebild" },
  { name: "vessel-outliers", path: "/api/vessel-outliers", group: "lagebild" },
  { name: "fleet-positions", path: "/api/fleet-positions", group: "lagebild" },
  { name: "nba-incidents", path: "/api/nba/incidents", group: "lagebild" },
  { name: "position-tracks", path: "/api/position-tracks?hours=24", group: "lagebild-slow" },
  { name: "buoy-analytics", path: "/api/analyses/buoy-analytics", group: "analysen" },
  { name: "rus-fleet-analytics", path: "/api/analyses/rus-fleet-analytics", group: "analysen" },
  { name: "ais-analytics", path: "/api/analyses/ais-analytics", group: "analysen" },
  { name: "restricted-analytics", path: "/api/analyses/restricted-analytics", group: "analysen" },
  { name: "lidar-scans", path: "/api/lidar/scans", group: "lidar" },
  { name: "sitrep-reports", path: "/api/sitrep/reports", group: "sitrep" },
  { name: "proximity-alerts", path: "/api/proximity-alerts", group: "proximity" },
];

async function timeOne(path) {
  const t0 = performance.now();
  const res = await fetch(BASE + path);
  const body = await res.text();
  const ms = performance.now() - t0;
  let source = "";
  let rows = "";
  let fallback = "";
  try {
    const j = JSON.parse(body);
    source = j.source ?? j.dataSource ?? "";
    if (Array.isArray(j.rows)) rows = String(j.rows.length);
    else if (j.infrastruktur) rows = `infra:${(j.infrastruktur || []).length}`;
    else if (j.fallbackReason) fallback = j.fallbackReason.slice(0, 60);
  } catch {
  }
  return { ms, ok: res.ok, status: res.status, source, rows, fallback };
}

async function main() {
  const runs = Number(process.env.BENCH_RUNS || 3);
  console.log(`BASE=${BASE} runs=${runs} at ${new Date().toISOString()}`);

  const results = [];
  for (const ep of ENDPOINTS) {
    const samples = [];
    let meta = {};
    for (let i = 0; i < runs; i++) {
      const r = await timeOne(ep.path);
      samples.push(r.ms);
      meta = r;
    }
    samples.sort((a, b) => a - b);
    const med = samples[Math.floor(samples.length / 2)];
    const p95 = samples[samples.length - 1];
    results.push({
      ...ep,
      medianMs: Math.round(med),
      p95Ms: Math.round(p95),
      minMs: Math.round(samples[0]),
      maxMs: Math.round(samples[samples.length - 1]),
      status: meta.status,
      source: meta.source,
      rows: meta.rows,
      fallback: meta.fallback,
    });
  }

  console.log("\n| Endpoint | Group | median ms | p95 ms | source | rows |");
  console.log("|----------|-------|-----------|--------|--------|------|");
  for (const r of results) {
    console.log(
      `| ${r.name} | ${r.group || "-"} | ${r.medianMs} | ${r.p95Ms} | ${r.source || "?"} | ${r.rows || "-"} |`
    );
  }

  const lagebild = results.filter((r) => r.group === "lagebild");
  const wall = lagebild.reduce((s, r) => s + r.medianMs, 0);
  console.log(`\nLagebild parallel bundle (7 calls): sum median = ${wall} ms (UI uses Promise.all → wall ≈ max, not sum)`);
  const slowest = [...lagebild].sort((a, b) => b.medianMs - a.medianMs)[0];
  console.log(`Lagebild critical path (max median): ${slowest?.name} ≈ ${slowest?.medianMs} ms`);

  // simulate parallel wall clock
  const parallel = await (async () => {
    const paths = lagebild.map((x) => x.path);
    const t0 = performance.now();
    await Promise.all(paths.map((p) => timeOne(p)));
    return Math.round(performance.now() - t0);
  })();
  console.log(`Lagebild measured parallel wall (1 run): ${parallel} ms`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});