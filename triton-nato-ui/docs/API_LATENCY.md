# API latency analysis (live Trino)

Repeatable benchmark:

```bash
# API must be running (API_PORT=3001, TRINO_* set)
cd triton-nato-ui
BENCH_RUNS=3 node scripts/bench-api-latency.mjs
```

## Method

- **Target:** Express routes consumed by the React UI (`scripts/bench-api-latency.mjs`).
- **Metrics:** Per-endpoint **median** and **max** of 3 sequential runs; extra **parallel wall** for the seven Lagebild `Promise.all` calls (matches `Lagebild.tsx`).
- **Live stack:** `dataSource: trino` on `GET /api/health`, catalog `iceberg.defense`, host `cdw-triton` (Nov 2026 agent run).

## Results (live Trino, 2026-11-20)

| Endpoint | UI zone | Median | Max (p95) | Source | Notes |
|----------|---------|--------|-----------|--------|-------|
| `/api/health` | header | 3 ms | 30 ms | — | metadata only |
| `/api/kpis` | Lagebild KPIs | **3.8 s** | **36.7 s** | trino | 4 Trino queries; first run cold |
| `/api/ais` | Lagebild map | **5.2 s** | 5.2 s | trino | LIMIT 500 |
| `/api/fleet-positions` | Lagebild | 0.84 s | 0.85 s | trino | DE + RUS |
| `/api/vessel-outliers` | Lagebild | 0.34 s | 0.35 s | trino | 106 rows |
| `/api/observation-areas` | Lagebild | 0.29 s | 0.31 s | trino | 17 AOIs |
| `/api/ostsee-infrastructure` | Lagebild | 0.33 s | 0.33 s | trino | |
| `/api/nba/incidents` | Lagebild + NBA | 0.44 s | 0.50 s | trino | 21 rows |
| **Lagebild bundle (parallel)** | initial load | **~5.6 s wall** | — | — | dominated by **AIS** + **KPIs** |
| `/api/position-tracks?hours=24` | Lagebild (opt-in) | **7.3 s** | 7.4 s | trino | 1000 points; heavy |
| `/api/analyses/buoy-analytics` | Analysen | 0.89 s | 0.95 s | trino | |
| `/api/analyses/rus-fleet-analytics` | Analysen | 1.7 s | 1.7 s | trino | spatial + restricted |
| `/api/analyses/ais-analytics` | Analysen | **32.6 s** | **32.6 s** | trino | multi-query panel |
| `/api/analyses/restricted-analytics` | Analysen | **47.8 s** | **48.6 s** | trino | tracks + ST_Within |
| `/api/lidar/scans` | LiDAR | 0.44 s | 0.64 s | trino | metadata |
| `/api/sitrep/reports` | SITREP | 23 ms | 23 ms | not configured | no S3 prefix |
| `/api/proximity-alerts` | Schiffsnähe | 0.31 s | 0.32 s | sample/fallback | external API |

**NBA detail (warm):**

| Endpoint | Typical |
|----------|---------|
| `.../coa-runs` | ~0.38 s |
| `.../decision-record` | ~0.31 s |

## Interpretation

1. **Lagebild first paint** waits on the **slowest** of seven parallel calls → typically **`/api/ais` (~5 s)** once Trino is warm; **`/api/kpis`** can be worse on **cold** clusters (Trino coordinator + four aggregates).
2. **Route tracks** are a **second-phase** cost (~7 s) when the user enables the layer — plan UX loading state accordingly.
3. **Analysen** tabs **AIS** and **Restricted** run **large analytical SQL** (tens of seconds). Acceptable for analyst workflow; poor for “instant” tab switch — consider materialized views, tighter limits, or server-side caching.
4. Each API call spawns **`trino-bridge.py`** (process per query) — adds ~100–300 ms overhead vs a persistent JDBC pool (trade-off of the MVP bridge).
5. **NBA HITL** list + matrix + decision record are **sub-second** once incidents exist in Trino; not the bottleneck.

## Recommendations

| Priority | Action |
|----------|--------|
| High | Add loading skeleton on Lagebild until `ais` + `kpis` complete; show per-layer spinners for outliers/NBA. |
| High | Cache `/api/kpis` and `/api/ais` for 30–60 s server-side (same for all users on CML). |
| Medium | Precompute or narrow **ais-analytics** / **restricted-analytics** queries; show progressive panels. |
| Medium | Lazy-load **position-tracks** only when toggle on (already true). |
| Low | Document expected **SITREP** / **LiDAR points** latency separately (S3-bound). |

Re-run after schema changes, cluster resize, or Iceberg snapshot growth.