# Situation awareness (Lagebild)

Modular Cloudera AI Workbench project for **Baltic maritime situation pictures**: query `defense.lagebild`, summarise for LLM context, generate Mistral reports, PDF artifacts, and 24h/48h comparisons.

## Quickstart (notebook)

```bash
cd cai-workbench/situation-awareness
./cdsw-build.sh
export LAGEBILD_USE_SAMPLE=1   # offline without CML connection
jupyter notebook lagebild.ipynb
```

With a live cluster: set `LAGEBILD_CONNECTION_NAME` to your **Trino** or legacy **Impala** data connection, configure `LAGEBILD_LLM_BASE_URL` + `LAGEBILD_API_KEY` (or JWT at `LAGEBILD_JWT_PATH`), then run the notebook without `LAGEBILD_USE_SAMPLE`.

## Hourly job (CML / CDSW)

1. Create a **Scheduled Job** pointing at this directory.
2. **Build command:** `bash cdsw-build.sh`
3. **Job command:** `python run_hourly.py`
4. Schedule: hourly (cron `0 * * * *`).

Optional flags: `python run_hourly.py --use-sample` (offline), `python run_hourly.py --skip-llm` (PDF shell without model).

Enable SQL persistence: `LAGEBILD_INSERT_SQL=1` (writes to `defense.Situation_Awareness_Summary`).

## PDF (NATO SITREP layout)

Each run writes `artifacts/reports/nato_sitrep_<timestamp>.pdf` using the NATO maritime format: classification banner, DTG, precedence, summary table, and numbered sections. Companion PNGs include a **German Baltic harbour map** (land/sea basemap via **cartopy** when installed, otherwise bundled Natural Earth 110m coast GeoJSON under `data/`), a **data visualization** composite (source mix, harbour counts, severity, deltas or source×harbour), and a **Zebra-style analytics** page (source-mix radar vs prior run plus harbour ΔPY variance). Visual reference: [`templates/nato_sitrep_reference.pdf`](templates/nato_sitrep_reference.pdf).

**Live Trino column shapes:** `src/lagebild_viz_prep.normalize_lagebild_frame` standardizes harbour/source/timestamp names and infers **severity** from `details` text when Trino omits a severity column. Source labels are rolled up for pies/radar (tail → **Other**; small **Social Media** slices may fold into **Other**). On a **first run** with comparison text that is not delta-shaped, chart panel 4 falls back to **Events by data source (stacked)** instead of an empty delta panel.

Optional overrides: `LAGEBILD_CLASSIFICATION`, `LAGEBILD_THEATRE`, `LAGEBILD_PRECEDENCE`, `LAGEBILD_PRECEDENCE_ESCALATED` (defaults to `FLASH / IMMEDIATE` when comparison contains `ESCALATED`).

## Data sources

| Engine | View |
|--------|------|
| Impala (legacy) | `defense.lagebild` |
| Trino / Iceberg | `iceberg.defense.lagebild` |

Default SQL loads **all Baltic harbours** from `defense.baltic_sea_harbours` plus offshore rows (`harbour_name IS NULL`) and explicit feeds (`GPS_Jammer`, `REF` area violations, `RUS_Fleet`). Set `LAGEBILD_GERMAN_ONLY=1` to restore the former German-only harbour filter. Override entirely with `LAGEBILD_SQL`.

Offline sample: `cai-workbench/data/lagebild/sample_lagebild.csv`.

## Environment variables

See [`.env.example`](.env.example). Key settings:

| Variable | Purpose |
|----------|---------|
| `LAGEBILD_CONNECTION_NAME` | CML data connection |
| `LAGEBILD_TRINO_CATALOG` | `iceberg` for Trino; empty for Impala names |
| `LAGEBILD_SQL` | Optional query override |
| `LAGEBILD_GERMAN_ONLY` | `1` = German harbours only (legacy default SQL) |
| `LAGEBILD_LLM_BASE_URL` | Model serving OpenAI-compatible base URL |
| `LAGEBILD_MODEL_ID` | Default Mistral 7B instruct |
| `LAGEBILD_API_KEY` / `LAGEBILD_JWT_PATH` | Auth |
| `LAGEBILD_MAX_CHAR_LIMIT` | Markdown table char budget (~32k) |
| `LAGEBILD_INSERT_SQL` | Gate bulk insert |

Reports land in `artifacts/reports/` (`latest.pdf`, `index.json`).

## Tests

```bash
cd cai-workbench/situation-awareness
pip install -r requirements.txt   # includes cartopy for geographic harbour map; works without network at render time
python -m unittest discover -s tests -p 'test_*.py'
```

## Layout

```
situation-awareness/
  lagebild.py          # run_pipeline()
  run_hourly.py        # CML job entry
  lagebild.ipynb       # Demo notebook
  src/                 # data_access, prompts, llm_client, report_pdf, lagebild_viz_prep, lagebild_zebra, config
  tests/
```

Architecture: see project docs `situation-awareness-architecture.md` in Cursor Project store.
