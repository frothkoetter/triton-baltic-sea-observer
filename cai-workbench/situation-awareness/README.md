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

Each run writes `artifacts/reports/nato_sitrep_<timestamp>.pdf` using the NATO maritime format: classification banner, DTG, precedence, summary table, and numbered sections. Companion PNGs include a **German Baltic harbour map** and a **data visualization** composite (source mix, harbour counts, severity, deltas or source×harbour). Visual reference: [`templates/nato_sitrep_reference.pdf`](templates/nato_sitrep_reference.pdf).

Optional overrides: `LAGEBILD_CLASSIFICATION`, `LAGEBILD_THEATRE`, `LAGEBILD_PRECEDENCE`, `LAGEBILD_PRECEDENCE_ESCALATED` (defaults to `FLASH / IMMEDIATE` when comparison contains `ESCALATED`).

## Data sources

| Engine | View |
|--------|------|
| Impala (legacy) | `defense.lagebild` |
| Trino / Iceberg | `iceberg.defense.lagebild` |

Default SQL filters **German** harbours via `defense.baltic_sea_harbours`. Override with `LAGEBILD_SQL`.

Offline sample: `cai-workbench/data/lagebild/sample_lagebild.csv`.

## Environment variables

See [`.env.example`](.env.example). Key settings:

| Variable | Purpose |
|----------|---------|
| `LAGEBILD_CONNECTION_NAME` | CML data connection |
| `LAGEBILD_TRINO_CATALOG` | `iceberg` for Trino; empty for Impala names |
| `LAGEBILD_SQL` | Optional query override |
| `LAGEBILD_LLM_BASE_URL` | Model serving OpenAI-compatible base URL |
| `LAGEBILD_MODEL_ID` | Default Mistral 7B instruct |
| `LAGEBILD_API_KEY` / `LAGEBILD_JWT_PATH` | Auth |
| `LAGEBILD_MAX_CHAR_LIMIT` | Markdown table char budget (~32k) |
| `LAGEBILD_INSERT_SQL` | Gate bulk insert |

Reports land in `artifacts/reports/` (`latest.pdf`, `index.json`).

## Tests

```bash
cd cai-workbench/situation-awareness
pip install -r requirements.txt
python -m unittest discover -s tests -p 'test_*.py'
```

## Layout

```
situation-awareness/
  lagebild.py          # run_pipeline()
  run_hourly.py        # CML job entry
  lagebild.ipynb       # Demo notebook
  src/                 # data_access, prompts, llm_client, report_pdf, compare_reports, config
  tests/
```

Architecture: see project docs `situation-awareness-architecture.md` in Cursor Project store.
