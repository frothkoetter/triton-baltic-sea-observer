# CML: CrewAI / MCP install vs MLflow (protobuf conflict)

## What your log means

| Line | Meaning |
|------|---------|
| `Uninstalled mcp-2.3.0` → `mcp-1.30.0` | **CrewAI 1.15** depends on MCP **1.x**, not 2.x (e.g. Cursor / newer MCP servers). |
| `Successfully installed crewai-1.15.44 …` | Install **finished** — pip’s message is a **warning**, not a failed install. |
| `protobuf 7.34.0` vs **mlflow-cml-plugin** `protobuf==4.25.3` | **Incompatible** with Cloudera’s MLflow CML plugin on the **same** Python engine. |
| `mlflow-skinny` wants `protobuf<6` | Also broken with **7.34.0**. |

Root cause: CrewAI 1.15 pulls **OpenTelemetry / grpc** stacks that upgrade **protobuf** to 7.x. CML’s preinstalled **mlflow-cml-plugin** is pinned to **protobuf 4.25.3**.

---

## Recommended on CML (Triton showcase)

| Workload | Install | Agent / NBA mode |
|----------|---------|------------------|
| **Scheduled NBA job** | `pip install -r requirements.txt` only | No CrewAI — Phase 7 with `--skip-llm` |
| **NATO UI Application** | **No** `requirements-crewai.txt` on the app engine | `TRITON_AGENT_MODE=llm` or `offline` + `LAGEBILD_*` / `NBA_*` LLM env |
| **Experiments with CrewAI** | Isolated venv (see below) | Not on the same kernel as MLflow tracking |

Triton does **not** require CrewAI in production CML: agent chat falls back to **LLM** or **offline** templates.

---

## If you still want CrewAI on CML

### Option A — Isolated venv (safest)

```bash
cd /home/cdsw/cai-workbench/next-best-action
python3 -m venv .venv-crewai
source .venv-crewai/bin/activate
pip install -U pip
pip install -r requirements.txt
pip install 'crewai>=0.86,<1.16'   # or pin exactly what you tested
# Run only from this venv:
.venv-crewai/bin/python3 ../agent-console/chat_bridge.py
```

Point a **custom Application start command** or job at that interpreter only if you accept **no** shared MLflow UI on that process.

### Option B — Downgrade protobuf (risky, may break CrewAI)

```bash
pip install 'protobuf>=4.25.3,<6' 'typing-extensions==4.10.0'
```

Only if you must keep one global engine; **re-test** CrewAI and `mlflow` logging afterward.

### Option C — Older CrewAI line

`crewai>=0.86.0` in repo was written for lighter deps. Avoid blind `pip install crewai` (pulls 1.15.x today). Pin e.g.:

```bash
pip install 'crewai==0.86.0'
```

Still may conflict — prefer Option A.

---

## MCP 1.x vs 2.x

| Package | Typical use |
|---------|----------------|
| **mcp 2.3** | Cursor IDE, newer MCP servers |
| **mcp 1.30** | CrewAI 1.15 internal tooling |

Do not expect **one** project venv to host both without conflicts. Keep **Cursor MCP** on your laptop; CML runs **JDBC / optional requirements-mcp** for NBA Iceberg.

---

## Verify after install

```bash
python3 -c "import mlflow; print('mlflow ok')"
python3 -c "import crewai; print('crewai ok')"   # only if you need it
cd cai-workbench/agent-console && TRITON_AGENT_MODE=offline python3 -m unittest tests.test_agent_offline -v
```

For NATO UI without CrewAI:

```bash
export TRITON_AGENT_MODE=llm
# LAGEBILD_LLM_BASE_URL, LAGEBILD_API_KEY, ...
```

---

## `crewai run` / `gpt-6-luna` / `OPENAI_API_KEY`

This is **not** the Triton NBA or NATO UI path. The CLI `crewai run` uses a **separate** CrewAI project (often with a template `.env` and `YOUR_API_KEY`).

| Symptom | Fix for Triton |
|---------|----------------|
| `OPENAI_API_KEY … YOUR_API_KEY` | Do **not** use `crewai run` for the showcase. |
| NATO Agenten-Tab | `TRITON_AGENT_MODE=llm` + existing **`LAGEBILD_LLM_BASE_URL`**, **`LAGEBILD_API_KEY`** (Azure/Mistral). |
| NBA Job | `NBA_SKIP_LLM=1` or same Lagebild LLM env — no CrewAI CLI. |
| Agent bridge smoke | `TRITON_AGENT_MODE=offline` or `llm`. |

If you **intentionally** use `crewai run` (own experiment): set a real `OPENAI_API_KEY` in that project's `.env`, or point the CrewAI project LLM to Azure/OpenAI-compatible URL per CrewAI docs — unrelated to `cml_job.py` / `run_nba.py`.

---

## Summary

- Your install **succeeded**; the **dependency conflict** is real for **MLflow on CML**.
- **Do not** install `requirements-crewai.txt` into the **default CML project engine** used for MLflow jobs.
- Use **`TRITON_AGENT_MODE=llm`** for live agent answers, or an **isolated venv** for CrewAI experiments.
- **Do not** run `crewai run` for Triton — use `python3 cml_job.py`, `run_nba.py`, or NATO UI with `TRITON_AGENT_MODE`.