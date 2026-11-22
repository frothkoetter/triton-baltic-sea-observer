# Cloudera AI — project environment via API v2

Set **Project → Settings → Environment variables** programmatically ([API v2](https://docs.cloudera.com/machine-learning/cloud/api/topics/ml-api-v2.html)).

Trino host, DDL, and UI **Trino live**: [`cdw-analyse/TRINO_SETUP_RUNBOOK.md`](../../cdw-analyse/TRINO_SETUP_RUNBOOK.md).  
**UC1 / UC2 scheduled jobs**: [`cai-workbench/CML_UC_JOBS_SETUP.md`](../../cai-workbench/CML_UC_JOBS_SETUP.md).

## Prerequisites

1. **API key:** Cloudera AI → User Settings → **API Keys** → Create (Audience: **API**).
2. **Host:** workbench URL, e.g. `https://ml-xxxxxx.cloudera.site` (no trailing path).
3. **Local secrets file** (never commit):

   ```bash
   cp scripts/cml/triton-project.env.example scripts/cml/triton-project.env.local
   # edit triton-project.env.local — passwords and API keys
   ```

## Run

```bash
export CML_API_KEY='your-api-key'
export CML_HOST='https://your-workbench.cloudera.site'

python3 scripts/cml/set_project_environment.py \
  --project your-cml-project-name \
  --env-file scripts/cml/triton-project.env.local \
  --dry-run

# then without --dry-run
python3 scripts/cml/set_project_environment.py \
  --project your-cml-project-name \
  --env-file scripts/cml/triton-project.env.local
```

`searchFilter` on the API must be JSON, e.g. `{"name":"triton-lidar"}` — the script builds that from `--project`.

Optional:

- `--project-id <uuid>` if name search is ambiguous.
- `--replace` to overwrite the whole project `environment` map (default **merges** with existing).
- `--use-cmlapi` after `pip3 install $CML_HOST/api/v2/python.tar.gz`.
- `--ca-cert /path/to/ca.pem` for custom TLS (or `CML_CA_CERT`).

## curl equivalent (single key)

```bash
curl -sS -X PATCH \
  -H "Authorization: Bearer $CML_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"environment":{"TRINO_USER":"..."}}' \
  "$CML_HOST/api/v2/projects/$PROJECT_ID"
```

Prefer the Python script to **merge** many keys from `triton-project.env.local`.

## Security

- `*.local.env` and `*cml-secrets*` under this folder are **gitignored**.
- Do not paste production passwords into git, chat, or `triton-project.env.example`.
- Applications must **inherit** project environment (or set the same vars on each Application).