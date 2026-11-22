# Local CDW Trino DDL (`setup_cdw_trino_local.sh`)

One shot: **all defense Iceberg tables**, **NBA tables**, and **views** (`lagebild`, `vessel_ais_route_geojson`, …). Supplemental manifest: `sql/trino_supplemental_ddl.order`.

**Full day-one checklist (CDW DDL + CML UI):** [TRINO_SETUP_RUNBOOK.md](./TRINO_SETUP_RUNBOOK.md)

## Symptom: every query returns `error 404` (even `SELECT version()`)

That is **not** missing table/schema metadata. The Python client never reached a live Trino coordinator.

### Checklist

1. **Virtual Warehouse** is **Running** (not Stopped/Suspended). Use host **`triton-trino.…cloudera.site`** (Copy Trino URL), not the DW overview hostname `cdw-triton.…`.
2. In CDW UI: **⋮ → Copy Trino URL** (HTTPS `https://host:443`). Put in `.env.trino.local`:
   ```bash
   TRINO_SERVER='https://YOUR-COORDINATOR.cloudera.site:443'
   ```
   Do **not** use a generic example host from git if your environment hash differs.
3. **JDBC** can stay for docs/tools:
   ```bash
   TRINO_JDBC_URL=jdbc:trino://SAME-HOST:443/iceberg/defense
   ```
4. **Doctor** before DDL:
   ```bash
   set -a && source .env.trino.local && set +a
   python3 trino_cdw_doctor.py
   ```
5. **CLI sanity** (optional, same Copy Trino URL):
   ```bash
   ./trino --server 'https://YOUR-HOST:443' --user YOU --password \
     --catalog iceberg --schema defense
   ```
   Then `SELECT version();`

If HTTP probe finds a non-default path, doctor sets `TRINO_STATEMENT_PATH` automatically.

## Symptom: `error 401` / `Access Denied: Invalid credentials`

The coordinator host is correct. Fix **CDP / Data Warehouse login** in `.env.trino.local`:

- Same password as the Cloudera UI (not CML API token, not S3 keys).
- No stray quotes or spaces in `TRINO_PASSWORD`.
- Try corporate email as `TRINO_USER` if short names fail.
- Confirm DW query role on the virtual warehouse.

`python3 trino_cdw_doctor.py` exit code **3** = auth failure.

## Git pull blocked

```bash
git stash push -m local -p scripts/cml/triton-project.env.example
# or: git checkout -- scripts/cml/triton-project.env.example
git pull origin main
```

### After connectivity works

- `TRINO_CONNECT_SCHEMA=defense`
- `TRINO_SKIP_CREATE_SCHEMA=1` after creating schema **defense** on catalog **iceberg** in DW UI
- Greenfield: `TRINO_SKIP_DROP=1` (default in setup script)