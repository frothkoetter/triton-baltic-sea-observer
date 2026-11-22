# Local CDW Trino DDL (`setup_cdw_trino_local.sh`)

## Symptom: every query returns `error 404` (even `SELECT version()`)

That is **not** missing table/schema metadata. The Python client never reached a live Trino coordinator.

### Checklist

1. **Virtual Warehouse** (`cdw-triton` or yours) is **Running** (not Stopped/Suspended).
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

### After connectivity works

- `TRINO_CONNECT_SCHEMA=defense`
- `TRINO_SKIP_CREATE_SCHEMA=1` after creating schema **defense** on catalog **iceberg** in DW UI
- Greenfield: `TRINO_SKIP_DROP=1` (default in setup script)