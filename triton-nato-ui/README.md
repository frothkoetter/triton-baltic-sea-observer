# Triton NATO UI (MVP)

React + Vite front end with an Express API and Python Impala bridge for the **defense** schema (Baltic Sea Observer).

## Quick start

```bash
cd triton-nato-ui
cp .env.example .env.local
# Edit .env.local — set IMPALA_USER and IMPALA_PASSWORD (never commit this file)
npm install
npm run dev
```

Open http://localhost:5173 (API on http://127.0.0.1:3001).

## Environment

See `.env.example`. Credentials belong only in `.env.local`.

| Variable | Purpose |
|----------|---------|
| `IMPALA_HOST` | CDW Impala coordinator hostname |
| `IMPALA_PORT` | Usually `443` for HTTPS transport |
| `IMPALA_DATABASE` | `defense` |
| `IMPALA_USER` / `IMPALA_PASSWORD` | LDAP or workload credentials |
| `IMPALA_AUTH_MECH` | `LDAP` (typical for AuthMech=12 workloads) |
| `IMPALA_HTTP_PATH` | `cliservice` |
| `TRITON_FORCE_SAMPLE_DATA` | `true` to skip Impala and use embedded demo data |
| `PROXIMITY_ALERT_API_KEY` | SSB MVE key for Schiffsnähe (`GET /api/proximity-alerts`) |

## Impala notes

JDBC reference used in CDV:

`jdbc:impala://<host>:443/default;AuthMech=12;transportMode=http;httpPath=cliservice;ssl=1;auth=browser`

The MVP maps this to `impyla` with `use_http_transport=true`. Browser OAuth cannot run inside the API; use username/password in `.env.local` when your CDW tenant allows LDAP/PLAIN for programmatic access.

If Impala is unreachable (503, VPN, or auth failure), map and analysis panels automatically use **sample data**; the status bar shows `SAMPLE DATA`.

## SQL workspace

Read-only: `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN` only. DDL/DML is rejected server-side.

## Production

```bash
npm run build
NODE_ENV=production npm start
```

Serves the built SPA from `dist/` on `API_PORT`.
