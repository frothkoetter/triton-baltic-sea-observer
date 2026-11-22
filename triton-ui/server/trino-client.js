import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(__dirname, "trino-bridge.py");

export const TRINO_CATALOG = process.env.TRINO_CATALOG || "iceberg";
export const TRINO_SCHEMA = process.env.TRINO_SCHEMA || "defense";
export const TRINO_FQ = `${TRINO_CATALOG}.${TRINO_SCHEMA}`;

let trinoState = {
  checked: false,
  live: false,
  lastError: null,
};

export function trinoConfigured() {
  return Boolean(
    String(process.env.TRINO_USER ?? "").trim() &&
      String(process.env.TRINO_PASSWORD ?? "").trim()
  );
}

/** Surface JDBC/host mistakes (cdw-triton vs triton-trino) in /api/health. */
export function trinoEnvDiagnostics() {
  const jdbc = String(process.env.TRINO_JDBC_URL ?? "").trim();
  const jdbcMatch = jdbc.match(/^jdbc:trino:\/\/([^:/?#]+)/i);
  const jdbcHost = jdbcMatch?.[1] ?? null;
  const lagHost = String(process.env.LAGEBILD_TRINO_HOST ?? "").trim();
  const host = String(process.env.TRINO_HOST ?? "").trim() || jdbcHost || lagHost || null;
  const wrong =
    /^cdw-triton\./i.test(host ?? "") ||
    /^cdw-triton\./i.test(jdbcHost ?? "") ||
    /^cdw-triton\./i.test(lagHost);
  return {
    jdbcHost,
    lagHost: lagHost || null,
    resolvedHost: host,
    likelyWrongCoordinator: wrong,
    fixHint: wrong
      ? "Set TRINO_JDBC_URL and LAGEBILD_TRINO_HOST to triton-trino.* (CDW Copy Trino URL), not cdw-triton.*, then restart Application."
      : null,
  };
}

export function getTrinoState() {
  return { ...trinoState, configured: trinoConfigured() };
}

/** Rewrite Impala-style `defense.*` SQL for Iceberg Trino. */
export function qualifyDefenseSql(sql) {
  let out = sql.replace(/\bdefense\./g, `${TRINO_FQ}.`);
  out = out.replace(/\bSHOW TABLES IN defense\b/i, `SHOW TABLES FROM ${TRINO_FQ}`);
  return out;
}

export function runTrino(sql, mode = "query") {
  return new Promise((resolve, reject) => {
    const child = spawn("python3", [BRIDGE, sql, mode], {
      env: { ...process.env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => {
      out += d.toString();
    });
    child.stderr.on("data", (d) => {
      err += d.toString();
    });
    child.on("close", (code) => {
      try {
        const parsed = JSON.parse(out || "{}");
        trinoState.checked = true;
        if (parsed.ok) {
          trinoState.live = true;
          trinoState.lastError = null;
        } else {
          trinoState.live = false;
          trinoState.lastError = parsed.error || err;
        }
        if (code !== 0 && !parsed.error) {
          parsed.error = err || `Bridge exited ${code}`;
        }
        if (parsed.ok && !parsed.source) {
          parsed.source = "trino";
        }
        resolve(parsed);
      } catch {
        reject(new Error(err || out || "Invalid Trino bridge response"));
      }
    });
  });
}