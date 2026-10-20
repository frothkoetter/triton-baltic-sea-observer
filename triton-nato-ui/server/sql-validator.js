const FORBIDDEN =
  /\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|REPLACE|MERGE|UPSERT|CALL|EXEC(?:UTE)?|LOAD|SET\s+ROLE)\b/i;

const ALLOWED_START =
  /^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN)\b/i;

export function validateReadOnlySql(sql) {
  if (!sql || typeof sql !== "string") {
    return { ok: false, error: "Empty query" };
  }
  const trimmed = sql.trim();
  if (!trimmed) {
    return { ok: false, error: "Empty query" };
  }
  if (trimmed.includes(";")) {
    const parts = trimmed.split(";").map((p) => p.trim()).filter(Boolean);
    if (parts.length > 1) {
      return { ok: false, error: "Multi-statement batches are not allowed" };
    }
  }
  const normalized = trimmed.replace(/;+\s*$/, "");
  if (!ALLOWED_START.test(normalized)) {
    return {
      ok: false,
      error: "Only SELECT, WITH, SHOW, DESCRIBE, and EXPLAIN are allowed",
    };
  }
  if (FORBIDDEN.test(normalized)) {
    return { ok: false, error: "Write or DDL operations are blocked" };
  }
  return { ok: true, sql: normalized };
}
