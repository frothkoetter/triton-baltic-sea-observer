import {
  getTrinoState,
  qualifyDefenseSql,
  runTrino,
  trinoConfigured,
} from "./trino-client.js";

function impalaConfigured() {
  return Boolean(String(process.env.IMPALA_HOST ?? "").trim());
}

/** Which warehouse backs Lagebild / SQL tabs. */
export function lagebildEnginePreference() {
  const pref = String(process.env.LAGEBILD_DATA_ENGINE ?? "auto").toLowerCase();
  if (pref === "trino") {
    return trinoConfigured() ? "trino" : null;
  }
  if (pref === "impala") {
    return impalaConfigured() ? "impala" : null;
  }
  if (trinoConfigured()) return "trino";
  if (impalaConfigured()) return "impala";
  return null;
}

export async function queryTrinoDefense(sql) {
  return runTrino(qualifyDefenseSql(sql));
}

export { getTrinoState, trinoConfigured };