import type { ProximityEventRaw } from "./lib/proximity";

export type QueryResult = {
  ok: boolean;
  columns?: string[];
  rows?: Record<string, unknown>[];
  source?: string;
  error?: string;
  fallbackReason?: string;
  deduped?: boolean;
};

const DATA_MODE_STORAGE = "triton_data_mode";

export function getClientDataMode(): "live" | "sample" | "auto" {
  const raw = localStorage.getItem(DATA_MODE_STORAGE);
  if (raw === "live" || raw === "sample" || raw === "auto") return raw;
  return "auto";
}

export function setClientDataMode(mode: "live" | "sample" | "auto") {
  localStorage.setItem(DATA_MODE_STORAGE, mode);
}

export async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const mode = getClientDataMode();
  const headers = new Headers(init?.headers);
  headers.set("X-Triton-Data-Mode", mode);
  const res = await fetch(path, { ...init, headers, credentials: "same-origin" });
  return res.json() as Promise<T>;
}

export async function setServerDataMode(mode: "live" | "sample" | "auto") {
  setClientDataMode(mode);
  return fetchJson<{ ok: boolean; mode?: string; error?: string }>("/api/data-mode", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode }),
  });
}

export async function postQuery(sql: string): Promise<QueryResult> {
  const res = await fetch("/api/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  return res.json() as Promise<QueryResult>;
}

export type ProximityAlertsResponse = {
  ok: boolean;
  source?: "live" | "sample";
  events?: ProximityEventRaw[];
  fallbackReason?: string;
  registrySource?: "live" | "sample" | "none";
  registryFallbackReason?: string;
  error?: string;
};

export async function fetchProximityAlerts(): Promise<ProximityAlertsResponse> {
  const res = await fetch("/api/proximity-alerts");
  return res.json() as Promise<ProximityAlertsResponse>;
}

export type NbaIncident = {
  flow_uuid: string;
  status?: string;
  alert_type?: string;
  priority?: string;
  theatre?: string;
  latitude?: number;
  longitude?: number;
  best_coa_id?: string | null;
  approved_coa_id?: string | null;
  approved_at?: string | null;
  approved_by?: string | null;
  sitrep_link?: string | null;
  created_at?: string;
  updated_at?: string;
};

export type NbaCoaRun = {
  coa_id: string;
  rank?: number | null;
  score_total?: number | null;
  score_latenz?: number | null;
  score_entdeckung?: number | null;
  score_eskalation?: number | null;
  score_kosten?: number | null;
  gates_passed?: boolean;
  detail_json?: string;
};

export async function fetchNbaIncidents(): Promise<{
  ok?: boolean;
  rows?: NbaIncident[];
  source?: string;
  fallbackReason?: string;
}> {
  return fetchJson("/api/nba/incidents");
}

export async function fetchNbaCoaRuns(flowUuid: string): Promise<{
  ok?: boolean;
  rows?: NbaCoaRun[];
  source?: string;
}> {
  return fetchJson(`/api/nba/incidents/${encodeURIComponent(flowUuid)}/coa-runs`);
}

export async function fetchNbaDecisionRecord(flowUuid: string): Promise<{
  ok?: boolean;
  record?: import("./lib/nba-decision").NbaDecisionRecord;
  source?: string;
}> {
  return fetchJson(`/api/nba/incidents/${encodeURIComponent(flowUuid)}/decision-record`);
}

export async function approveNbaCoa(
  flowUuid: string,
  coaId: string,
  approvedBy?: string
): Promise<{ ok: boolean; error?: string }> {
  const res = await fetch(`/api/nba/incidents/${encodeURIComponent(flowUuid)}/approve`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ coaId, approvedBy }),
  });
  return res.json() as Promise<{ ok: boolean; error?: string }>;
}
