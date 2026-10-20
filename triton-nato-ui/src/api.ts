import type { ProximityEventRaw } from "./lib/proximity";

export type QueryResult = {
  ok: boolean;
  columns?: string[];
  rows?: Record<string, unknown>[];
  source?: string;
  error?: string;
  fallbackReason?: string;
};

export async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(path);
  return res.json() as Promise<T>;
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
  error?: string;
};

export async function fetchProximityAlerts(): Promise<ProximityAlertsResponse> {
  const res = await fetch("/api/proximity-alerts");
  return res.json() as Promise<ProximityAlertsResponse>;
}
