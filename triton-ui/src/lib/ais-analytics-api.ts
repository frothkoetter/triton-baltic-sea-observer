import type { LagebildFilters } from "./lagebild-filters";
import { serializeFilterQueryParams } from "./lagebild-filters";

export type AisAnalyticsPayload = {
  ok?: boolean;
  source?: string;
  fallbackReason?: string;
  filters?: LagebildFilters;
  panels?: {
    kpi?: { rows?: Record<string, unknown>[] };
    byFlag?: { rows?: Record<string, unknown>[] };
    byType?: { rows?: Record<string, unknown>[] };
    topDestinations?: { rows?: Record<string, unknown>[] };
    byStatus?: { rows?: Record<string, unknown>[] };
    bySpeedBand?: { rows?: Record<string, unknown>[] };
    avgSogByFlag?: { rows?: Record<string, unknown>[] };
  };
};

export function aisAnalyticsQuery(filters: LagebildFilters): string {
  const q = serializeFilterQueryParams(filters).toString();
  return q ? `?${q}` : "";
}