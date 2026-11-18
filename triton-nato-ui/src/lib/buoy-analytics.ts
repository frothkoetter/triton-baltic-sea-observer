export type ClassificationGridRow = {
  payload_object_type: string;
  payload_object_classification: string;
  avg_confidence: number;
  record_count: number;
};

export type ScatterPoint = {
  x: number;
  y: number;
  z: number;
  avg_confidence: number;
  payload_object_type: string;
  payload_object_classification: string;
};

export function buildClassificationScatter(
  rows: Record<string, unknown>[]
): { points: ScatterPoint[]; typeLabels: string[]; classLabels: string[] } {
  const typed = rows.map((r) => ({
    payload_object_type: String(r.payload_object_type ?? "—"),
    payload_object_classification: String(r.payload_object_classification ?? "—"),
    avg_confidence: Number(r.avg_confidence ?? 0),
    record_count: Number(r.record_count ?? 0),
  }));
  const typeLabels = [...new Set(typed.map((t) => t.payload_object_type))].sort();
  const classLabels = [...new Set(typed.map((t) => t.payload_object_classification))].sort();
  const points: ScatterPoint[] = typed.map((t) => ({
    x: typeLabels.indexOf(t.payload_object_type),
    y: classLabels.indexOf(t.payload_object_classification),
    z: t.record_count,
    avg_confidence: t.avg_confidence,
    payload_object_type: t.payload_object_type,
    payload_object_classification: t.payload_object_classification,
  }));
  return { points, typeLabels, classLabels };
}

export function confidenceColor(confidence: number): string {
  const t = Math.max(0, Math.min(100, confidence)) / 100;
  const r = Math.round(69 + (61 - 69) * t);
  const g = Math.round(186 + (220 - 186) * t);
  const b = Math.round(199 + (141 - 199) * t);
  return `rgb(${r},${g},${b})`;
}