/** Demo NBA incidents when Trino is offline. */

export const nbaIncidentsSample = [
  {
    flow_uuid: "49d1d9d5-2ef1-458b-9ca9-d5284bae2b1a",
    status: "SCORED",
    alert_type: "demo_greifswald",
    priority: "FLASH",
    theatre: "Greifswald / Adlergrund",
    latitude: 54.55,
    longitude: 14.1,
    best_coa_id: "A",
    approved_coa_id: null,
    approved_at: null,
    approved_by: null,
    sitrep_link: "FLASH / IMMEDIATE | NBA demo_greifswald",
    created_at: "2026-11-18T17:25:29.000000",
    updated_at: "2026-11-18T17:25:29.000000",
  },
  {
    flow_uuid: "77870eca-63e5-4af4-94af-5097340fe906",
    status: "SCORED",
    alert_type: "gps_jammer",
    priority: "FLASH",
    theatre: "St. Petersburg",
    latitude: 59.91,
    longitude: 30.2,
    best_coa_id: "A",
    approved_coa_id: null,
    approved_at: null,
    approved_by: null,
    sitrep_link: "FLASH / IMMEDIATE | NBA gps_jammer",
    created_at: "2026-11-17T12:35:20.000000",
    updated_at: "2026-11-17T12:35:20.000000",
  },
  {
    flow_uuid: "f408bcbd-975d-4d97-b291-a2719569820d",
    status: "APPROVED",
    alert_type: "gps_jammer",
    priority: "FLASH",
    theatre: "Kronstadt",
    latitude: 59.77,
    longitude: 29.44,
    best_coa_id: "A",
    approved_coa_id: "A",
    approved_at: "2026-11-17T11:10:00.000000",
    approved_by: "demo-operator",
    sitrep_link: "FLASH / IMMEDIATE",
    created_at: "2026-11-17T11:05:00.000000",
    updated_at: "2026-11-17T11:10:00.000000",
  },
];

const DETAIL = {
  A: {
    coa_id: "A",
    title: "Gestaffelter Multidomänen-Verbund (ICEYE SAR + P-8A + K130)",
    gates_passed: true,
    gate_fail_reasons: [],
    score_latenz: 1,
    score_entdeckung: 0.92,
    score_eskalation: 0.75,
    score_kosten: 0.45,
    score_total: 0.866,
  },
  D: {
    coa_id: "D",
    title: "Passive UAV/Partner-Beobachtung",
    gates_passed: true,
    gate_fail_reasons: [],
    score_latenz: 0.85,
    score_entdeckung: 0.28,
    score_eskalation: 0.8,
    score_kosten: 0.85,
    score_total: 0.713,
  },
  B: {
    coa_id: "B",
    title: "Nordsee-Fregatten + optische EO",
    gates_passed: false,
    gate_fail_reasons: ["EO blocked: night or cloud cover > 15%"],
    score_latenz: 0.5,
    score_entdeckung: 0.35,
    score_eskalation: 0.7,
    score_kosten: 0.3,
    score_total: 0,
  },
  C: {
    coa_id: "C",
    title: "K130-Einzelfahrt + ALB-LiDAR",
    gates_passed: false,
    gate_fail_reasons: ["ALB blocked: turbidity or depth > 15m"],
    score_latenz: 0.85,
    score_entdeckung: 0.4,
    score_eskalation: 0.65,
    score_kosten: 0.65,
    score_total: 0,
  },
};

export function nbaCoaRunsSample(flowUuid) {
  return ["A", "D", "B", "C"].map((coa_id, idx) => {
    const d = DETAIL[coa_id];
    const gates_passed = d.gates_passed;
    const rank = gates_passed ? (coa_id === "A" ? 1 : coa_id === "D" ? 2 : idx + 1) : 0;
    return {
      flow_uuid: flowUuid,
      coa_id,
      rank,
      score_total: d.score_total,
      score_latenz: d.score_latenz,
      score_entdeckung: d.score_entdeckung,
      score_eskalation: d.score_eskalation,
      score_kosten: d.score_kosten,
      gates_passed,
      detail_json: JSON.stringify(d),
    };
  });
}