/** Demo NBA incidents when Trino is offline. */

export const nbaIncidentsSample = [
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

export function nbaCoaRunsSample(flowUuid) {
  return [
    {
      flow_uuid: flowUuid,
      coa_id: "A",
      rank: 1,
      score_total: 0.866,
      gates_passed: true,
      detail_json: '{"title":"Multidomänen-Verbund"}',
    },
    {
      flow_uuid: flowUuid,
      coa_id: "D",
      rank: 2,
      score_total: 0.713,
      gates_passed: true,
      detail_json: '{"title":"Passive UAV"}',
    },
    {
      flow_uuid: flowUuid,
      coa_id: "B",
      rank: 0,
      score_total: 0,
      gates_passed: false,
      detail_json: "{}",
    },
  ];
}