export type ProximityEventRaw = {
  ship_a: string;
  ship_b: string;
  event_time: string;
  ship_b_time: string;
  ship_a_lat: string;
  ship_a_lon: string;
  ship_b_lat: string;
  ship_b_lon: string;
  distance_km: string;
};

export type ProximityEvent = {
  id: number;
  ship_a: string;
  ship_b: string;
  event_time: string;
  ship_b_time: string;
  ship_a_lat: number;
  ship_a_lon: number;
  ship_b_lat: number;
  ship_b_lon: number;
  distance_km: number;
};

export function parseProximityEvents(raw: ProximityEventRaw[]): ProximityEvent[] {
  return raw.map((row, id) => ({
    id,
    ship_a: row.ship_a,
    ship_b: row.ship_b,
    event_time: row.event_time,
    ship_b_time: row.ship_b_time,
    ship_a_lat: Number(row.ship_a_lat),
    ship_a_lon: Number(row.ship_a_lon),
    ship_b_lat: Number(row.ship_b_lat),
    ship_b_lon: Number(row.ship_b_lon),
    distance_km: Number(row.distance_km),
  }));
}

export function distanceLineColor(km: number): string {
  if (km < 0.4) return "#3ddc84";
  if (km <= 0.7) return "#ffb020";
  return "#e53935";
}

export function pairMidpoint(ev: ProximityEvent): [number, number] {
  return [(ev.ship_a_lat + ev.ship_b_lat) / 2, (ev.ship_a_lon + ev.ship_b_lon) / 2];
}
