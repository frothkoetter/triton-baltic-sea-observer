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
  ship_a_mmsi?: string;
  ship_b_mmsi?: string;
  ship_a_name?: string | null;
  ship_a_type?: string | null;
  ship_b_name?: string | null;
  ship_b_type?: string | null;
};

export type ProximityEvent = {
  id: number;
  ship_a_mmsi: string;
  ship_b_mmsi: string;
  ship_a_name: string | null;
  ship_a_type: string | null;
  ship_b_name: string | null;
  ship_b_type: string | null;
  /** Sort/display label — name · type, else MMSI */
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

export function formatShipLabel(
  mmsi: string,
  name: string | null | undefined,
  vesselType: string | null | undefined
): string {
  const n = name?.trim();
  const t = vesselType?.trim();
  if (n && t) return `${n} · ${t}`;
  if (n) return n;
  return mmsi;
}

export function parseProximityEvents(raw: ProximityEventRaw[]): ProximityEvent[] {
  return raw.map((row, id) => {
    const ship_a_mmsi = row.ship_a_mmsi ?? row.ship_a;
    const ship_b_mmsi = row.ship_b_mmsi ?? row.ship_b;
    const ship_a_name = row.ship_a_name ?? null;
    const ship_a_type = row.ship_a_type ?? null;
    const ship_b_name = row.ship_b_name ?? null;
    const ship_b_type = row.ship_b_type ?? null;
    return {
      id,
      ship_a_mmsi,
      ship_b_mmsi,
      ship_a_name,
      ship_a_type,
      ship_b_name,
      ship_b_type,
      ship_a: formatShipLabel(ship_a_mmsi, ship_a_name, ship_a_type),
      ship_b: formatShipLabel(ship_b_mmsi, ship_b_name, ship_b_type),
      event_time: row.event_time,
      ship_b_time: row.ship_b_time,
      ship_a_lat: Number(row.ship_a_lat),
      ship_a_lon: Number(row.ship_a_lon),
      ship_b_lat: Number(row.ship_b_lat),
      ship_b_lon: Number(row.ship_b_lon),
      distance_km: Number(row.distance_km),
    };
  });
}

export function distanceLineColor(km: number): string {
  if (km < 0.4) return "#3ddc84";
  if (km <= 0.7) return "#ffb020";
  return "#e53935";
}

export function pairMidpoint(ev: ProximityEvent): [number, number] {
  return [(ev.ship_a_lat + ev.ship_b_lat) / 2, (ev.ship_a_lon + ev.ship_b_lon) / 2];
}
