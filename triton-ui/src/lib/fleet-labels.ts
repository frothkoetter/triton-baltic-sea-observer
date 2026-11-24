/** Neutral exercise labels (Blue / Red) — internal track_kind and table names unchanged. */

export const BLUE_FLEET_LABEL = "Blue Fleet";
export const RED_FLEET_LABEL = "Red Fleet";
export const BLUE_FLEET_STAMMDATEN = "BLUE_FLEET";
export const RED_FLEET_STAMMDATEN = "RED_FLEET";

export const TRACK_KIND_LABEL: Record<"ais" | "deu_fleet" | "rus_fleet", string> = {
  ais: "AIS",
  deu_fleet: BLUE_FLEET_LABEL,
  rus_fleet: RED_FLEET_LABEL,
};

export const TRACK_KIND_SHORT: Record<"ais" | "deu_fleet" | "rus_fleet", string> = {
  ais: "AIS",
  deu_fleet: "Blue",
  rus_fleet: "Red",
};