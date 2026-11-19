/** Lagebild header KPI copy — tied to `/api/kpis` Trino aggregates. */
export type LagebildKpiKey = "fleetUnits" | "personnel" | "surveillanceReports" | "alerts";

export type LagebildKpiDef = {
  key: LagebildKpiKey;
  title: string;
  /** Shown under the number (UI). */
  hint: string;
  /** Tooltip with source table / rule. */
  detail: string;
  alert?: boolean;
};

export const LAGEBILD_KPI_DEFS: LagebildKpiDef[] = [
  {
    key: "fleetUnits",
    title: "Flotte",
    hint: "DE Marine — registrierte Einheiten",
    detail:
      "Anzahl Zeilen in defense.german_navy_fleet (Fregatten, Korvetten, U-Boote, …). Nicht identisch mit AIS- oder Karteneinheiten.",
  },
  {
    key: "personnel",
    title: "Personal",
    hint: "Summe Besatzungsstärken (DE)",
    detail:
      "SUM(besatzung) über german_navy_fleet — typische Crew pro Schiff laut Flottenstammdaten, kein Live-Bordpersonal.",
  },
  {
    key: "surveillanceReports",
    title: "Surveillance",
    hint: "Maritime Lageberichte gesamt",
    detail:
      "Anzahl Meldungen in maritime_surveillance_reports (Simulator + Showcases), unabhängig von AIS-Outliers auf der Karte.",
  },
  {
    key: "alerts",
    title: "Alerts",
    hint: "AIS-Anomalien 24 h (hoch)",
    detail:
      "Oben: AIS-Anomalien (vessel_outliers, severity ≥ 70, 24 h). Darunter: NBA-Fälle aus nba_incidents (Next Best Action / HITL) — gleiche Quelle wie Kartenebene NBA.",
    alert: true,
  },
];