/**
 * AIS flag resolution for analytics (Trino SQL fragments).
 * Many MMSI in ais_events_ice lack a matching defense.ships row (e.g. NSB trade sim
 * without cdw-analyse/ships_nsb_trade_1000.sql loaded) → NULL flag → misleading "Unknown" bucket.
 */

/** SQL expression for display flag (alias `s` = ships, join on a.mmsi). */
export const AIS_RESOLVED_FLAG_SQL = `
  CASE
    WHEN s.mmsi IS NULL THEN 'Unknown (kein ships-Eintrag)'
    WHEN NULLIF(trim(s.flag), '') IS NULL THEN 'Unknown (Flagge leer)'
    ELSE trim(s.flag)
  END`;
