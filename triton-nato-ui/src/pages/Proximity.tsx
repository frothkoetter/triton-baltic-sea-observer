import { useCallback, useEffect, useMemo, useState } from "react";
import fallbackEvents from "../data/ship-proximity-events.json";
import { fetchProximityAlerts } from "../api";
import { ProximityMap } from "../components/ProximityMap";
import {
  distanceLineColor,
  parseProximityEvents,
  type ProximityEvent,
  type ProximityEventRaw,
} from "../lib/proximity";

type SortKey = "event_time" | "distance_km" | "ship_a" | "ship_b";

export function ProximityPage() {
  const [rawEvents, setRawEvents] = useState<ProximityEventRaw[]>(
    fallbackEvents as ProximityEventRaw[]
  );
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [dataSource, setDataSource] = useState<"live" | "sample" | null>(null);
  const [fallbackReason, setFallbackReason] = useState<string | null>(null);

  const loadEvents = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const res = await fetchProximityAlerts();
      if (!res.ok || !res.events?.length) {
        setRawEvents(fallbackEvents as ProximityEventRaw[]);
        setDataSource("sample");
        setFallbackReason(res.error ?? res.fallbackReason ?? "Empty API response");
        setLoadError(res.error ?? "Using embedded sample data");
        return;
      }
      setRawEvents(res.events);
      setDataSource(res.source ?? "sample");
      setFallbackReason(res.fallbackReason ?? null);
    } catch (err) {
      setRawEvents(fallbackEvents as ProximityEventRaw[]);
      setDataSource("sample");
      const message = err instanceof Error ? err.message : String(err);
      setFallbackReason(message);
      setLoadError(message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadEvents();
  }, [loadEvents]);

  const events = useMemo(() => parseProximityEvents(rawEvents), [rawEvents]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>("distance_km");
  const [sortAsc, setSortAsc] = useState(true);

  const sorted = useMemo(() => {
    const copy = [...events];
    copy.sort((a, b) => {
      let cmp = 0;
      if (sortKey === "distance_km") {
        cmp = a.distance_km - b.distance_km;
      } else if (sortKey === "ship_a") {
        cmp = a.ship_a.localeCompare(b.ship_a);
      } else if (sortKey === "ship_b") {
        cmp = a.ship_b.localeCompare(b.ship_b);
      } else {
        cmp = a.event_time.localeCompare(b.event_time);
      }
      return sortAsc ? cmp : -cmp;
    });
    return copy;
  }, [events, sortKey, sortAsc]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortAsc((v) => !v);
    } else {
      setSortKey(key);
      setSortAsc(key === "distance_km" || key === "event_time");
    }
  }

  function sortIndicator(key: SortKey) {
    if (sortKey !== key) return "";
    return sortAsc ? " ▲" : " ▼";
  }

  const feedBadge =
    dataSource === "live" ? (
      <span className="status-ok">LIVE PROXIMITY API</span>
    ) : dataSource === "sample" ? (
      <span className="status-warn">SAMPLE / FALLBACK</span>
    ) : null;

  return (
    <div>
      <div className="proximity-toolbar">
        {feedBadge}
        <button type="button" className="btn-secondary" disabled={loading} onClick={() => void loadEvents()}>
          {loading ? "Lädt…" : "Aktualisieren"}
        </button>
      </div>
      {loading && events.length === 0 ? (
        <p className="proximity-status-msg">Proximity-Daten werden geladen…</p>
      ) : null}
      {!loading && loadError && dataSource === "sample" ? (
        <p className="proximity-status-msg proximity-status-warn" title={fallbackReason ?? undefined}>
          API nicht erreichbar — eingebettete Beispieldaten ({events.length} Ereignisse).
        </p>
      ) : null}

      <div className="kpi-row">
        <div className="kpi">
          <div className="kpi-value">{events.length}</div>
          <div className="kpi-label">Nähe-Ereignisse</div>
        </div>
        <div className="kpi">
          <div className="kpi-value mono" style={{ fontSize: 22 }}>
            {events.filter((e) => e.distance_km < 0.4).length}
          </div>
          <div className="kpi-label">&lt; 0,4 km (grün)</div>
        </div>
        <div className="kpi">
          <div className="kpi-value mono" style={{ fontSize: 22 }}>
            {events.filter((e) => e.distance_km >= 0.4 && e.distance_km <= 0.7).length}
          </div>
          <div className="kpi-label">0,4–0,7 km (amber)</div>
        </div>
        <div className="kpi">
          <div className="kpi-value status-alert mono" style={{ fontSize: 22 }}>
            {events.filter((e) => e.distance_km > 0.7).length}
          </div>
          <div className="kpi-label">&gt; 0,7 km (rot)</div>
        </div>
      </div>

      <div className="proximity-legend">
        <span className="legend-item">
          <span className="legend-swatch" style={{ background: "#ffb020" }} /> Schiff A
        </span>
        <span className="legend-item">
          <span className="legend-swatch" style={{ background: "#4da3ff" }} /> Schiff B
        </span>
        <span className="legend-item">
          <span className="legend-line" style={{ background: "#3ddc84" }} /> &lt; 0,4 km
        </span>
        <span className="legend-item">
          <span className="legend-line" style={{ background: "#ffb020" }} /> 0,4–0,7 km
        </span>
        <span className="legend-item">
          <span className="legend-line" style={{ background: "#e53935" }} /> &gt; 0,7 km
        </span>
      </div>

      <div className="proximity-layout">
        <section className="panel proximity-map-panel">
          <h2 className="panel-title">Schiffsnähe — Ostsee (Baltic Sea)</h2>
          <ProximityMap events={events} selectedId={selectedId} />
        </section>

        <section className="panel proximity-table-panel">
          <h2 className="panel-title">Ereignisliste — Proximity Events</h2>
          <div className="proximity-table-scroll">
            <table className="data-table proximity-table">
              <thead>
                <tr>
                  {(
                    [
                      ["ship_a", "Schiff A"],
                      ["ship_b", "Schiff B"],
                      ["distance_km", "Abstand (km)"],
                      ["event_time", "event_time"],
                    ] as const
                  ).map(([key, label]) => (
                    <th key={key}>
                      <button type="button" className="sort-header" onClick={() => toggleSort(key)}>
                        {label}
                        {sortIndicator(key)}
                      </button>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sorted.map((ev) => (
                  <ProximityRow
                    key={ev.id}
                    ev={ev}
                    selected={selectedId === ev.id}
                    onSelect={() => setSelectedId(ev.id)}
                  />
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  );
}

function ProximityRow({
  ev,
  selected,
  onSelect,
}: {
  ev: ProximityEvent;
  selected: boolean;
  onSelect: () => void;
}) {
  const distColor = distanceLineColor(ev.distance_km);
  return (
    <tr
      className={selected ? "proximity-row-selected" : undefined}
      onClick={onSelect}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect();
        }
      }}
      tabIndex={0}
      role="button"
    >
      <td className="mono">{ev.ship_a}</td>
      <td className="mono">{ev.ship_b}</td>
      <td className="mono" style={{ color: distColor }}>
        {ev.distance_km.toFixed(3)}
      </td>
      <td className="mono">{ev.event_time}</td>
    </tr>
  );
}
