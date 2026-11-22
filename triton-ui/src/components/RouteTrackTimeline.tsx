import { useEffect, useMemo, useState } from "react";
import type { PositionTrack, TrackKind } from "../lib/position-tracks";
import { formatTrackTime, trackTimeRange } from "../lib/track-timeline";
import { RouteTrackMap } from "./RouteTrackMap";

const MAX_OBJECTS = 10;

const KIND_GROUP: Record<TrackKind, string> = {
  ais: "AIS",
  deu_fleet: "DEU Fleet",
  rus_fleet: "RUS Fleet",
};

type Props = {
  tracks: PositionTrack[];
  loading?: boolean;
  /** True when Trino bridge hit fetch/SQL limit (not all 24h points returned). */
  truncated?: boolean;
};

export function RouteTrackTimeline({ tracks, loading, truncated }: Props) {
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [hiddenIds, setHiddenIds] = useState<Set<string>>(() => new Set());
  const [cursorMs, setCursorMs] = useState<number | null>(null);

  useEffect(() => {
    setSelectedIds((prev) => {
      const valid = prev.filter((id) => tracks.some((t) => t.id === id));
      if (valid.length > 0) return valid.slice(0, MAX_OBJECTS);
      return tracks.slice(0, MAX_OBJECTS).map((t) => t.id);
    });
    setHiddenIds(new Set());
  }, [tracks]);

  const selectedTracks = useMemo(
    () => tracks.filter((t) => selectedIds.includes(t.id)),
    [tracks, selectedIds]
  );

  const visibleTracks = useMemo(
    () => selectedTracks.filter((t) => !hiddenIds.has(t.id)),
    [selectedTracks, hiddenIds]
  );

  const range = useMemo(
    () => trackTimeRange(selectedTracks.length ? selectedTracks : tracks),
    [selectedTracks, tracks]
  );

  useEffect(() => {
    if (!range) {
      setCursorMs(null);
      return;
    }
    setCursorMs((prev) => {
      if (prev == null || prev < range.min || prev > range.max) return range.max;
      return prev;
    });
  }, [range?.min, range?.max]);

  const effectiveCursor = range && cursorMs != null ? cursorMs : null;

  function toggleSelect(id: string) {
    setSelectedIds((prev) => {
      if (prev.includes(id)) return prev.filter((x) => x !== id);
      if (prev.length >= MAX_OBJECTS) return prev;
      return [...prev, id];
    });
    setHiddenIds((h) => {
      const next = new Set(h);
      next.delete(id);
      return next;
    });
  }

  function toggleVisible(id: string) {
    if (!selectedIds.includes(id)) return;
    setHiddenIds((h) => {
      const next = new Set(h);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  const grouped = useMemo(() => {
    const order: TrackKind[] = ["ais", "deu_fleet", "rus_fleet"];
    return order
      .map((kind) => ({
        kind,
        title: KIND_GROUP[kind],
        items: tracks.filter((t) => t.kind === kind),
      }))
      .filter((g) => g.items.length > 0);
  }, [tracks]);

  const atMin = range && effectiveCursor != null && effectiveCursor <= range.min;
  const atMax = range && effectiveCursor != null && effectiveCursor >= range.max;

  return (
    <div className="route-track-timeline">
      <p className="route-track-timeline-hint">
        Bis zu {MAX_OBJECTS} Objekte aus AIS, DEU Fleet und RUS Fleet wählen. Mit dem Regler die Zeit
        verschieben — die Route wird dynamisch bis zum gewählten Zeitpunkt gezeichnet.
        {truncated ? (
          <>
            {" "}
            <strong>Hinweis:</strong> Daten gekürzt — hohe <span className="mono">pts</span>-Zahlen bei
            wenigen Schiffen entstehen, wenn Trino nur einen Teil der Historie liefert.
          </>
        ) : null}
      </p>

      <div className="route-track-picker">
        {grouped.map((g) => (
          <div key={g.kind} className="route-track-picker-group">
            <div className="route-track-picker-group-title mono">{g.title}</div>
            <ul className="route-track-picker-list">
              {g.items.map((t) => {
                const selected = selectedIds.includes(t.id);
                const hidden = hiddenIds.has(t.id);
                const selectDisabled = !selected && selectedIds.length >= MAX_OBJECTS;
                return (
                  <li key={t.id} className="route-track-picker-row">
                    <label className="route-track-picker-select">
                      <input
                        type="checkbox"
                        checked={selected}
                        disabled={selectDisabled}
                        onChange={() => toggleSelect(t.id)}
                      />
                      <span className="route-track-picker-label" title={t.mmsi}>
                        {t.label}
                      </span>
                      <span className="route-track-picker-meta mono">{t.pointCount} pts</span>
                    </label>
                    <button
                      type="button"
                      className={`chip chip-sm ${selected && !hidden ? "on" : ""}`}
                      disabled={!selected}
                      title={hidden ? "Einblenden" : "Ausblenden"}
                      onClick={() => toggleVisible(t.id)}
                    >
                      {hidden ? "Aus" : "An"}
                    </button>
                  </li>
                );
              })}
            </ul>
          </div>
        ))}
        {!loading && tracks.length === 0 ? (
          <p className="mono" style={{ color: "var(--text-muted)", fontSize: 12 }}>
            Keine Routen geladen.
          </p>
        ) : null}
      </div>

      <div className="route-track-timebar" aria-label="Zeitachse">
        <div className="route-track-timebar-labels mono">
          <span>
            <strong>Beginn</strong>
            <br />
            {range ? formatTrackTime(range.min) : "—"}
          </span>
          <span className="route-track-timebar-now">
            <strong>Zeitpunkt</strong>
            <br />
            {effectiveCursor != null ? formatTrackTime(effectiveCursor) : "—"}
          </span>
          <span style={{ textAlign: "right" }}>
            <strong>Ende</strong>
            <br />
            {range ? formatTrackTime(range.max) : "—"}
          </span>
        </div>
        <input
          type="range"
          className="route-track-slider"
          min={range?.min ?? 0}
          max={range?.max ?? 1}
          step={range ? Math.max(60_000, Math.floor((range.max - range.min) / 500)) : 60_000}
          value={effectiveCursor ?? range?.max ?? 0}
          disabled={!range || loading}
          onChange={(e) => setCursorMs(Number(e.target.value))}
        />
        <div className="route-track-timebar-actions">
          <button
            type="button"
            className="chip chip-sm"
            disabled={!range || atMin}
            onClick={() => range && setCursorMs(range.min)}
          >
            Zum Beginn
          </button>
          <button
            type="button"
            className="chip chip-sm"
            disabled={!range || atMax}
            onClick={() => range && setCursorMs(range.max)}
          >
            Zum Ende
          </button>
          <span className="mono route-track-selection-count">
            {visibleTracks.length} / {selectedTracks.length} sichtbar
          </span>
        </div>
      </div>

      <RouteTrackMap
        tracks={visibleTracks}
        cursorTimeMs={effectiveCursor}
        showStartMarkers={!atMin}
      />
    </div>
  );
}