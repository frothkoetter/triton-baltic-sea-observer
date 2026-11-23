import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "../api";
import { DataTable } from "../components/DataTable";
import { SocmintMap } from "../components/SocmintMap";
import {
  collapseGpsJammerByGeohash,
  filterGpsJammerEvents,
  normalizeGpsJammerRow,
  type GpsJammingFilter,
  type GpsJammerEvent,
} from "../lib/gps-jammer";
import {
  normalizeSocmintDensityRow,
  normalizeSocmintRow,
  normalizeThreatTileRow,
  type SocmintMessage,
  type SocmintDensityTile,
  type ThreatHotTile,
} from "../lib/socmint";

type FeedPayload = {
  ok?: boolean;
  source?: string;
  messages?: Record<string, unknown>[];
  threatTiles?: Record<string, unknown>[];
  densityTiles?: Record<string, unknown>[];
  gpsJammerEvents?: Record<string, unknown>[];
  gpsJammerSource?: string;
};

const TRUST_FILTERS = [
  { id: "all", label: "Alle Trust-Tags" },
  { id: "SENSOR_CORROBORATED", label: "Sensor bestätigt" },
  { id: "UNVERIFIED_SOCMINT", label: "Unverifiziert" },
  { id: "DISINFORMATION_CONFIRMED", label: "Desinformation" },
  { id: "LOW_CONFIDENCE", label: "Niedrig" },
] as const;

export function SocmintPage() {
  const [source, setSource] = useState("…");
  const [messages, setMessages] = useState<SocmintMessage[]>([]);
  const [threatTiles, setThreatTiles] = useState<ThreatHotTile[]>([]);
  const [densityTiles, setDensityTiles] = useState<SocmintDensityTile[]>([]);
  const [gpsJammerEvents, setGpsJammerEvents] = useState<GpsJammerEvent[]>([]);
  const [gpsJammerSource, setGpsJammerSource] = useState("…");
  const [trustFilter, setTrustFilter] = useState<string>("all");
  const [platformFilter, setPlatformFilter] = useState<string>("");
  const [iwOnly, setIwOnly] = useState(false);
  const [selected, setSelected] = useState<SocmintMessage | null>(null);
  const [layers, setLayers] = useState({
    posts: true,
    threatTiles: true,
    densityTiles: false,
    gpsJammer: true,
  });
  const [gpsJammingFilter, setGpsJammingFilter] = useState<GpsJammingFilter>("all");
  const [gpsMaxSignalIntegrity, setGpsMaxSignalIntegrity] = useState(1);

  useEffect(() => {
    void (async () => {
      const res = await fetchJson<FeedPayload>("/api/socmint/feed");
      setSource(res.source ?? "unknown");
      setMessages(
        (res.messages ?? [])
          .map((r) => normalizeSocmintRow(r))
          .filter((r): r is SocmintMessage => r != null)
      );
      setThreatTiles(
        (res.threatTiles ?? [])
          .map((r) => normalizeThreatTileRow(r))
          .filter((r): r is ThreatHotTile => r != null)
      );
      setDensityTiles(
        (res.densityTiles ?? [])
          .map((r) => normalizeSocmintDensityRow(r))
          .filter((r): r is SocmintDensityTile => r != null)
      );
      setGpsJammerSource(res.gpsJammerSource ?? "unknown");
      setGpsJammerEvents(
        (res.gpsJammerEvents ?? [])
          .map((r) => normalizeGpsJammerRow(r))
          .filter((r): r is GpsJammerEvent => r != null)
      );
    })();
  }, []);

  const platforms = useMemo(() => {
    return [...new Set(messages.map((m) => m.platform ?? "x").filter(Boolean))].sort();
  }, [messages]);

  const filteredGpsJammer = useMemo(() => {
    const narrowed = filterGpsJammerEvents(gpsJammerEvents, {
      jamming: gpsJammingFilter,
      maxSignalIntegrity: gpsMaxSignalIntegrity,
    });
    return collapseGpsJammerByGeohash(narrowed);
  }, [gpsJammerEvents, gpsJammingFilter, gpsMaxSignalIntegrity]);

  const filtered = useMemo(() => {
    return messages.filter((m) => {
      if (iwOnly && !m.disinformation_flag) return false;
      if (trustFilter !== "all" && m.trust_tag !== trustFilter) return false;
      if (platformFilter && (m.platform ?? "x") !== platformFilter) return false;
      return true;
    });
  }, [messages, trustFilter, platformFilter, iwOnly]);

  const kpis = useMemo(() => {
    const n = filtered.length;
    const corroborated = filtered.filter((m) => m.trust_tag === "SENSOR_CORROBORATED").length;
    const disinfo = filtered.filter(
      (m) => m.disinformation_flag || m.trust_tag === "DISINFORMATION_CONFIRMED"
    ).length;
    const hotTiles = threatTiles.length;
    const gpsJam = filteredGpsJammer.length;
    return { n, corroborated, disinfo, hotTiles, gpsJam };
  }, [filtered, threatTiles, filteredGpsJammer]);

  const tableRows = useMemo(
    () =>
      filtered.map((m) => ({
        ts: m.ts,
        user: m.user_username,
        platform: m.platform ?? "—",
        trust: m.trust_tag ?? "—",
        intel: m.intel_category ?? "—",
        quadkey: m.quadkey ?? "—",
        priority: m.priority ?? "—",
        tweet: m.tweet.length > 90 ? `${m.tweet.slice(0, 90)}…` : m.tweet,
      })),
    [filtered]
  );

  return (
    <div className="socmint-page">
      <header className="panel-header">
        <div>
          <h1 className="panel-title">SOCMINT · Ostsee</h1>
          <p className="panel-subtitle mono">
            Quelle {source} · Posts {kpis.n} · bestätigt {kpis.corroborated} · IW {kpis.disinfo} ·
            Hot tiles {kpis.hotTiles}
            {layers.gpsJammer ? ` · GPS jammer ${kpis.gpsJam} (${gpsJammerSource})` : ""}
          </p>
        </div>
        <div className="socmint-toolbar">
          <label className="mono">
            Trust
            <select
              value={trustFilter}
              onChange={(e) => setTrustFilter(e.target.value)}
              aria-label="Trust filter"
            >
              {TRUST_FILTERS.map((f) => (
                <option key={f.id} value={f.id}>
                  {f.label}
                </option>
              ))}
            </select>
          </label>
          <label className="mono">
            Plattform
            <select
              value={platformFilter}
              onChange={(e) => setPlatformFilter(e.target.value)}
              aria-label="Platform filter"
            >
              <option value="">Alle</option>
              {platforms.map((p) => (
                <option key={p} value={p}>
                  {p}
                </option>
              ))}
            </select>
          </label>
          <label className="mono checkbox-inline">
            <input
              type="checkbox"
              checked={iwOnly}
              onChange={(e) => setIwOnly(e.target.checked)}
            />
            nur IW
          </label>
        </div>
      </header>

      <div className="socmint-layer-toggles mono">
        {(
          [
            ["posts", "Posts"],
            ["threatTiles", "Threat-Kacheln (sketch)"],
            ["densityTiles", "SOCMINT-Dichte"],
            ["gpsJammer", `GPS jammer (${filteredGpsJammer.length})`],
          ] as const
        ).map(([key, label]) => (
          <label key={key} className="checkbox-inline">
            <input
              type="checkbox"
              checked={layers[key]}
              onChange={(e) => setLayers((prev) => ({ ...prev, [key]: e.target.checked }))}
            />
            {label}
          </label>
        ))}
      </div>

      {layers.gpsJammer ? (
        <div className="socmint-gps-filters mono">
          <label>
            jamming_indicator
            <select
              value={gpsJammingFilter}
              onChange={(e) => setGpsJammingFilter(e.target.value as GpsJammingFilter)}
              aria-label="GPS jamming indicator filter"
            >
              <option value="all">Alle</option>
              <option value="jamming">Jamming (true)</option>
              <option value="monitor">Monitor (false)</option>
            </select>
          </label>
          <label className="socmint-range">
            signal_integrity ≤{" "}
            <input
              type="range"
              min={0}
              max={100}
              value={Math.round(gpsMaxSignalIntegrity * 100)}
              onChange={(e) => setGpsMaxSignalIntegrity(Number(e.target.value) / 100)}
              aria-label="Maximum signal integrity"
            />
            <span>{gpsMaxSignalIntegrity.toFixed(2)}</span>
          </label>
          <span className="text-muted">Kacheln: 1× geohash (keine Punkt-Marker)</span>
        </div>
      ) : null}

      <div className="socmint-layout">
        <div className="socmint-map-panel">
          <SocmintMap
            messages={filtered}
            threatTiles={threatTiles}
            densityTiles={densityTiles}
            selectedUsername={selected?.user_username}
            selectedTs={selected?.ts}
            showPosts={layers.posts}
            showThreatTiles={layers.threatTiles}
            showDensityTiles={layers.densityTiles}
            showGpsJammer={layers.gpsJammer}
            gpsJammerEvents={layers.gpsJammer ? filteredGpsJammer : []}
            onSelectMessage={setSelected}
          />
        </div>
        <aside className="socmint-detail-panel">
          {selected ? (
            <motionDetail selected={selected} />
          ) : (
            <p className="text-muted">Post auf der Karte oder in der Tabelle wählen.</p>
          )}
          <DataTable
            columns={["ts", "user", "platform", "trust", "intel", "quadkey", "priority", "tweet"]}
            rows={tableRows}
            onRowClick={(row) => {
              const hit = filtered.find(
                (m) => m.user_username === row.user && m.ts === row.ts
              );
              if (hit) setSelected(hit);
            }}
          />
        </aside>
      </div>
    </div>
  );
}

function motionDetail({ selected }: { selected: SocmintMessage }) {
  return (
    <div className="socmint-detail-card">
      <div className="mono" style={{ fontSize: 12, color: "var(--text-muted)" }}>
        @{selected.user_username} · {selected.platform ?? "x"} · Kachel {selected.quadkey}
      </div>
      <div style={{ fontWeight: 600, marginTop: 6 }}>
        Trust: {selected.trust_tag}
        {selected.disinformation_flag ? " · IW flagged" : ""}
      </div>
      <p className="socmint-tweet-full">{selected.tweet}</p>
      {selected.verification_notes ? (
        <p className="mono" style={{ fontSize: 12, color: "var(--accent-amber)" }}>
          Verify: {selected.verification_notes}
        </p>
      ) : null}
      <div className="mono" style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 8 }}>
        {selected.intel_category ?? "—"} · {selected.scenario_id ?? "—"} ·{" "}
        {selected.correlation_hint ?? "—"}
      </div>
    </div>
  );
}