type Props = {
  flags: string[];
  vesselTypes: string[];
  shipNames: string[];
  flag: string;
  vesselType: string;
  shipName: string;
  onFlag: (v: string) => void;
  onVesselType: (v: string) => void;
  onShipName: (v: string) => void;
};

export function FilterRail({
  flags,
  vesselTypes,
  shipNames,
  flag,
  vesselType,
  shipName,
  onFlag,
  onVesselType,
  onShipName,
}: Props) {
  return (
    <aside className="filter-rail">
      <h3>Filter Rail</h3>
      <div className="filter-field">
        <label htmlFor="flag">Flag AIS (pk 130)</label>
        <select id="flag" value={flag} onChange={(e) => onFlag(e.target.value)}>
          <option value="">All</option>
          {flags.map((f) => (
            <option key={f} value={f}>
              {f}
            </option>
          ))}
        </select>
      </div>
      <div className="filter-field">
        <label htmlFor="vtype">Vessel type (pk 163)</label>
        <select id="vtype" value={vesselType} onChange={(e) => onVesselType(e.target.value)}>
          <option value="">All</option>
          {vesselTypes.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>
      <div className="filter-field">
        <label htmlFor="sname">Ship name (pk 164)</label>
        <select id="sname" value={shipName} onChange={(e) => onShipName(e.target.value)}>
          <option value="">All</option>
          {shipNames.map((n) => (
            <option key={n} value={n}>
              {n}
            </option>
          ))}
        </select>
      </div>
      <p style={{ fontSize: 11, color: "var(--text-muted)", margin: 0 }}>
        Global picklists from CDV export — URL/session sync in Phase 2.
      </p>
    </aside>
  );
}
