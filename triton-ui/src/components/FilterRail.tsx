import type { LagebildFilters } from "../lib/lagebild-filters";

function activeFilterCount(f: LagebildFilters): number {
  return (
    f.flags.length +
    f.vesselTypes.length +
    f.shipNames.length +
    f.germanUnits.length +
    f.russianUnits.length
  );
}

export type FilterRailPicklists = {
  flags: string[];
  vesselTypes: string[];
  shipNames: string[];
  germanUnits: string[];
  russianUnits: string[];
};

type Props = FilterRailPicklists & {
  filters: LagebildFilters;
  onFiltersChange: (next: LagebildFilters) => void;
  expanded: boolean;
  onExpandedChange: (expanded: boolean) => void;
};

function toggleValue(list: string[], value: string): string[] {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value];
}

function MultiFilterField({
  id,
  label,
  options,
  selected,
  onToggle,
  onClear,
  hint,
}: {
  id: string;
  label: string;
  options: string[];
  selected: string[];
  onToggle: (value: string) => void;
  onClear: () => void;
  hint?: string;
}) {
  return (
    <div className="filter-field">
      <label htmlFor={`${id}-search`}>{label}</label>
      {hint ? (
        <p className="filter-hint" style={{ margin: "0 0 4px" }}>
          {hint}
        </p>
      ) : null}
      <div className="filter-multi" id={id} role="group" aria-label={label}>
        {options.length === 0 ? (
          <span className="filter-multi-empty">Keine Einträge</span>
        ) : (
          options.map((opt) => (
            <label key={opt} className="filter-multi-item">
              <input
                type="checkbox"
                checked={selected.includes(opt)}
                onChange={() => onToggle(opt)}
              />
              <span>{opt}</span>
            </label>
          ))
        )}
      </div>
      {selected.length > 0 ? (
        <button type="button" className="filter-clear-link" onClick={onClear}>
          Auswahl löschen ({selected.length})
        </button>
      ) : null}
    </div>
  );
}

export function FilterRail({
  flags,
  vesselTypes,
  shipNames,
  germanUnits,
  russianUnits,
  filters,
  onFiltersChange,
  expanded,
  onExpandedChange,
}: Props) {
  const active = activeFilterCount(filters);
  const patch = (partial: Partial<LagebildFilters>) =>
    onFiltersChange({ ...filters, ...partial });

  const clearAis = () =>
    patch({ flags: [], vesselTypes: [], shipNames: [] });

  const clearAll = () =>
    onFiltersChange({
      flags: [],
      vesselTypes: [],
      shipNames: [],
      germanUnits: [],
      russianUnits: [],
    });

  if (!expanded) {
    return (
      <aside className="filter-rail filter-rail--mini" aria-label="Filter Rail (minimiert)">
        <button
          type="button"
          className="filter-rail-expand-btn"
          title="Filter einblenden"
          aria-expanded={false}
          onClick={() => onExpandedChange(true)}
        >
          <span className="filter-rail-expand-icon" aria-hidden>
            »
          </span>
          <span className="filter-rail-mini-label">Filter</span>
        </button>
        {active > 0 ? (
          <span className="filter-rail-active-badge" title={`${active} aktive Filter`}>
            {active}
          </span>
        ) : null}
        <button type="button" className="chip filter-rail-mini-clear" onClick={clearAll} title="Alle Filter löschen">
          ✕
        </button>
      </aside>
    );
  }

  return (
    <aside className="filter-rail filter-rail--expanded">
      <div className="filter-rail-head">
        <h3>Filter Rail</h3>
        <button
          type="button"
          className="filter-rail-minimize-btn"
          title="Filter minimieren"
          aria-expanded
          onClick={() => onExpandedChange(false)}
        >
          « Minimieren
        </button>
      </div>
      <MultiFilterField
        id="flag"
        label="Flag AIS (Mehrfach)"
        options={flags}
        selected={filters.flags}
        onToggle={(v) => patch({ flags: toggleValue(filters.flags, v) })}
        onClear={() => patch({ flags: [] })}
      />
      <MultiFilterField
        id="vtype"
        label="Vessel type (Mehrfach)"
        options={vesselTypes}
        selected={filters.vesselTypes}
        onToggle={(v) => patch({ vesselTypes: toggleValue(filters.vesselTypes, v) })}
        onClear={() => patch({ vesselTypes: [] })}
      />
      <MultiFilterField
        id="sname"
        label="Ship name AIS (Mehrfach)"
        options={shipNames}
        selected={filters.shipNames}
        onToggle={(v) => patch({ shipNames: toggleValue(filters.shipNames, v) })}
        onClear={() => patch({ shipNames: [] })}
      />
      <MultiFilterField
        id="deu-fleet"
        label="DEU Fleet (Mehrfach)"
        options={germanUnits}
        selected={filters.germanUnits}
        onToggle={(v) => patch({ germanUnits: toggleValue(filters.germanUnits, v) })}
        onClear={() => patch({ germanUnits: [] })}
        hint="Kartenmarker & 24h-Tracks"
      />
      <MultiFilterField
        id="rus-fleet"
        label="RUS Fleet (Mehrfach)"
        options={russianUnits}
        selected={filters.russianUnits}
        onToggle={(v) => patch({ russianUnits: toggleValue(filters.russianUnits, v) })}
        onClear={() => patch({ russianUnits: [] })}
        hint="Kartenmarker & 24h-Tracks"
      />
      <div className="filter-rail-actions">
        <button type="button" className="chip" onClick={clearAis}>
          AIS Filter zurücksetzen
        </button>
        <button type="button" className="chip" onClick={clearAll}>
          Alle Filter löschen
        </button>
      </div>
      <p style={{ fontSize: 11, color: "var(--text-muted)", margin: 0 }}>
        Leer = alle Objekte. Mehrfachauswahl schränkt Karte, Tabelle und Routen ein.
      </p>
    </aside>
  );
}