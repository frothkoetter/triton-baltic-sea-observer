/** v2: reset legacy expanded preference so fresh sessions start minimized. */
const STORAGE_KEY = "triton_filter_rail_expanded_v2";
const LEGACY_STORAGE_KEY = "triton_filter_rail_expanded";

/** Default: minimized (false). */
export function loadFilterRailExpanded(): boolean {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw === "1") return true;
    if (raw === "0") return false;
    const legacy = localStorage.getItem(LEGACY_STORAGE_KEY);
    if (legacy === "0") return false;
  } catch {
    /* ignore */
  }
  return false;
}

export function saveFilterRailExpanded(expanded: boolean): void {
  try {
    localStorage.setItem(STORAGE_KEY, expanded ? "1" : "0");
  } catch {
    /* ignore */
  }
}