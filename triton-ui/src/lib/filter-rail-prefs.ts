const STORAGE_KEY = "triton_filter_rail_expanded";

/** Default: minimized (false). */
export function loadFilterRailExpanded(): boolean {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw === "1") return true;
    if (raw === "0") return false;
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