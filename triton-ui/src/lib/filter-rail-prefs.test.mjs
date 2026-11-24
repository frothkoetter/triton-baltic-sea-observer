import test from "node:test";
import assert from "node:assert/strict";

test("filter rail defaults minimized", async () => {
  const store = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => store.set(k, v),
    removeItem: (k) => store.delete(k),
  };
  const { loadFilterRailExpanded, saveFilterRailExpanded } = await import("./filter-rail-prefs.ts");
  assert.equal(loadFilterRailExpanded(), false);
  store.set("triton_filter_rail_expanded", "1");
  assert.equal(loadFilterRailExpanded(), false, "legacy expanded must not stick");
  saveFilterRailExpanded(true);
  assert.equal(loadFilterRailExpanded(), true);
  saveFilterRailExpanded(false);
  assert.equal(loadFilterRailExpanded(), false);
});