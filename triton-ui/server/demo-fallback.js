/** Offline / autonomous demo when warehouse is unreachable or unset. */

import { nbaIncidentsSample } from "./nba-sample.js";
import { trinoConfigured } from "./trino-client.js";
import {
  dataModeMeta,
  envForceSample,
  getSampleReason,
  shouldUseSampleData,
} from "./data-mode.js";

export function forceSampleData() {
  return envForceSample() || shouldUseSampleData();
}

/** Skip Trino/Impala and use embedded payloads (explicit or no credentials). */
export function useOfflineDemoData() {
  return shouldUseSampleData();
}

export function demoMeta() {
  const offline = useOfflineDemoData();
  return {
    demoMode: offline,
    forceSample: envForceSample(),
    trinoConfigured: trinoConfigured(),
    nbaIncidentsSample: nbaIncidentsSample.length,
    ...dataModeMeta(),
    sampleReason: getSampleReason(),
  };
}