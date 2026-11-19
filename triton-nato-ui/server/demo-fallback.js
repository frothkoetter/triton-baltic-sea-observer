/** Offline / autonomous demo when warehouse is unreachable or unset. */

import { nbaIncidentsSample } from "./nba-sample.js";
import { trinoConfigured } from "./trino-client.js";

export function forceSampleData() {
  return process.env.TRITON_FORCE_SAMPLE_DATA === "true";
}

/** Skip Trino/Impala and use embedded payloads (explicit or no credentials). */
export function useOfflineDemoData() {
  return forceSampleData() || !trinoConfigured();
}

export function demoMeta() {
  const offline = useOfflineDemoData();
  return {
    demoMode: offline,
    forceSample: forceSampleData(),
    trinoConfigured: trinoConfigured(),
    nbaIncidentsSample: nbaIncidentsSample.length,
  };
}