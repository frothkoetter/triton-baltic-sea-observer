import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STAMP_PATH = path.join(__dirname, "..", "build-stamp.json");

let _cached;

export function loadBuildStamp() {
  if (_cached) return _cached;
  try {
    _cached = JSON.parse(readFileSync(STAMP_PATH, "utf8"));
    return _cached;
  } catch {
    return {
      version: "0.2.2",
      build: "0.2.2-das-preview",
      features: { dataModeFooterToggle: true, agentConsole: true, dasSpectrogramAudio: true },
    };
  }
}