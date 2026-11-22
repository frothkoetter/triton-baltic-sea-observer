import fs from "node:fs";
import path from "node:path";

/** True when the Vite bundle includes DAS spectrogram + audio (not a pre-DAS dist). */
export function distDasPreviewBundled(distRoot) {
  const assets = path.join(distRoot, "assets");
  try {
    const names = fs.readdirSync(assets).filter((n) => n.endsWith(".js"));
    for (const name of names) {
      const body = fs.readFileSync(path.join(assets, name), "utf8");
      if (
        body.includes("das-spectrogram-canvas") &&
        body.includes("das-audio-player")
      ) {
        return { ok: true, asset: name };
      }
    }
    return {
      ok: false,
      error: "dist/ lacks DAS preview components — run: cd triton-ui && bash cdsw-build.sh",
    };
  } catch (exc) {
    return { ok: false, error: String(exc.message || exc) };
  }
}