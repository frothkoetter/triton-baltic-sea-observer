import stamp from "../../build-stamp.json";

/** Bump `build-stamp.json` when CML must run `npm run build` after git pull. */
export const TRITON_UI_VERSION = stamp.version;
export const TRITON_UI_BUILD = stamp.build;