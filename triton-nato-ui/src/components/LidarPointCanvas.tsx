import { useEffect, useRef } from "react";
import { classificationColor, heightColor } from "../lib/lidar-colors";

export type ViewerPoint = {
  x: number;
  y: number;
  z: number;
  cls: number;
  intensity?: number;
};

export type LidarViewFrame = "enu" | "vessel";

type Props = {
  points: ViewerPoint[];
  /** World heading (deg); used only when viewFrame is `enu` (S3 LAS). */
  headingDeg?: number;
  viewFrame?: LidarViewFrame;
  label: string;
  colorMode?: "class" | "height";
};

const WATER_SURFACE_CLS = 10;
/** Port-quarter elevated view — starboard profile, bow to the right. */
const BEAM_SKEW = 0.38;
const ELEVATION_SKEW = 0.22;
/** Expand fit box so the cloud sits slightly smaller (camera farther back). */
const VIEW_BBOX_EXPAND = 1.28;

export function enuToVesselFrame(
  x: number,
  y: number,
  headingDeg: number
): { bow: number; stbd: number } {
  const h = (headingDeg * Math.PI) / 180;
  const ch = Math.cos(h);
  const sh = Math.sin(h);
  return {
    bow: x * sh + y * ch,
    stbd: x * ch - y * sh,
  };
}

export function projectObliqueSideView(bow: number, stbd: number, up: number): { sx: number; sy: number; depth: number } {
  const sx = bow + stbd * BEAM_SKEW;
  const sy = -up - stbd * ELEVATION_SKEW;
  const depth = bow - stbd * 0.15;
  return { sx, sy, depth };
}

function toVesselAxes(
  p: ViewerPoint,
  headingDeg: number,
  viewFrame: LidarViewFrame
): { bow: number; stbd: number; up: number } {
  if (viewFrame === "vessel") {
    return { bow: p.x, stbd: p.y, up: p.z };
  }
  const { bow, stbd } = enuToVesselFrame(p.x, p.y, headingDeg);
  return { bow, stbd, up: p.z };
}

function drawFrame(
  canvas: HTMLCanvasElement,
  points: ViewerPoint[],
  headingDeg: number,
  viewFrame: LidarViewFrame,
  label: string,
  colorMode: "class" | "height"
) {
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  if (w < 8 || h < 8) return;

  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  canvas.width = Math.floor(w * dpr);
  canvas.height = Math.floor(h * dpr);

  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  let minSx = Infinity;
  let maxSx = -Infinity;
  let minSy = Infinity;
  let maxSy = -Infinity;
  let minZ = Infinity;
  let maxZ = -Infinity;

  type DrawPt = { sx: number; sy: number; z: number; cls: number; depth: number; hull: boolean };
  const projected: DrawPt[] = [];

  for (const p of points) {
    if (!Number.isFinite(p.x) || !Number.isFinite(p.y) || !Number.isFinite(p.z)) continue;
    const { bow, stbd, up } = toVesselAxes(p, headingDeg, viewFrame);
    const { sx, sy, depth } = projectObliqueSideView(bow, stbd, up);
    const hull = p.cls !== WATER_SURFACE_CLS;
    projected.push({ sx, sy, z: p.z, cls: p.cls, depth, hull });
    minZ = Math.min(minZ, p.z);
    maxZ = Math.max(maxZ, p.z);
    if (!hull) continue;
    minSx = Math.min(minSx, sx);
    maxSx = Math.max(maxSx, sx);
    minSy = Math.min(minSy, sy);
    maxSy = Math.max(maxSy, sy);
  }

  if (!projected.length) {
    ctx.fillStyle = "#0a0e14";
    ctx.fillRect(0, 0, w, h);
    ctx.fillStyle = "#f85149";
    ctx.font = "12px IBM Plex Sans, sans-serif";
    ctx.fillText("No drawable points in selection", 12, 24);
    return;
  }

  if (!Number.isFinite(minSx)) {
    for (const pt of projected) {
      minSx = Math.min(minSx, pt.sx);
      maxSx = Math.max(maxSx, pt.sx);
      minSy = Math.min(minSy, pt.sy);
      maxSy = Math.max(maxSy, pt.sy);
    }
  }

  const pad = 28;
  const rangeX = (maxSx - minSx || 1) * VIEW_BBOX_EXPAND;
  const rangeY = (maxSy - minSy || 1) * VIEW_BBOX_EXPAND;
  const scale = Math.min((w - pad * 2) / rangeX, (h - pad * 2 - 28) / rangeY);
  const midSx = (minSx + maxSx) / 2;
  const midSy = (minSy + maxSy) / 2;
  const ox = w / 2;
  const oy = h / 2;

  ctx.fillStyle = "#0a0e14";
  ctx.fillRect(0, 0, w, h);

  projected.sort((a, b) => a.depth - b.depth);

  const radius = Math.max(0.75, points.length > 8000 ? 0.7 : points.length > 2500 ? 0.95 : 1.25);

  for (const pt of projected) {
    const px = ox + (pt.sx - midSx) * scale;
    const py = oy + (pt.sy - midSy) * scale;
    if (px < -4 || py < -4 || px > w + 4 || py > h + 4) continue;
    ctx.fillStyle =
      colorMode === "height" ? heightColor(pt.z, minZ, maxZ) : classificationColor(pt.cls);
    ctx.globalAlpha = pt.hull ? 0.94 : 0.35;
    ctx.beginPath();
    ctx.arc(px, py, radius, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalAlpha = 1;

  ctx.fillStyle = "#8b949e";
  ctx.font = "11px IBM Plex Mono, monospace";
  ctx.fillText(label, 10, 16);
  ctx.fillStyle = "#6e7681";
  ctx.font = "10px IBM Plex Sans, sans-serif";
  ctx.fillText(
    `${points.length.toLocaleString()} pts · oblique side (port ↑, bow →)`,
    10,
    h - 8
  );
}

/** Canvas point cloud: fixed oblique side view (bow right, elevated port quarter). */
export function LidarPointCanvas({
  points,
  headingDeg = 0,
  viewFrame = "enu",
  label,
  colorMode = "class",
}: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const stateRef = useRef({ points, headingDeg, viewFrame, label, colorMode });

  stateRef.current = { points, headingDeg, viewFrame, label, colorMode };

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !points.length) return;

    const render = () => {
      const s = stateRef.current;
      drawFrame(
        canvas,
        s.points,
        s.headingDeg ?? 0,
        s.viewFrame ?? "enu",
        s.label,
        s.colorMode ?? "class"
      );
    };

    render();

    const ro = new ResizeObserver(() => render());
    ro.observe(canvas);
    return () => ro.disconnect();
  }, [points, headingDeg, viewFrame, label, colorMode]);

  return (
    <canvas
      ref={canvasRef}
      className="lidar-point-canvas"
      role="img"
      aria-label="LiDAR point cloud side view"
    />
  );
}