import { useEffect, useRef } from "react";
import { classificationColor, heightColor } from "../lib/lidar-colors";

export type ViewerPoint = {
  x: number;
  y: number;
  z: number;
  cls: number;
  intensity?: number;
};

type Props = {
  points: ViewerPoint[];
  headingDeg?: number;
  label: string;
  colorMode?: "class" | "height";
};

const ISO_X = 0.866;
const ISO_Z = 0.5;
const PITCH = 0.72;

function drawFrame(
  canvas: HTMLCanvasElement,
  points: ViewerPoint[],
  headingDeg: number,
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

  const head = (headingDeg * Math.PI) / 180;
  const ch = Math.cos(head);
  const sh = Math.sin(head);
  const cp = Math.cos(PITCH);
  const sp = Math.sin(PITCH);

  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  let minZ = Infinity;
  let maxZ = -Infinity;

  const projected: { sx: number; sy: number; z: number; cls: number; depth: number }[] = [];

  for (const p of points) {
    if (!Number.isFinite(p.x) || !Number.isFinite(p.y) || !Number.isFinite(p.z)) continue;
    const rx = p.x * ch - p.y * sh;
    const ry = p.x * sh + p.y * ch;
    const ryTilt = ry * cp - p.z * sp;
    const rzLift = ry * sp + p.z * cp;
    const sx = rx * ISO_X + rzLift * ISO_Z;
    const sy = -ryTilt + rzLift * ISO_Z * 0.65;
    const depth = sx + sy;
    projected.push({ sx, sy, z: p.z, cls: p.cls, depth });
    minX = Math.min(minX, sx);
    maxX = Math.max(maxX, sx);
    minY = Math.min(minY, sy);
    maxY = Math.max(maxY, sy);
    minZ = Math.min(minZ, p.z);
    maxZ = Math.max(maxZ, p.z);
  }

  if (!projected.length) {
    ctx.fillStyle = "#0a0e14";
    ctx.fillRect(0, 0, w, h);
    ctx.fillStyle = "#f85149";
    ctx.font = "12px IBM Plex Sans, sans-serif";
    ctx.fillText("No drawable points in selection", 12, 24);
    return;
  }

  const pad = 18;
  const rangeX = maxX - minX || 1;
  const rangeY = maxY - minY || 1;
  const scale = Math.min((w - pad * 2) / rangeX, (h - pad * 2 - 28) / rangeY);
  const midX = (minX + maxX) / 2;
  const midY = (minY + maxY) / 2;
  const ox = w / 2;
  const oy = (h - 10) / 2 + 10;

  ctx.fillStyle = "#0a0e14";
  ctx.fillRect(0, 0, w, h);

  projected.sort((a, b) => a.depth - b.depth);

  const radius = Math.max(0.75, points.length > 8000 ? 0.7 : points.length > 2500 ? 0.95 : 1.25);

  for (const pt of projected) {
    const px = ox + (pt.sx - midX) * scale;
    const py = oy + (pt.sy - midY) * scale;
    if (px < -4 || py < -4 || px > w + 4 || py > h + 4) continue;
    ctx.fillStyle =
      colorMode === "height" ? heightColor(pt.z, minZ, maxZ) : classificationColor(pt.cls);
    ctx.globalAlpha = 0.94;
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
  ctx.fillText(`${points.length.toLocaleString()} pts · isometric XYZ`, 10, h - 8);
}

/** High-density canvas point cloud (isometric XYZ). */
export function LidarPointCanvas({ points, headingDeg = 0, label, colorMode = "class" }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const stateRef = useRef({ points, headingDeg, label, colorMode });

  stateRef.current = { points, headingDeg, label, colorMode };

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !points.length) return;

    const render = () => {
      const s = stateRef.current;
      drawFrame(canvas, s.points, s.headingDeg ?? 0, s.label, s.colorMode ?? "class");
    };

    render();

    const ro = new ResizeObserver(() => render());
    ro.observe(canvas);
    return () => ro.disconnect();
  }, [points, headingDeg, label, colorMode]);

  return (
    <canvas
      ref={canvasRef}
      className="lidar-point-canvas"
      role="img"
      aria-label="LiDAR point cloud"
    />
  );
}