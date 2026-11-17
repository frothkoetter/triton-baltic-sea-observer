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

/** High-density canvas point cloud (isometric XYZ). */
export function LidarPointCanvas({ points, headingDeg = 0, label, colorMode = "class" }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !points.length) return;

    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;
    canvas.width = Math.floor(w * dpr);
    canvas.height = Math.floor(h * dpr);

    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const head = (headingDeg * Math.PI) / 180;
    const ch = Math.cos(head);
    const sh = Math.sin(head);

    let minX = Infinity;
    let maxX = -Infinity;
    let minY = Infinity;
    let maxY = -Infinity;
    let minZ = Infinity;
    let maxZ = -Infinity;

    const projected: { sx: number; sy: number; z: number; cls: number }[] = [];

    for (const p of points) {
      const rx = p.x * ch - p.y * sh;
      const ry = p.x * sh + p.y * ch;
      const sx = rx * ISO_X + p.z * ISO_Z;
      const sy = -ry + p.z * ISO_Z * 0.65;
      projected.push({ sx, sy, z: p.z, cls: p.cls });
      minX = Math.min(minX, sx);
      maxX = Math.max(maxX, sx);
      minY = Math.min(minY, sy);
      maxY = Math.max(maxY, sy);
      minZ = Math.min(minZ, p.z);
      maxZ = Math.max(maxZ, p.z);
    }

    const pad = 16;
    const rangeX = maxX - minX || 1;
    const rangeY = maxY - minY || 1;
    const scale = Math.min((w - pad * 2) / rangeX, (h - pad * 2 - 24) / rangeY);
    const midX = (minX + maxX) / 2;
    const midY = (minY + maxY) / 2;
    const ox = w / 2;
    const oy = (h - 8) / 2 + 8;

    ctx.fillStyle = "#0a0e14";
    ctx.fillRect(0, 0, w, h);

    projected.sort((a, b) => a.sy - b.sy);

    const radius = points.length > 6000 ? 0.65 : points.length > 2500 ? 0.85 : 1.1;

    for (const pt of projected) {
      const px = ox + (pt.sx - midX) * scale;
      const py = oy + (pt.sy - midY) * scale;
      ctx.fillStyle =
        colorMode === "height"
          ? heightColor(pt.z, minZ, maxZ)
          : classificationColor(pt.cls);
      ctx.globalAlpha = 0.92;
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