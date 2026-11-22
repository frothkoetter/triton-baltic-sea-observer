import { useEffect, useRef } from "react";

type Props = {
  values: number[][];
  frequenciesHz?: number[];
  label?: string;
};

function lerp(a: number, b: number, t: number) {
  return a + (b - a) * t;
}

function spectrogramColor(t: number): string {
  const x = Math.max(0, Math.min(1, t));
  const r = Math.round(lerp(12, 245, x));
  const g = Math.round(lerp(28, 180, x * 0.85));
  const b = Math.round(lerp(64, 55, 1 - x));
  return `rgb(${r},${g},${b})`;
}

export function DasSpectrogramCanvas({ values, frequenciesHz, label }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !values.length) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const rows = values.length;
    const cols = values[0]?.length ?? 0;
    if (!cols) return;

    let max = 0;
    for (const row of values) {
      for (const v of row) {
        if (v > max) max = v;
      }
    }
    max = max || 1;

    const dpr = window.devicePixelRatio || 1;
    const w = canvas.clientWidth || 360;
    const h = canvas.clientHeight || 220;
    canvas.width = Math.floor(w * dpr);
    canvas.height = Math.floor(h * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    ctx.fillStyle = "#0b1220";
    ctx.fillRect(0, 0, w, h);

    const cellW = w / cols;
    const cellH = h / rows;
    for (let r = 0; r < rows; r++) {
      for (let c = 0; c < cols; c++) {
        const t = values[r][c] / max;
        ctx.fillStyle = spectrogramColor(t);
        ctx.fillRect(c * cellW, (rows - 1 - r) * cellH, cellW + 1, cellH + 1);
      }
    }

    ctx.fillStyle = "rgba(200,220,240,0.75)";
    ctx.font = "10px var(--font-mono, monospace)";
    const f0 = frequenciesHz?.[0];
    const f1 = frequenciesHz?.[frequenciesHz.length - 1];
    if (f0 != null && f1 != null) {
      ctx.fillText(`${f0.toFixed(0)} Hz`, 4, h - 4);
      ctx.fillText(`${f1.toFixed(0)} Hz`, 4, 12);
    }
    if (label) {
      ctx.fillText(label, w - 8 - ctx.measureText(label).width, 12);
    }
  }, [values, frequenciesHz, label]);

  return (
    <canvas
      ref={canvasRef}
      className="das-spectrogram-canvas"
      aria-label="DAS spectrogram preview"
      role="img"
    />
  );
}