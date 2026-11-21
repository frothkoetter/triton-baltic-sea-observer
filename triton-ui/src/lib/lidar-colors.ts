/** ASPRS classification colors (LiDAR viewer). */
export function classificationColor(cls: number): string {
  const table: Record<number, string> = {
    0: "#ffd54f",
    1: "#8b949e",
    2: "#3fb950",
    3: "#58a6ff",
    4: "#f0883e",
    5: "#4ea1ff",
    6: "#a371f7",
    7: "#ff7b72",
    8: "#d29922",
    9: "#e3b341",
    10: "#484f58",
    11: "#56d364",
    12: "#79c0ff",
  };
  return table[cls] ?? "#79c0ff";
}

export function heightColor(z: number, zMin: number, zMax: number): string {
  const t = zMax > zMin ? (z - zMin) / (zMax - zMin) : 0.5;
  const h = Math.round(200 + t * 55);
  const s = 70;
  const l = Math.round(28 + t * 42);
  return `hsl(${h}, ${s}%, ${l}%)`;
}