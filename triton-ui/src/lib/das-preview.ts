/** Stylized spectrogram when S3 HDF5 preview is unavailable (local / demo). */
export function pseudoDasSpectrogram(
  cableAsset: string,
  segmentIndex: number,
  threat: string
): { values: number[][]; frequencies_hz: number[] } {
  const rows = 64;
  const cols = 96;
  const seed = (cableAsset.length * 7 + segmentIndex * 13 + threat.length * 3) % 997;
  const values: number[][] = [];
  for (let r = 0; r < rows; r++) {
    const row: number[] = [];
    for (let c = 0; c < cols; c++) {
      const band = Math.exp(-((r - 20 - (seed % 15)) ** 2) / 180);
      const tone = 0.35 + 0.65 * Math.sin((c + seed) * 0.22) ** 2;
      row.push(band * tone + ((seed + r * c) % 17) * 0.01);
    }
    values.push(row);
  }
  const frequencies_hz = Array.from({ length: rows }, (_, i) => 2 + i * 12.5);
  return { values, frequencies_hz };
}