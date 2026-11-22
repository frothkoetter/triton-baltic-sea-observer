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

/** Demo audio when S3 HDF5 is unavailable (≤10 s synthetic tone). */
export function pseudoDasDemoAudioDataUrl(
  segmentIndex: number,
  fsHz = 2000,
  windowSeconds = 4
): string {
  const playSec = Math.min(10, Math.max(0.5, windowSeconds || 4));
  const sampleRate = Math.min(8000, Math.max(1000, Math.floor(fsHz / 4)));
  const nSamples = Math.floor(sampleRate * playSec);
  const dataBytes = nSamples * 2;
  const buffer = new ArrayBuffer(44 + dataBytes);
  const view = new DataView(buffer);
  const writeStr = (offset: number, s: string) => {
    for (let i = 0; i < s.length; i++) view.setUint8(offset + i, s.charCodeAt(i));
  };
  writeStr(0, "RIFF");
  view.setUint32(4, 36 + dataBytes, true);
  writeStr(8, "WAVE");
  writeStr(12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, 1, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  writeStr(36, "data");
  view.setUint32(40, dataBytes, true);
  const freq = 60 + (segmentIndex % 8) * 14;
  for (let i = 0; i < nSamples; i++) {
    const t = i / sampleRate;
    const amp = 0.12 * Math.sin(2 * Math.PI * freq * t);
    view.setInt16(44 + i * 2, Math.max(-32768, Math.min(32767, Math.floor(amp * 32767))), true);
  }
  const bytes = new Uint8Array(buffer);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return `data:audio/wav;base64,${btoa(binary)}`;
}