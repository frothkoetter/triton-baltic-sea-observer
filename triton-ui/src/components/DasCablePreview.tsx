import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { fetchJson } from "../api";
import type { DasCableSegment } from "../lib/das-cable";
import { resolveDasHdfsUrl } from "../lib/das-cable";
import { pseudoDasDemoAudioDataUrl, pseudoDasSpectrogram } from "../lib/das-preview";
import { DasSpectrogramCanvas } from "./DasSpectrogramCanvas";

type PreviewPayload = {
  ok?: boolean;
  error?: string;
  hint?: string;
  spectrogram?: { values?: number[][]; frequencies_hz?: number[] };
  audio?: { wav_base64?: string; duration_sec?: number; sample_rate_hz?: number; caption?: string };
  segment_id?: string;
};

type Props = {
  segment: DasCableSegment | null;
};

export function DasCablePreview({ segment }: Props) {
  const [loading, setLoading] = useState(false);
  const [source, setSource] = useState<"s3" | "pseudo" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [hint, setHint] = useState<string | null>(null);
  const [specValues, setSpecValues] = useState<number[][] | null>(null);
  const [freqs, setFreqs] = useState<number[] | undefined>();
  const [audioUrl, setAudioUrl] = useState<string | null>(null);
  const [audioCaption, setAudioCaption] = useState<string>("");
  const audioObjectUrl = useRef<string | null>(null);

  const setPlaybackUrl = useCallback((b64: string | null, fallback?: string | null) => {
    if (audioObjectUrl.current) {
      URL.revokeObjectURL(audioObjectUrl.current);
      audioObjectUrl.current = null;
    }
    if (b64) {
      try {
        const bin = atob(b64);
        const bytes = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
        const url = URL.createObjectURL(new Blob([bytes], { type: "audio/wav" }));
        audioObjectUrl.current = url;
        setAudioUrl(url);
        return;
      } catch {
        /* fall through */
      }
    }
    setAudioUrl(fallback ?? null);
  }, []);

  useEffect(
    () => () => {
      if (audioObjectUrl.current) URL.revokeObjectURL(audioObjectUrl.current);
    },
    []
  );

  const hdfsUrl = useMemo(() => resolveDasHdfsUrl(segment), [segment]);

  const pseudo = useMemo(() => {
    if (!segment) return null;
    return pseudoDasSpectrogram(
      String(segment.cable_asset ?? "cable"),
      Number(segment.segment_index ?? 0),
      String(segment.threat_class ?? segment.predicted_threat_type ?? "ambient")
    );
  }, [segment]);

  const applyPseudo = useCallback(
    (reason?: string) => {
      if (!pseudo || !segment) return;
      setSpecValues(pseudo.values);
      setFreqs(pseudo.frequencies_hz);
      setSource("pseudo");
      const idx = Number(segment.segment_index ?? 0);
      const fs = Number(segment.fs_hz ?? 2000);
      const win = Number(segment.window_seconds ?? 4);
      setPlaybackUrl(null, pseudoDasDemoAudioDataUrl(idx, fs, win));
      setAudioCaption(`Demo strain-rate tone · seg ${idx} (not from HDF5)`);
      if (reason) {
        setError(reason);
        setHint(null);
      }
    },
    [pseudo, segment]
  );

  useEffect(() => {
    setSpecValues(null);
    setFreqs(undefined);
    setPlaybackUrl(null, null);
    setError(null);
    setHint(null);
    setSource(null);
    if (!segment) return;

    if (!hdfsUrl) {
      applyPseudo("No hadoop_file_url on segment — set NiFi PutCDPObjectStore → hadoop.file.url");
      return;
    }

    const uri = encodeURIComponent(hdfsUrl);
    const idx = Number(segment.segment_index ?? 0);
    let cancelled = false;
    setLoading(true);
    applyPseudo();

    void (async () => {
      const res = await fetchJson<PreviewPayload>(
        `/api/das-cable/preview?hadoop_file_url=${uri}&segment_index=${idx}`
      );
      if (cancelled) return;
      setLoading(false);

      const values = res.spectrogram?.values;
      if (res.ok && values && values.length > 0 && values[0]?.length) {
        setSpecValues(values);
        setFreqs(res.spectrogram?.frequencies_hz);
        setSource("s3");
        setError(null);
        setHint(null);
        const b64 = res.audio?.wav_base64;
        const dur = Number(res.audio?.duration_sec ?? 0);
        const idx = Number(segment.segment_index ?? 0);
        const fs = Number(segment.fs_hz ?? 2000);
        const win = Number(segment.window_seconds ?? 4);
        const demo = pseudoDasDemoAudioDataUrl(idx, fs, win);
        if (b64 && dur >= 0.2) {
          setPlaybackUrl(b64);
          setAudioCaption(
            res.audio?.caption ??
              `HDF5 · ${res.audio?.duration_sec ?? "?"} s @ ${res.audio?.sample_rate_hz ?? "?"} Hz`
          );
        } else if (b64) {
          setPlaybackUrl(b64, demo);
          setAudioCaption(
            `${res.audio?.caption ?? "HDF5"} · short waveform — demo tone mixed for playback`
          );
        } else {
          setPlaybackUrl(null, demo);
          setAudioCaption("HDF5 spectrogram only — demo audio (no WAV in segment)");
        }
      } else {
        applyPseudo(res.error ?? "Could not load HDF5 from object store");
        if (res.hint) setHint(res.hint);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [hdfsUrl, segment, applyPseudo, setPlaybackUrl]);

  if (!segment) return null;

  const grid = specValues ?? pseudo?.values ?? [];

  return (
    <div className="das-preview-block">
      <div className="lidar-viewer-toolbar">
        <span className="mono" style={{ fontSize: 10, color: "var(--text-muted)" }}>
          {loading
            ? "Loading HDF5…"
            : source === "s3"
              ? `S3 · seg ${segment.segment_index ?? 0}`
              : "Spectrogram + audio (demo until S3 HDF5 loads)"}
        </span>
      </div>
      {grid.length > 0 ? (
        <DasSpectrogramCanvas
          values={grid}
          frequenciesHz={freqs ?? pseudo?.frequencies_hz}
          label={segment.threat_class ?? "segment"}
        />
      ) : (
        <p className="lidar-hint">No spectrogram data for this segment.</p>
      )}
      {audioUrl ? (
        <div className="das-audio-row">
          <audio
            key={audioUrl}
            className="das-audio-player"
            controls
            preload="metadata"
            src={audioUrl}
          >
            DAS audio preview
          </audio>
          <p className="lidar-hint">
            {audioCaption}
            {source === "s3" ? " (max 10 s from HDF5)" : " · configure S3 for live HDF5 audio"}
          </p>
        </div>
      ) : null}
      {error && source !== "s3" ? (
        <p className="lidar-hint" style={{ color: "var(--status-unknown)" }}>
          {error}
          {hint ? ` — ${hint}` : null}
        </p>
      ) : null}
    </div>
  );
}