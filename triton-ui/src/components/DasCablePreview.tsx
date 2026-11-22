import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "../api";
import type { DasCableSegment } from "../lib/das-cable";
import { pseudoDasSpectrogram } from "../lib/das-preview";
import { DasSpectrogramCanvas } from "./DasSpectrogramCanvas";

type PreviewPayload = {
  ok?: boolean;
  error?: string;
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
  const [specValues, setSpecValues] = useState<number[][] | null>(null);
  const [freqs, setFreqs] = useState<number[] | undefined>();
  const [audioUrl, setAudioUrl] = useState<string | null>(null);
  const [audioCaption, setAudioCaption] = useState<string>("");

  const pseudo = useMemo(() => {
    if (!segment) return null;
    return pseudoDasSpectrogram(
      String(segment.cable_asset ?? "cable"),
      Number(segment.segment_index ?? 0),
      String(segment.threat_class ?? segment.predicted_threat_type ?? "ambient")
    );
  }, [segment]);

  useEffect(() => {
    setSpecValues(null);
    setFreqs(undefined);
    setAudioUrl(null);
    setError(null);
    setSource(null);
    if (!segment?.hadoop_file_url) return;

    const uri = encodeURIComponent(segment.hadoop_file_url);
    const idx = Number(segment.segment_index ?? 0);
    let cancelled = false;
    setLoading(true);

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
        const b64 = res.audio?.wav_base64;
        if (b64) {
          setAudioUrl(`data:audio/wav;base64,${b64}`);
          setAudioCaption(
            res.audio?.caption ??
              `Preview · ${res.audio?.duration_sec ?? "?"} s @ ${res.audio?.sample_rate_hz ?? "?"} Hz`
          );
        }
        setError(null);
      } else {
        setError(res.error ?? "Could not load HDF5 from object store");
        setSource("pseudo");
        if (pseudo) {
          setSpecValues(pseudo.values);
          setFreqs(pseudo.frequencies_hz);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [segment?.hadoop_file_url, segment?.segment_index, pseudo]);

  if (!segment) return null;

  const grid = specValues ?? pseudo?.values ?? [];
  const freqAxis = freqs ?? pseudo?.frequencies_hz;

  return (
    <div className="das-preview-block">
      <div className="lidar-viewer-toolbar">
        <span className="mono" style={{ fontSize: 10, color: "var(--text-muted)" }}>
          {loading
            ? "Loading HDF5…"
            : source === "s3"
              ? `S3 · seg ${segment.segment_index ?? 0}`
              : "Spectrogram preview"}
        </span>
      </div>
      <DasSpectrogramCanvas
        values={grid}
        frequenciesHz={freqAxis}
        label={segment.threat_class ?? "segment"}
      />
      {audioUrl ? (
        <div className="das-audio-row">
          <audio className="das-audio-player" controls preload="none" src={audioUrl}>
            DAS audio preview
          </audio>
          <p className="lidar-hint">{audioCaption} (max 10 s)</p>
        </div>
      ) : !loading && source !== "s3" ? (
        <p className="lidar-hint">Audio requires S3 HDF5 preview.</p>
      ) : null}
      {error && source !== "s3" ? (
        <p className="lidar-hint" style={{ color: "var(--status-unknown)" }}>
          S3: {error} — showing stylized spectrogram.
        </p>
      ) : null}
    </div>
  );
}