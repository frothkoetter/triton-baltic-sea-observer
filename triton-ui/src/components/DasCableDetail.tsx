import type { DasCableSegment } from "../lib/das-cable";
import { dasThreatColor } from "../lib/das-cable";
import { DasCablePreview } from "./DasCablePreview";

type Props = { segment: DasCableSegment | null };

export function DasCableDetail({ segment }: Props) {
  if (!segment) {
    return <p className="lidar-preview empty">Wählen Sie ein Segment auf der Karte oder in der Tabelle.</p>;
  }
  const threat = segment.predicted_threat_type ?? segment.threat_class ?? "ambient";
  const color = dasThreatColor(threat);
  return (
    <div className="lidar-preview">
      <div className="lidar-preview-title">{segment.cable_name ?? segment.cable_asset}</div>
      <span className="type-pill" style={{ borderColor: color, color }}>
        {threat}
      </span>
      <DasCablePreview segment={segment} />
      <table className="lidar-meta-table">
        <tbody>
          <tr>
            <th>Chainage</th>
            <td>{segment.chainage_m != null ? `${Math.round(segment.chainage_m)} m` : "—"}</td>
          </tr>
          <tr>
            <th>Position</th>
            <td className="mono">
              {segment.segment_lat?.toFixed(4)}, {segment.segment_lon?.toFixed(4)}
            </td>
          </tr>
          <tr>
            <th>ML confidence</th>
            <td>
              {segment.model_confidence != null ? segment.model_confidence.toFixed(2) : "—"}
            </td>
          </tr>
          <tr>
            <th>Channels / fs</th>
            <td>
              {segment.channel_count ?? "—"} ch · {segment.fs_hz ?? "—"} Hz
            </td>
          </tr>
          <tr>
            <th>HDF5 batch</th>
            <td className="lidar-uri">{segment.hadoop_file_url ?? segment.filename ?? "—"}</td>
          </tr>
          <tr>
            <th>Ingested</th>
            <td>{segment.ingested_at ?? "—"}</td>
          </tr>
        </tbody>
      </table>
      <p className="lidar-hint">
        Spectrogram + audio (≤10 s) load from HDF5 via S3 when credentials are configured; otherwise a stylized preview is shown.
      </p>
    </div>
  );
}