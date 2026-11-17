import { useMemo } from "react";
import type { LidarScan } from "../lib/lidar";
import { classificationLabel, pseudoLidarPoints } from "../lib/lidar";

type Props = {
  scan: LidarScan | null;
};

const CLS_COLOR: Record<number, string> = {
  0: "#ffd54f",
  2: "#3fb950",
  5: "#4ea1ff",
  6: "#a371f7",
  10: "#6e7681",
};

export function LidarPointPreview({ scan }: Props) {
  const points = useMemo(() => {
    if (!scan) return [];
    const vt = String(scan.vessel_type ?? "undefined");
    return pseudoLidarPoints(scan.filename, vt, 240);
  }, [scan]);

  if (!scan) {
    return (
      <div className="lidar-preview empty">
        <p>Select a scene on the map or in the table.</p>
      </div>
    );
  }

  const heading = Number(scan.heading_deg ?? 0) * (Math.PI / 180);
  const cos = Math.cos(heading);
  const sin = Math.sin(heading);

  return (
    <div className="lidar-preview">
      <h3 className="lidar-preview-title">
        {scan.vessel_name ?? "UNCLASSIFIED"}
        <span className="type-pill">{scan.vessel_type}</span>
      </h3>
      <svg viewBox="0 0 320 200" className="lidar-preview-svg" role="img" aria-label="LiDAR point cloud preview">
        <rect width="320" height="200" fill="#0d1117" />
        <text x="8" y="16" fill="#8b949e" fontSize="9" fontFamily="monospace">
          Pseudo preview · {scan.filename}
        </text>
        {points.map((p, i) => {
          const rx = p.x * cos - p.y * sin;
          const ry = p.x * sin + p.y * cos;
          const cx = 160 + rx * 2.2;
          const cy = 110 - ry * 2.2;
          const fill = CLS_COLOR[p.cls] ?? "#79c0ff";
          return <circle key={i} cx={cx} cy={cy} r={1.1} fill={fill} opacity={0.9} />;
        })}
        <text x="8" y="192" fill="#6e7681" fontSize="8" fontFamily="sans-serif">
          Full LAS: {scan.hadoop_file_url ? "object store" : "—"}
        </text>
      </svg>
      <table className="lidar-meta-table">
        <tbody>
          <tr>
            <th>Classification</th>
            <td>{classificationLabel(scan.classification)}</td>
          </tr>
          <tr>
            <th>Center</th>
            <td className="mono">
              {scan.center_lat?.toFixed(5)}°N, {scan.center_lon?.toFixed(5)}°E
            </td>
          </tr>
          <tr>
            <th>Heading</th>
            <td>{scan.heading_deg ?? "—"}°</td>
          </tr>
          <tr>
            <th>Points</th>
            <td>{scan.point_count?.toLocaleString() ?? "—"}</td>
          </tr>
          <tr>
            <th>ML predict</th>
            <td>
              {scan.predicted_vessel_type ?? "—"}
              {scan.model_confidence != null ? ` (${(scan.model_confidence * 100).toFixed(0)}%)` : ""}
            </td>
          </tr>
          <tr>
            <th>Ingested</th>
            <td className="mono">{scan.ingested_at ?? "—"}</td>
          </tr>
          <tr>
            <th>LAS URI</th>
            <td className="mono lidar-uri">{scan.hadoop_file_url ?? "—"}</td>
          </tr>
        </tbody>
      </table>
      <p className="lidar-hint">
        NiFi <em>LidarSimulator</em> → <code>iceberg.defense.lidar_simulation_metadata</code>. COA{" "}
        <strong>C</strong> (NBA) uses ALB-LiDAR when depth ≤ 15 m and turbidity low.
      </p>
    </div>
  );
}