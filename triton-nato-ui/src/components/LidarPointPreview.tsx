import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "../api";
import type { LidarScan } from "../lib/lidar";
import { classificationLabel, normalizeViewerPoints, pseudoLidarPoints } from "../lib/lidar";
import { LidarPointCanvas, type ViewerPoint } from "./LidarPointCanvas";

type Props = {
  scan: LidarScan | null;
};

export function LidarPointPreview({ scan }: Props) {
  const [livePoints, setLivePoints] = useState<ViewerPoint[] | null>(null);
  const [totalPoints, setTotalPoints] = useState<number | null>(null);
  const [lasSource, setLasSource] = useState<"s3" | "pseudo" | null>(null);
  const [lasError, setLasError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [colorMode, setColorMode] = useState<"class" | "height">("class");

  const pseudo = useMemo(() => {
    if (!scan) return [];
    const vt = String(scan.vessel_type ?? "undefined");
    return pseudoLidarPoints(scan.filename, vt, 2200);
  }, [scan]);

  useEffect(() => {
    setLivePoints(null);
    setTotalPoints(null);
    setLasSource(null);
    setLasError(null);
    if (!scan?.hadoop_file_url) return;
    const uri = encodeURIComponent(scan.hadoop_file_url);
    let cancelled = false;
    setLoading(true);
    void (async () => {
      const res = await fetchJson<{
        ok?: boolean;
        points?: unknown;
        point_count_total?: number;
        error?: string;
      }>(`/api/lidar/points?hadoop_file_url=${uri}`);
      if (cancelled) return;
      setLoading(false);
      const normalized = res.ok ? normalizeViewerPoints(res.points) : [];
      if (normalized.length >= 50) {
        setLivePoints(normalized);
        setTotalPoints(res.point_count_total ?? normalized.length);
        setLasSource("s3");
        setLasError(null);
      } else {
        setLasError(
          res.error ??
            (res.ok ? "LAS returned no valid points (check bridge version)" : "Could not load LAS from object store")
        );
        setLasSource("pseudo");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [scan?.hadoop_file_url]);

  const points = livePoints && livePoints.length >= 50 ? livePoints : pseudo;
  const previewLabel =
    lasSource === "s3"
      ? `S3 LAS · ${scan?.filename ?? ""}`
      : loading
        ? "Loading LAS…"
        : `Preview · ${scan?.filename ?? ""}`;

  if (!scan) {
    return (
      <div className="lidar-preview empty">
        <p>Select a scene on the map or in the table.</p>
      </div>
    );
  }

  return (
    <div className="lidar-preview">
      <h3 className="lidar-preview-title">
        {scan.vessel_name ?? "UNCLASSIFIED"}
        <span className="type-pill">{scan.vessel_type}</span>
      </h3>
      <div className="lidar-viewer-toolbar">
        <span className="mono" style={{ fontSize: 10, color: "var(--text-muted)" }}>
          {previewLabel}
        </span>
        <label className="lidar-filter">
          Color
          <select
            value={colorMode}
            onChange={(e) => setColorMode(e.target.value as "class" | "height")}
          >
            <option value="class">ASPRS class</option>
            <option value="height">Height (Z)</option>
          </select>
        </label>
      </div>
      <LidarPointCanvas
        points={points}
        headingDeg={Number(scan.heading_deg ?? 0)}
        label={previewLabel}
        colorMode={colorMode}
      />
      {lasError && lasSource !== "s3" ? (
        <p className="lidar-hint" style={{ color: "var(--status-unknown)" }}>
          S3: {lasError} — showing stylized preview.
        </p>
      ) : null}
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
            <td>
              {scan.point_count?.toLocaleString() ?? totalPoints?.toLocaleString() ?? "—"}
              {livePoints
                ? ` (rendering ${livePoints.length.toLocaleString()}${
                    totalPoints ? ` of ${totalPoints.toLocaleString()}` : ""
                  })`
                : ""}
            </td>
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
    </div>
  );
}