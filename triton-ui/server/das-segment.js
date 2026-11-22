/** Normalize DAS metadata rows for UI preview (S3 URI for HDF5 batch). */

export function resolveDasHdfsUrl(row) {
  const hadoop = String(row?.hadoop_file_url ?? "").trim();
  if (/^s3a?:\/\//i.test(hadoop)) {
    return hadoop;
  }
  const filename = String(row?.filename ?? "").trim();
  if (/^s3a?:\/\//i.test(filename)) {
    return filename;
  }
  if (filename) {
    for (const key of ("S3_DAS_DEFAULT_PREFIX", "S3_LIDAR_ALLOWED_PREFIX")) {
      const prefix = String(process.env[key] ?? "").trim();
      if (prefix) {
        const base = prefix.replace(/\/$/, "");
        const leaf = filename.replace(/^\//, "");
        return `${base}/${leaf}`;
      }
    }
  }
  return hadoop || null;
}

export function normalizeDasSegment(row) {
  const url = resolveDasHdfsUrl(row);
  return {
    ...row,
    ...(url ? { hadoop_file_url: url } : {}),
  };
}