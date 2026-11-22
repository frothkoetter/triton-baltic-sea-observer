/** Whether S3 bridges (LiDAR LAS, DAS HDF5) may run — aligned with server/s3_boto.py. */

function onCmlWorkbench() {
  return Boolean(process.env.CDSW_PROJECT_ID || process.env.CDSW_WORKING_DIRECTORY);
}

function explicitKeysOnly() {
  return ["1", "true", "yes"].includes(
    String(process.env.S3_USE_EXPLICIT_KEYS ?? "").toLowerCase()
  );
}

function explicitKeysPresent() {
  return Boolean(
    (process.env.S3_WORKLOAD_USER && process.env.S3_WORKLOAD_PASSWORD) ||
      (process.env.S3_ACCESS_KEY_ID && process.env.S3_SECRET_ACCESS_KEY) ||
      (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY)
  );
}

export function s3Configured() {
  if (explicitKeysPresent()) {
    return true;
  }
  const lidarCml = String(process.env.LIDAR_USE_CML_S3 ?? "auto").toLowerCase();
  if (lidarCml === "false" || lidarCml === "0" || lidarCml === "no") {
    return false;
  }
  if (lidarCml === "true" || lidarCml === "1") {
    return true;
  }
  // auto: on CML try project data connection (cml.data_v1) unless forced explicit keys
  if (onCmlWorkbench() && !explicitKeysOnly()) {
    return true;
  }
  return false;
}

export function s3ConfigHint() {
  if (explicitKeysPresent()) {
    return "using explicit S3 keys";
  }
  if (onCmlWorkbench()) {
    return (
      "bind project Data Connection (default name: S3 Object Store) or set " +
      "S3_ACCESS_KEY_ID + S3_SECRET_ACCESS_KEY on the Application"
    );
  }
  return "set S3_WORKLOAD_USER/S3_WORKLOAD_PASSWORD or S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY";
}