# LiDAR NiFi — „No space left on device“

## Symptom

```text
LidarSimulator … FlowFileAccessException: Could not write to StandardFlowFileRecord
Caused by: java.io.IOException: No space left on device
```

The processor fails while writing the **FlowFile body** into NiFi’s **content repository** on the pod volume — not while keeping `.las` files under `/tmp`.

## What `LidarSimulator` does (code)

From `upload/LidarSimulator.py`:

1. Builds one vessel scene (~**9k points**, ~**300 KB** LAS per run in a quick local test).
2. Writes LAS under `tempfile.TemporaryDirectory(prefix="lidar_sim_")` → **deleted** after `transform()` returns.
3. Returns `contents=payload` (binary LAS) on the FlowFile → stored in the **content repository** until downstream processors finish.

So: **no intentional local LAS archive in the Python processor**; disk pressure is **NiFi repo + Python extension venvs + other flows**.

## Showcase flow (`LiDar Simulation` in `nifi-flow/proj_triton_showcase-version-2.json`)

| Step | Processor | Content on the wire |
|------|-----------|---------------------|
| 1 / min | `GenerateFlowFile` | empty trigger |
| | `LidarSimulator` | **full LAS** (~300 KB+) |
| | `PutCDPObjectStore` | still **LAS** (upload to `#{s3 bucket lidar}` / `#{s3 lidar Directory}`) |
| | `UpdateAttribute` | still LAS |
| | `ReplaceText` | **small JSON** for classifier API |
| | `InvokeHTTP` | JSON |
| | `AttributesToJSON` → `MergeContent` | JSON metadata batches (min **100** records, max bin age **15 min**) |
| | `PutIceberg` | `defense.lidar_simulation_metadata` |

If **PutCDPObjectStore** or **InvokeHTTP** is slow or failing, LAS FlowFiles accumulate on connections (backpressure up to **10 000** files / **1 GB** per queue). That fills a small NiFi PVC quickly, and **LidarSimulator** cannot allocate the next FlowFile.

## Check on the NiFi node / pod

```bash
df -h
du -sh /opt/nifi/nifi-current/content_repository \
       /opt/nifi/nifi-current/flowfile_repository \
       /opt/nifi/nifi-current/provenance_repository \
       /var/opt/nifi/python/work/extensions 2>/dev/null
```

In **NiFi UI**:

1. **Summary** → JVM / disk; **FlowFiles queued** (global).
2. Open **LiDar Simulation** → right‑click connections → **List queue** (count + data size).
3. **LidarSimulator** / **PutCDPObjectStore** / **InvokeHTTP** → **Bulletins** and last 5 min throughput / failures.

## Remediation (ops)

1. **Stop** the `LiDar Simulation` group (or **GenerateFlowFile**) to stop new LAS.
2. Fix the blocker:
   - **PutCDPObjectStore** — bucket/RAZ/credentials, `cdp-storage-location`, `Directory`.
   - **InvokeHTTP** — classifier URL / auth; 1 s timeouts are tight for cold APIs.
3. **Empty queues** only after you accept losing stuck FlowFiles (LAS already in S3 if PutCDP succeeded).
4. **Prune Python extension venvs** if `du` under `python/work/extensions` is large (failed pip runs per `upload/requirements.txt` — keep that file **empty**; deps only on `ProcessorDetails.dependencies`).
5. **Enlarge** the NiFi data volume or move content repo to a larger PVC (platform).
6. **Purge provenance** / run NiFi’s content cleanup (per CDP runbook).

## Flow tuning (reduce repo pressure)

- Lower **GenerateFlowFile** rate (e.g. 5–15 min) while testing.
- After **PutCDPObjectStore** `success`, add **TruncateFlowFile** or **ReplaceText** to drop binary content and keep **attributes** only (`hadoop.file.url`, `lidar.*`) before UpdateAttribute — avoids carrying LAS through HTTP/classifier legs.
- Lower **LidarSimulator** property **Sea Surface Point Budget** (default 1200) if smaller files are acceptable.
- Set **MergeContent** **Minimum Number of Entries** lower (e.g. 10) for faster metadata commits during dev (watch Iceberg small files).

## Verify S3, not pod

Successful path: LAS in object store (`hadoop.file.url` / `s3a://…`), row in `iceberg.defense.lidar_simulation_metadata`. NATO UI LiDAR tab reads via S3 — not from NiFi local disk.

```sql
-- Trino
SELECT hadoop_file_url, vessel_type, ingested_at
FROM iceberg.defense.lidar_simulation_metadata
ORDER BY ingested_at DESC
LIMIT 10;
```

## Related

- `nifi-processors/README.md` — LiDAR disk + empty `requirements.txt`
- `upload/requirements.txt` — `[Errno 28]` under extension venvs