# NiFi → LiDAR vessel classifier (CML Model Service)

Your Mac `curl` proves the **model endpoint and `accessKey` query param** work.  
NiFi **`Connect timed out`** means the **NiFi JVM never opened a TCP connection** to that host (firewall, DNS, wrong URL, or proxy)—not a missing Bearer token.

## Working request shape

**Minimum (query `accessKey` only):**

```bash
curl -sS -H "Content-Type: application/json" -X POST \
  "https://modelservice.ml-<workspace-id>.se-sandb.a465-9q4k.cloudera.site/model?accessKey=<MODEL_ACCESS_KEY>" \
  -d '{"request":{"s3_uri":"s3a://<bucket>/path/to/file.las"}}'
```

**Also valid (optional model API key as Bearer + same `accessKey`):** some deployments accept both; use the **model access key** from CML Model → Access Key, not a CDP workload JWT unless your docs say otherwise.

Expected response (example):

```json
{"success":true,"response":{"confidence":0.54,"vessel_type":"gas_tanker"},...}
```

For NiFi:

- **`accessKey`** must be on **HTTP URL**: `…/model?accessKey=${lidar.model.access.key}`
- **Optional:** dynamic property **`Authorization`** = `Bearer ${lidar.model.api.key}` if your environment requires it (same as local `f.sh`).
- Do **not** use a stale CDP Knox JWT or OpenAI-style token here unless you know the gateway expects it.

## InvokeHTTP settings (NiFi 2.x)

| Property | Value |
|----------|--------|
| **HTTP Method** | `POST` |
| **HTTP URL** | `https://modelservice.ml-….cloudera.site/model?accessKey=${lidar.model.access.key}` (Parameter Context; mark key **sensitive**) |
| **Request Content-Type** | `application/json` |
| **Request Body Enabled** | `true` |
| **Connection Timeout** | `60 sec` (or higher while testing) |
| **Socket Read Timeout** | `120 sec` or more (model reads LAS from S3) |
| **Authorization** (optional dynamic property) | `Bearer ${lidar.model.api.key}` if required; omit if `accessKey`-only curl works |

**Request body** (FlowFile content), e.g. after **ReplaceText** / **AttributesToJSON**:

```json
{"request":{"s3_uri":"${lidar.s3.uri}"}}
```

Use the **same S3 URI** NiFi wrote the LAS to (`s3a://…` is fine; the model normalizes `s3a` → `s3`).  
Set `lidar.s3.uri` from **PutObject** / **ListS3** / `hadoop.file.url` if your flow already has it.

Map response fields with **EvaluateJsonPath** on the response FlowFile, e.g.:

- `$.response.vessel_type` → `lidar.predicted_vessel_type`
- `$.response.confidence` → `lidar.model_confidence`

## Why Mac works but NiFi times out

1. **Run the same curl on the NiFi node** (or Data Flow worker pod), not only on your laptop:
   ```bash
   curl -v --connect-timeout 15 -H "Content-Type: application/json" -X POST \
     "https://modelservice.ml-….cloudera.site/model?accessKey=…" \
     -d '{"request":{"s3_uri":"s3a://goes-se-sandbox/data/user/…/file.las"}}'
   ```
   - **Works on Mac, fails on NiFi** → egress / firewall / private cluster: open HTTPS to `*.cloudera.site` from NiFi, or ask Cloudera for an **internal** model URL reachable from the Data Flow network.
   - **Fails everywhere** → wrong hostname, model stopped, or bad `accessKey`.

2. **HTTP URL typo** — full URL including `?accessKey=…` must be on **HTTP URL** (NiFi 2.x). Old exports used **Remote URL**; upgrade mappings if the processor shows empty URL.

3. **Proxy** — if the NiFi cluster requires an outbound proxy, set **Proxy Configuration** on InvokeHTTP; without it, connect can hang until timeout.

4. **Timeouts too low** — `Connect timed out` is usually connect phase; still bump **Connection Timeout** and **Read Timeout** after connectivity is fixed.

## Security

- Store **`accessKey`** in a **Parameter Context** (sensitive), not in the flow JSON export.
- Rotate the key if it was committed to git or shared in chat.

## Direct CML deploy (no Model Service URL)

If you call the model API inside CML without the `model?accessKey=` gateway, see [README.md](./README.md) — body may be `{"s3_uri":"…"}` only. The **Model Service** wrapper uses `{"request":{…}}` as in your working curl.
