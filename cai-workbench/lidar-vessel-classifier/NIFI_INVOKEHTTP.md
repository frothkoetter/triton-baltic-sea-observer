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
| **Connection Timeout** | `30–60 sec` minimum while testing (avoid `5 sec` until connectivity is proven) |
| **Socket Read Timeout** | `120 sec` or more (model reads LAS from S3; `15 sec` is often too low) |
| **Authorization** (optional dynamic property) | `Bearer ${lidar.model.api.key}` if required; omit if `accessKey`-only curl works |

**Request body** (FlowFile content) must match the model gateway—**not** the same shape as the URL:

```json
{"request":{"s3_uri":"s3a://goes-se-sandbox/data/user/…/file.las"}}
```

**Wrong (common):** putting `accessKey` inside the JSON body **and** on the query string:

```json
{"accessKey":"…","request":{"s3_uri":"…"}}
```

Use **`accessKey` only on HTTP URL** (`…/model?accessKey=…`). Strip `accessKey` from the body with **JoltTransformJSON**, **ReplaceText**, or build the body in an upstream processor.

Use the **same S3 URI** NiFi wrote the LAS to (`s3a://…` is fine; the model normalizes `s3a` → `s3`).

### Parameter / URL expression (NiFi 2.x)

Use **Parameter Context** references with **`${…}`**, not `#{}`, unless your CDP docs explicitly say otherwise:

| Parameter name (example) | HTTP URL fragment |
|--------------------------|-------------------|
| `Lidar Classifier URL` | `${Lidar Classifier URL}?accessKey=${Lidar Classification Access Key}` |

In the InvokeHTTP UI, the resolved **HTTP URL** preview (or a one-shot test) must show a real host, e.g.  
`https://modelservice.ml-….cloudera.site/model?accessKey=…`  

If the URL stays literally `#{Lidar Classifier URL}?accessKey=…`, the processor will fail (often **Connect timed out** or unknown host).

Suggested timeouts while debugging: **Connection Timeout** `30–60 sec`, **Socket Read Timeout** `120 sec` (not `5 sec` / `15 sec`).

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
