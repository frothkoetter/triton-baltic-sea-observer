# NiFi Python processor — POST 15-minute AIS batch to Cloudera AI model endpoint.
# Output flowfile content is ONLY a JSON array (no LLM header/footer).
import json
import ssl
import sys
from pathlib import Path
from typing import Any, Dict, List
from urllib import error, request

_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators


def _parse_ais_payload(raw: bytes) -> List[Dict[str, Any]]:
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return []
    if text.startswith("["):
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    records = []
    for line in text.splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def _extract_outlier_array(response_body: bytes) -> str:
    parsed = json.loads(response_body.decode("utf-8"))
    if isinstance(parsed, list):
        return json.dumps(parsed, ensure_ascii=False)
    if isinstance(parsed, dict):
        if "body" in parsed:
            body = parsed["body"]
            if isinstance(body, str):
                return body
            return json.dumps(body, ensure_ascii=False)
        if "prediction" in parsed:
            pred = parsed["prediction"]
            if isinstance(pred, list):
                return json.dumps(pred, ensure_ascii=False)
            if isinstance(pred, dict) and "body" in pred:
                return str(pred["body"])
        if "records" in parsed and isinstance(parsed["records"], list):
            return json.dumps(parsed["records"], ensure_ascii=False)
    raise ValueError("Model response is not a JSON array of outliers")


class AisOutlierCmlInvoke(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "POST AIS NDJSON/JSON batch to Cloudera AI ais-outlier-detector model; "
            "writes only a JSON array of outliers to the flowfile body."
        )
        dependencies = []

    MODEL_URL = PropertyDescriptor(
        name="CML Model URL",
        description="HTTPS endpoint for detect_outliers / predict_array model",
        required=True,
        validators=[StandardValidators.URL_VALIDATOR],
    )
    API_KEY = PropertyDescriptor(
        name="CDP API Key",
        description="Bearer token for the model endpoint (leave empty if not required)",
        required=False,
        sensitive=True,
    )
    TIMEOUT = PropertyDescriptor(
        name="HTTP Timeout",
        description="Request timeout in seconds",
        default_value="120",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.MODEL_URL, self.API_KEY, self.TIMEOUT]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        url = context.getProperty(self.MODEL_URL.name).getValue()
        api_key = context.getProperty(self.API_KEY.name).getValue()
        timeout = int(context.getProperty(self.TIMEOUT.name).getValue() or "120")

        try:
            records = _parse_ais_payload(flowfile.getContentsAsBytes())
            payload = json.dumps({"records": records}).encode("utf-8")
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"

            req = request.Request(url, data=payload, headers=headers, method="POST")
            ctx = ssl.create_default_context()
            with request.urlopen(req, timeout=timeout, context=ctx) as resp:
                body = resp.read()
            out_text = _extract_outlier_array(body)
            return FlowFileTransformResult(
                contents=out_text.encode("utf-8"),
                attributes={
                    "mime.type": "application/json",
                    "ais.outlier.count": str(len(json.loads(out_text))),
                },
                relationship="success",
            )
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:2000]
            return FlowFileTransformResult(
                contents=detail.encode("utf-8"),
                attributes={"error": str(exc)},
                relationship="failure",
            )
        except Exception as exc:
            self.logger.error(f"AIS outlier model invoke failed: {exc}")
            return FlowFileTransformResult(
                contents=str(exc).encode("utf-8"),
                attributes={"error": str(exc)},
                relationship="failure",
            )
