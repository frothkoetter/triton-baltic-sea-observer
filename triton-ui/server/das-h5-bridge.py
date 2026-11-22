#!/usr/bin/env python3
"""Fetch DAS HDF5 from S3 and return downsampled spectrogram + short WAV preview JSON."""
from __future__ import annotations

import base64
import io
import json
import os
import sys
import wave
from urllib.parse import urlparse


def parse_s3_uri(uri: str) -> tuple[str, str]:
    u = uri.strip()
    if u.startswith("s3a://"):
        u = "s3://" + u[6:]
    if not u.startswith("s3://"):
        raise ValueError("URI must be s3:// or s3a://")
    parsed = urlparse(u)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError("Invalid S3 URI (need bucket and key)")
    return bucket, key


def allowed_uri(uri: str) -> bool:
    prefixes: list[str] = []
    for env_key in ("S3_DAS_ALLOWED_PREFIX", "S3_LIDAR_ALLOWED_PREFIX"):
        p = os.environ.get(env_key, "").strip()
        if p:
            prefixes.append(p.replace("s3a://", "s3://"))
    if not prefixes:
        return True
    norm = uri.replace("s3a://", "s3://")
    return any(norm.startswith(pref) for pref in prefixes)


def s3_client():
    root = os.path.dirname(os.path.abspath(__file__))
    if root not in sys.path:
        sys.path.insert(0, root)
    from s3_boto import get_s3_client

    client, _mode = get_s3_client()
    return client


def fetch_h5_bytes(uri: str) -> bytes:
    if not allowed_uri(uri):
        raise ValueError("URI not allowed by S3_DAS_ALLOWED_PREFIX / S3_LIDAR_ALLOWED_PREFIX")
    bucket, key = parse_s3_uri(uri)
    client = s3_client()
    max_mb = int(os.environ.get("DAS_H5_MAX_MB", "48"))
    head = client.head_object(Bucket=bucket, Key=key)
    size = int(head.get("ContentLength") or 0)
    if size > max_mb * 1024 * 1024:
        raise ValueError(f"HDF5 too large ({size} bytes, max {max_mb} MB)")
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def _downsample_2d(arr, max_rows: int, max_cols: int):
    import numpy as np

    a = np.asarray(arr, dtype=np.float32)
    if a.size == 0:
        return a, 0, 0
    nr, nc = a.shape
    row_idx = np.linspace(0, max(0, nr - 1), min(max_rows, nr)).astype(np.int64)
    col_idx = np.linspace(0, max(0, nc - 1), min(max_cols, nc)).astype(np.int64)
    return a[np.ix_(row_idx, col_idx)], len(row_idx), len(col_idx)


def _mono_wav_b64(channels, fs_hz: float, play_max_sec: float) -> tuple[str, float, int]:
    import numpy as np

    fs = float(fs_hz) if fs_hz > 0 else 2000.0
    play_max = float(os.environ.get("DAS_PLAY_MAX_SEC", str(play_max_sec)))
    play_max = max(0.5, min(play_max, 30.0))

    ch = np.asarray(channels, dtype=np.float64)
    if ch.ndim == 1:
        mono = ch
    else:
        mono = ch.mean(axis=0)

    n_take = int(min(len(mono), play_max * fs))
    mono = mono[:n_take]
    duration_s = len(mono) / fs if fs > 0 else 0.0

    target_fs = int(os.environ.get("DAS_AUDIO_TARGET_FS", "8000"))
    if fs > target_fs and len(mono) > 1:
        step = max(1, int(round(fs / target_fs)))
        mono = mono[::step]
        out_fs = int(round(fs / step))
    else:
        out_fs = int(round(fs))

    peak = float(np.max(np.abs(mono))) or 1.0
    mono = np.clip(mono / peak, -1.0, 1.0)
    pcm = (mono * 32767.0).astype(np.int16)

    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(out_fs)
        wf.writeframes(pcm.tobytes())
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return b64, duration_s, out_fs


def segment_preview(payload: bytes, segment_index: int, play_max_sec: float = 10.0) -> dict:
    import numpy as np
    import h5py

    with h5py.File(io.BytesIO(payload), "r") as h5:
        if "segments" not in h5:
            raise ValueError("HDF5 missing segments group")
        seg_name = f"seg_{int(segment_index):03d}"
        if seg_name not in h5["segments"]:
            names = sorted(h5["segments"].keys())
            if not names:
                raise ValueError("No segments in HDF5")
            seg_name = names[min(segment_index, len(names) - 1)]
        sg = h5["segments"][seg_name]
        spec = np.asarray(sg["spectrogram"][:], dtype=np.float32)
        freqs = np.asarray(sg["frequencies_hz"][:], dtype=np.float32)
        channels = sg["channels"][:]
        fs_hz = float(sg.attrs.get("fs_hz", 2000.0))
        threat = str(sg.attrs.get("threat_class", "ambient"))
        chainage_m = float(sg.attrs.get("chainage_m", 0.0))

    small, n_f, n_t = _downsample_2d(spec, 160, 120)
    log_spec = np.log1p(small).tolist()
    f_idx = np.linspace(0, max(0, len(freqs) - 1), n_f).astype(int) if len(freqs) else np.array([0])
    freq_axis = [float(freqs[i]) for i in f_idx] if len(freqs) else [0.0]

    wav_b64, duration_s, audio_fs = _mono_wav_b64(channels, fs_hz, play_max_sec)

    return {
        "segment_id": seg_name,
        "segment_index": int(seg_name.split("_")[-1]),
        "threat_class": threat,
        "chainage_m": chainage_m,
        "fs_hz": fs_hz,
        "spectrogram": {
            "values": log_spec,
            "n_freq": n_f,
            "n_time": n_t,
            "frequencies_hz": freq_axis,
            "scale": "log1p(magnitude)",
        },
        "audio": {
            "wav_base64": wav_b64,
            "duration_sec": round(duration_s, 3),
            "sample_rate_hz": audio_fs,
            "max_play_sec": min(play_max_sec, float(os.environ.get("DAS_PLAY_MAX_SEC", "10"))),
            "caption": "Strain-rate preview (channel mean, simulated DAS)",
        },
    }


def main() -> None:
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "Usage: das-h5-bridge.py <s3a://...> [segment_index]"}))
        sys.exit(1)
    uri = sys.argv[1]
    seg = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].strip().isdigit() else 0
    play_max = float(os.environ.get("DAS_PLAY_MAX_SEC", "10"))
    try:
        raw = fetch_h5_bytes(uri)
        preview = segment_preview(raw, seg, play_max_sec=play_max)
        print(
            json.dumps(
                {
                    "ok": True,
                    "source": "s3",
                    "uri": uri,
                    **preview,
                },
                separators=(",", ":"),
            )
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "source": "s3", "uri": uri}))
        sys.exit(2)


if __name__ == "__main__":
    main()