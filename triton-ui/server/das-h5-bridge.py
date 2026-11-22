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


def _orient_spectrogram(spec) -> "np.ndarray":
    """Ensure shape (n_freq, n_time) — tolerate legacy (time, freq) layouts."""
    import numpy as np

    a = np.asarray(spec, dtype=np.float32)
    if a.ndim != 2 or a.size == 0:
        return a
    r, c = a.shape
    # STFT: freq bins <= fs/2 (~500–1000); time frames often > freq bins
    if r > c and c <= 512 and r > 512:
        a = a.T
    # Tame DC leakage so global scaling is not dominated by row 0
    if a.shape[0] > 1:
        a = a.copy()
        n_dc = min(3, a.shape[0])
        a[:n_dc, :] *= 0.03
    return a


def _display_spectrogram_matrix(spec) -> "np.ndarray":
    """Row-wise dynamics so low-Hz tones are visible without a single DC strip."""
    import numpy as np

    a = np.asarray(spec, dtype=np.float32)
    if a.ndim != 2 or a.size == 0:
        return a
    row_peak = np.maximum(a.max(axis=1, keepdims=True), 1e-8)
    col_med = np.median(a, axis=0, keepdims=True) + 1e-8
    vis = a / np.maximum(row_peak, col_med * 2.0)
    return np.clip(vis, 0.0, 1.5).astype(np.float32)


def _mono_from_channels(channels):
    import numpy as np

    ch = np.asarray(channels, dtype=np.float64)
    if ch.ndim == 1:
        return ch
    if ch.ndim != 2:
        return ch.reshape(-1)
    r, c = ch.shape
    if c >= r:
        return ch.mean(axis=0)
    return ch.mean(axis=1)


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

    mono = _mono_from_channels(channels)
    ch = np.asarray(channels)
    if ch.ndim == 2 and len(mono) < int(fs * 0.25):
        mono = ch.mean(axis=1) if ch.shape[0] > ch.shape[1] else ch.mean(axis=0)

    if fs <= 0:
        fs = 2000.0
    n_take = int(min(len(mono), play_max * fs))
    if n_take < 8:
        raise ValueError(
            f"DAS channel waveform too short ({len(mono)} samples @ {fs} Hz)"
        )
    mono = mono[:n_take]
    duration_s = len(mono) / fs if fs > 0 else 0.0

    target_fs = int(os.environ.get("DAS_AUDIO_TARGET_FS", "8000"))
    out_fs = target_fs if target_fs > 0 else 8000
    if len(mono) > 1 and int(round(fs)) != out_fs:
        src_len = len(mono)
        dst_len = max(8, int(round(src_len * out_fs / fs)))
        x_old = np.linspace(0.0, 1.0, src_len)
        x_new = np.linspace(0.0, 1.0, dst_len)
        mono = np.interp(x_new, x_old, mono)
        duration_s = dst_len / out_fs

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
        spec = _orient_spectrogram(sg["spectrogram"][:])
        freqs = np.asarray(sg["frequencies_hz"][:], dtype=np.float32)
        channels = sg["channels"][:]
        fs_hz = float(sg.attrs.get("fs_hz", 2000.0) or 2000.0)
        threat = str(sg.attrs.get("threat_class", "ambient"))
        chainage_m = float(sg.attrs.get("chainage_m", 0.0))

    vis = _display_spectrogram_matrix(spec)
    small, n_f, n_t = _downsample_2d(vis, 160, 120)
    p_lo, p_hi = np.percentile(small, [8, 99.0])
    denom = max(float(p_hi - p_lo), 1e-9)
    norm = np.clip((small - p_lo) / denom, 0.0, 1.0)
    log_spec = (np.log1p(norm * 12.0)).tolist()
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
            "scale": "log1p(percentile-norm)",
            "display_floor": float(p_lo),
            "display_ceiling": float(p_hi),
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