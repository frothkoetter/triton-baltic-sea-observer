from __future__ import annotations

import io
from typing import Any, Dict, List

import h5py
import numpy as np

from .labels import FEATURE_NAMES

DEFAULT_FS_HZ = 2000.0


def features_from_summary(summary: Dict[str, float]) -> np.ndarray:
    return np.array([float(summary.get(k, 0.0)) for k in FEATURE_NAMES], dtype=np.float64)


def _summary_features(
    spec: np.ndarray,
    freqs: np.ndarray,
    channels: np.ndarray,
    chainage_m: float,
) -> Dict[str, float]:
    """Same math as nifi upload/das_acoustic_engine.DasSegment.summary_features."""
    total = float(spec.sum()) + 1e-12
    band = max(len(freqs) // 4, 1)
    bp = [float(spec[i * band : (i + 1) * band, :].sum() / total) for i in range(4)]
    centroid = float((spec * freqs[:, None]).sum() / total)
    rolloff_idx = int(np.searchsorted(np.cumsum(spec.sum(axis=1)) / (spec.sum() + 1e-12), 0.85))
    rolloff = float(freqs[min(rolloff_idx, len(freqs) - 1)])
    peak_idx = int(np.unravel_index(np.argmax(spec), spec.shape)[0])
    peak_f = float(freqs[peak_idx])
    channel_std = float(np.std(channels))
    channel_peak = float(np.max(np.abs(channels)))
    spec_flat = spec / (spec.sum() + 1e-12)
    entropy = float(-np.sum(spec_flat * np.log(spec_flat + 1e-12)))
    high_low_ratio = float(bp[3] / (bp[0] + 1e-6))
    return {
        "band_power_0": bp[0],
        "band_power_1": bp[1],
        "band_power_2": bp[2],
        "band_power_3": bp[3],
        "spectral_centroid_hz": centroid,
        "spectral_rolloff_hz": rolloff,
        "peak_frequency_hz": peak_f,
        "channel_std": channel_std,
        "channel_peak": channel_peak,
        "spectral_entropy": entropy,
        "high_low_band_ratio": high_low_ratio,
        "chainage_km": chainage_m / 1000.0,
    }


def features_from_hdf5_bytes(payload: bytes) -> List[Dict[str, Any]]:
    """Feature rows from NiFi DAS HDF5 (triton/das-cable/v1) — no nifi-processors dependency."""
    out: List[Dict[str, Any]] = []
    with h5py.File(io.BytesIO(payload), "r") as h5:
        if "segments" not in h5:
            return out
        for name in sorted(h5["segments"].keys()):
            sg = h5["segments"][name]
            spec = sg["spectrogram"][:]
            freqs = sg["frequencies_hz"][:]
            channels = sg["channels"][:]
            threat = str(sg.attrs.get("threat_class", "ambient"))
            chainage_m = float(sg.attrs.get("chainage_m", 0))
            row = _summary_features(spec, freqs, channels, chainage_m)
            row["threat_class"] = threat
            row["segment"] = name
            out.append(row)
    return out