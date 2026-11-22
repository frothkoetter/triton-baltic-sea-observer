"""
DAS (Distributed Acoustic Sensing) synthesis for submarine cable corridors.

Generates multi-channel strain-rate time series and STFT spectrograms (HDF5)
for continuous underwater monitoring (anchor drag, CUI sabotage indicators).
"""

from __future__ import annotations

import io
import json
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

from cable_corridor_registry import cable_length_m, cable_meta, cable_vertices, interpolate_chainage

# Ground-truth labels for ML (defense.das_cable_events ontology)
THREAT_CLASSES = (
    "ambient",
    "vessel_passage",
    "trawler_scrape",
    "anchor_drag",
    "cui_sabotage_tap",
    "cable_strain_anomaly",
)

DEFAULT_FS_HZ = 2000.0
DEFAULT_WINDOW_S = 4.0
DEFAULT_CHANNELS = 48
DEFAULT_SEGMENTS = 6


@dataclass
class DasSegment:
    segment_index: int
    chainage_m: float
    latitude: float
    longitude: float
    threat_class: str
    channels: np.ndarray  # (n_channels, n_samples)
    spectrogram: np.ndarray  # (n_freq, n_time)
    frequencies_hz: np.ndarray
    fs_hz: float
    cable_asset: str
    cable_name: str

    def summary_features(self) -> Dict[str, float]:
        """Compact features for classifier (mirrors das-cable-classifier/src/features.py)."""
        spec = self.spectrogram
        freqs = self.frequencies_hz
        total = float(spec.sum()) + 1e-12
        band = len(freqs) // 4
        bp = [float(spec[i * band : (i + 1) * band, :].sum() / total) for i in range(4)]
        centroid = float((spec * freqs[:, None]).sum() / total)
        rolloff_idx = int(np.searchsorted(np.cumsum(spec.sum(axis=1)) / (spec.sum() + 1e-12), 0.85))
        rolloff = float(freqs[min(rolloff_idx, len(freqs) - 1)])
        peak_idx = int(np.unravel_index(np.argmax(spec), spec.shape)[0])
        peak_f = float(freqs[peak_idx])
        channel_std = float(np.std(self.channels))
        channel_peak = float(np.max(np.abs(self.channels)))
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
            "chainage_km": self.chainage_m / 1000.0,
        }


def _stft(x: np.ndarray, fs: float, n_fft: int = 256, hop: int = 64) -> Tuple[np.ndarray, np.ndarray]:
    """Single-channel STFT magnitude."""
    if len(x) < n_fft:
        x = np.pad(x, (0, n_fft - len(x)))
    frames = []
    for start in range(0, len(x) - n_fft + 1, hop):
        frame = x[start : start + n_fft] * np.hanning(n_fft)
        frames.append(np.abs(np.fft.rfft(frame)))
    if not frames:
        frames = [np.abs(np.fft.rfft(x[:n_fft] * np.hanning(min(n_fft, len(x)))))]
    spec = np.stack(frames, axis=1)
    freqs = np.fft.rfftfreq(n_fft, 1.0 / fs)
    return spec.astype(np.float32), freqs.astype(np.float32)


# Class-specific centre tones (Hz) — stable fingerprints for ML across random seeds
_THREAT_TONE_HZ: Dict[str, float] = {
    "ambient": 7.0,
    "vessel_passage": 24.0,
    "trawler_scrape": 88.0,
    "anchor_drag": 5.5,
    "cui_sabotage_tap": 340.0,
    "cable_strain_anomaly": 1.2,
}


def _synthesize_waveform(
    threat: str,
    n_samples: int,
    fs: float,
    n_channels: int,
    rng: random.Random,
) -> np.ndarray:
    t = np.arange(n_samples, dtype=np.float64) / fs
    data = np.array(
        [[rng.uniform(-0.03, 0.03) for _ in range(n_samples)] for _ in range(n_channels)],
        dtype=np.float64,
    )
    f0 = _THREAT_TONE_HZ.get(threat, 10.0)
    # Fibre-wide micro-vibration (weak)
    for ch in range(n_channels):
        data[ch] += 0.015 * np.sin(2 * np.pi * 6.0 * t + ch * 0.12)

    if threat == "ambient":
        return data.astype(np.float32)

    if threat == "vessel_passage":
        envelope = np.exp(-((t - n_samples / fs / 2) ** 2) / (2 * (n_samples / fs / 5.5) ** 2))
        for ch in range(n_channels):
            data[ch] += envelope * 0.55 * np.sin(2 * np.pi * f0 * t + ch * 0.05)
            data[ch] += envelope * 0.15 * np.sin(2 * np.pi * (f0 * 2.1) * t)
        return data.astype(np.float32)

    if threat == "trawler_scrape":
        scrape = 0.5 + 0.5 * np.sin(2 * np.pi * 3.5 * t)
        for ch in range(n_channels):
            data[ch] += scrape * 0.65 * np.sin(2 * np.pi * f0 * t + ch * 0.2)
            data[ch] += scrape * 0.2 * np.sin(2 * np.pi * 150 * t)
        return data.astype(np.float32)

    if threat == "anchor_drag":
        drag = np.linspace(0.2, 1.0, n_samples)
        for ch in range(n_channels):
            data[ch] += drag * 1.1 * np.sin(2 * np.pi * f0 * t + ch * 0.07)
            data[ch] += drag * 0.35 * np.sin(2 * np.pi * 18 * t)
        return data.astype(np.float32)

    if threat == "cui_sabotage_tap":
        impulse = np.zeros(n_samples)
        k = n_samples // 2
        impulse[k : k + 12] = 1.5
        for ch in range(n_channels):
            data[ch] += impulse * np.sin(2 * np.pi * f0 * t) * 1.2
            data[ch] += impulse * 0.5 * (ch + 1) / n_channels
        return data.astype(np.float32)

    if threat == "cable_strain_anomaly":
        step = np.ones(n_samples)
        step[: n_samples // 3] = 0.2
        for ch in range(n_channels):
            data[ch] += step * 0.75 * np.sin(2 * np.pi * f0 * t)
            data[ch] += step * 0.4 * (ch % 4)
        return data.astype(np.float32)

    return data.astype(np.float32)


def _pick_threat_mix(rng: random.Random, segments: int) -> List[str]:
    """Realistic mix along a cable patrol window."""
    weights = {
        "ambient": 0.22,
        "vessel_passage": 0.28,
        "trawler_scrape": 0.12,
        "anchor_drag": 0.18,
        "cui_sabotage_tap": 0.08,
        "cable_strain_anomaly": 0.12,
    }
    labels = list(weights.keys())
    probs = [weights[k] for k in labels]
    return [rng.choices(labels, weights=probs, k=1)[0] for _ in range(segments)]


def generate_das_batch(
    cable_asset: str = "BALTICCONNECTOR",
    *,
    n_segments: int = DEFAULT_SEGMENTS,
    n_channels: int = DEFAULT_CHANNELS,
    window_s: float = DEFAULT_WINDOW_S,
    fs_hz: float = DEFAULT_FS_HZ,
    seed: Optional[int] = None,
    threat_labels: Optional[Sequence[str]] = None,
) -> Tuple[List[DasSegment], Dict[str, Any]]:
    rng = random.Random(seed)
    asset = cable_asset.upper()
    meta = cable_meta(asset)
    length_m = cable_length_m(asset)
    verts = cable_vertices(asset)
    labels = list(threat_labels) if threat_labels else _pick_threat_mix(rng, n_segments)
    if len(labels) < n_segments:
        labels.extend(_pick_threat_mix(rng, n_segments - len(labels)))

    n_samples = int(window_s * fs_hz)
    segments: List[DasSegment] = []
    for i in range(n_segments):
        chainage = (i + 0.5) / n_segments * length_m * rng.uniform(0.92, 1.0)
        pos = interpolate_chainage(verts, chainage)
        threat = labels[i % len(labels)]
        channels = _synthesize_waveform(threat, n_samples, fs_hz, n_channels, rng)
        # Collapse to virtual single-mode average + dominant channel STFT for storage
        avg = channels.mean(axis=0)
        spec, freqs = _stft(avg, fs_hz)
        segments.append(
            DasSegment(
                segment_index=i,
                chainage_m=pos["chainage_m"],
                latitude=pos["lat"],
                longitude=pos["lon"],
                threat_class=threat,
                channels=channels,
                spectrogram=spec,
                frequencies_hz=freqs,
                fs_hz=fs_hz,
                cable_asset=meta["id"],
                cable_name=meta["name"],
            )
        )

    batch_meta = {
        "cable_asset": asset,
        "cable_id": meta["id"],
        "cable_name": meta["name"],
        "cable_length_m": length_m,
        "n_segments": n_segments,
        "n_channels": n_channels,
        "window_s": window_s,
        "fs_hz": fs_hz,
        "threat_classes": list(THREAT_CLASSES),
        "segments": [
            {
                "index": s.segment_index,
                "chainage_km": round(s.chainage_m / 1000.0, 3),
                "lat": s.latitude,
                "lon": s.longitude,
                "threat_class": s.threat_class,
            }
            for s in segments
        ],
    }
    return segments, batch_meta


def batch_geolocation_attributes(segments: Sequence[DasSegment]) -> Dict[str, str]:
    """
  NiFi FlowFile geolocation (WGS84) for enrichment / CDW metadata mapping.

  - ``latitude`` / ``longitude``: centroid of all segments in the batch.
  - ``das.segment.lat`` / ``das.segment.lon``: primary event segment (first non-ambient
    threat, else first segment) per ``ontology/mappings/das_cable_spectrogram_metadata.yaml``.
  """
    if not segments:
        return {}
    lats = [s.latitude for s in segments]
    lons = [s.longitude for s in segments]
    centroid_lat = sum(lats) / len(lats)
    centroid_lon = sum(lons) / len(lons)
    primary = next(
        (
            s
            for s in segments
            if s.threat_class not in ("ambient", "vessel_passage")
        ),
        segments[0],
    )
    return {
        "latitude": f"{centroid_lat:.6f}",
        "longitude": f"{centroid_lon:.6f}",
        "das.centroid.lat": f"{centroid_lat:.6f}",
        "das.centroid.lon": f"{centroid_lon:.6f}",
        "das.segment.lat": f"{primary.latitude:.6f}",
        "das.segment.lon": f"{primary.longitude:.6f}",
        "das.segment.index": str(primary.segment_index),
        "das.chainage_m": f"{primary.chainage_m:.2f}",
        "das.threat_class": primary.threat_class,
    }


def write_hdf5_batch(segments: Sequence[DasSegment], batch_meta: Dict[str, Any]) -> bytes:
    import h5py

    bio = io.BytesIO()
    with h5py.File(bio, "w") as h5:
        h5.attrs["schema"] = "triton/das-cable/v1"
        h5.attrs["cable_id"] = batch_meta["cable_id"]
        h5.attrs["cable_name"] = batch_meta["cable_name"]
        h5.attrs["json_meta"] = json.dumps(batch_meta)
        grp = h5.create_group("segments")
        for seg in segments:
            sg = grp.create_group(f"seg_{seg.segment_index:03d}")
            sg.attrs["threat_class"] = seg.threat_class
            sg.attrs["chainage_m"] = seg.chainage_m
            sg.attrs["latitude"] = seg.latitude
            sg.attrs["longitude"] = seg.longitude
            sg.create_dataset(
                "channels",
                data=seg.channels,
                compression="gzip",
                compression_opts=4,
            )
            sg.create_dataset(
                "spectrogram",
                data=seg.spectrogram,
                compression="gzip",
                compression_opts=4,
            )
            sg.create_dataset("frequencies_hz", data=seg.frequencies_hz)
            sg.attrs["fs_hz"] = seg.fs_hz
    return bio.getvalue()


def read_hdf5_features_from_bytes(payload: bytes) -> List[Dict[str, Any]]:
    """Load segments and feature vectors (for ML validate script)."""
    import h5py

    out: List[Dict[str, Any]] = []
    with h5py.File(io.BytesIO(payload), "r") as h5:
        if "segments" not in h5:
            return out
        for name in sorted(h5["segments"].keys()):
            sg = h5["segments"][name]
            spec = sg["spectrogram"][:]
            freqs = sg["frequencies_hz"][:]
            channels = sg["channels"][:]
            threat = sg.attrs.get("threat_class", "ambient")
            seg = DasSegment(
                segment_index=int(name.split("_")[-1]),
                chainage_m=float(sg.attrs.get("chainage_m", 0)),
                latitude=float(sg.attrs.get("latitude", 0)),
                longitude=float(sg.attrs.get("longitude", 0)),
                threat_class=str(threat),
                channels=channels,
                spectrogram=spec,
                frequencies_hz=freqs,
                fs_hz=float(sg.attrs.get("fs_hz", DEFAULT_FS_HZ)),
                cable_asset=str(h5.attrs.get("cable_id", "")),
                cable_name=str(h5.attrs.get("cable_name", "")),
            )
            row = seg.summary_features()
            row["threat_class"] = threat
            row["segment"] = name
            out.append(row)
    return out