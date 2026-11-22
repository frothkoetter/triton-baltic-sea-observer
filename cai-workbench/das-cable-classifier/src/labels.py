"""DAS threat classes (aligned with das_acoustic_engine.THREAT_CLASSES)."""

THREAT_CLASSES = [
    "ambient",
    "vessel_passage",
    "trawler_scrape",
    "anchor_drag",
    "cui_sabotage_tap",
    "cable_strain_anomaly",
]

FEATURE_NAMES = [
    "band_power_0",
    "band_power_1",
    "band_power_2",
    "band_power_3",
    "spectral_centroid_hz",
    "spectral_rolloff_hz",
    "peak_frequency_hz",
    "channel_std",
    "channel_peak",
    "spectral_entropy",
    "high_low_band_ratio",
]