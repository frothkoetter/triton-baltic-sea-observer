import json
import random
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

from cable_corridor_registry import (
    cable_asset_allowable_values,
    cable_meta,
    resolve_cable_asset,
)
from das_acoustic_engine import (
    DAS_MONITOR_SIGNALS,
    DEFAULT_CHANNELS,
    DEFAULT_FS_HZ,
    DEFAULT_SEGMENTS,
    DEFAULT_WINDOW_S,
    MAX_WINDOW_S,
    THREAT_CLASSES,
    batch_geolocation_attributes,
    generate_das_batch,
    monitor_signal_attributes,
    write_hdf5_batch,
)


class DasCableAcousticSimulator(FlowFileTransform):
    """
    Seekabel-Akustik (DAS): Mehrkanal-Zeitreihen + Spektrogramme (HDF5)
    entlang eines EMODnet-Unterseekabels (Default: Balticconnector).
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.3.1'
        description = (
            'Synthetic underwater DAS strain-rate + STFT spectrograms (HDF5) along '
            'Baltic cables/pipelines — vessel, anchor slip, anchor drop/tow alarm, '
            'U-boat tonal, whale calls, hard beep, CUI tap (window up to 30 s). '
            'Optional DAS-Seekabel Akustik-Monitor presets (km 42 Ankerwurf/Schlepp).'
        )
        dependencies = ['numpy', 'h5py']

    CABLE_ASSET = PropertyDescriptor(
        name="Cable Asset",
        description=(
            "Submarine cable or pipeline corridor (WKT from defense.ostsee_infrastruktur). "
            "Gaspipelines: BALTICCONNECTOR, NORD_STREAM_1/2. "
            "Seekabel BSH/CONTIS: PCABLE_BSH_1 … PCABLE_BSH_4. "
            "RANDOM: pick one of the seven corridors per FlowFile."
        ),
        allowable_values=cable_asset_allowable_values(),
        default_value="BALTICCONNECTOR",
        required=True,
    )

    SEGMENT_COUNT = PropertyDescriptor(
        name="Segment Count",
        description="Spectrogram windows along the cable (chainage samples).",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_SEGMENTS),
        required=True,
    )

    CHANNEL_COUNT = PropertyDescriptor(
        name="Channel Count",
        description="DAS spatial channels (fibre samples).",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_CHANNELS),
        required=True,
    )

    WINDOW_SECONDS = PropertyDescriptor(
        name="Window Seconds",
        description=f"Time window per segment (1–{int(MAX_WINDOW_S)} s underwater record).",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(int(DEFAULT_WINDOW_S)),
        required=True,
    )

    SAMPLE_RATE_HZ = PropertyDescriptor(
        name="Sample Rate Hz",
        description="DAS sampling rate.",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(int(DEFAULT_FS_HZ)),
        required=True,
    )

    RANDOM_SEED = PropertyDescriptor(
        name="Random Seed",
        description="Optional seed for reproducible spectra (empty = random).",
        default_value="",
        required=False,
    )

    OUTPUT_FORMAT = PropertyDescriptor(
        name="Output Format",
        description="hdf5 binary or json preview (features only, no raw arrays).",
        allowable_values=["hdf5", "json_preview"],
        default_value="hdf5",
        required=True,
    )

    MONITOR_SIGNAL = PropertyDescriptor(
        name="Monitor Signal",
        description=(
            "Optional DAS-Seekabel Akustik-Monitor scenario. "
            "SEEKABEL_ANKERWURF_KM42: sharp +42 dB impulse, broadband to 1 kHz "
            "at chainage km 42 — ALARM Ankerwurf/Schleppvorgang."
        ),
        allowable_values=["OFF"] + list(DAS_MONITOR_SIGNALS.keys()),
        default_value="OFF",
        required=True,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [
            self.CABLE_ASSET,
            self.SEGMENT_COUNT,
            self.CHANNEL_COUNT,
            self.WINDOW_SECONDS,
            self.SAMPLE_RATE_HZ,
            self.RANDOM_SEED,
            self.OUTPUT_FORMAT,
            self.MONITOR_SIGNAL,
        ]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowFile):
        try:
            asset_sel = context.getProperty(self.CABLE_ASSET.name).getValue() or "BALTICCONNECTOR"
            n_seg = int(context.getProperty(self.SEGMENT_COUNT.name).getValue() or DEFAULT_SEGMENTS)
            n_ch = int(context.getProperty(self.CHANNEL_COUNT.name).getValue() or DEFAULT_CHANNELS)
            win_s = float(context.getProperty(self.WINDOW_SECONDS.name).getValue() or DEFAULT_WINDOW_S)
            win_s = max(1.0, min(win_s, MAX_WINDOW_S))
            fs = float(context.getProperty(self.SAMPLE_RATE_HZ.name).getValue() or DEFAULT_FS_HZ)
            seed_raw = (context.getProperty(self.RANDOM_SEED.name).getValue() or "").strip()
            seed = int(seed_raw) if seed_raw.isdigit() else None
            fmt = context.getProperty(self.OUTPUT_FORMAT.name).getValue() or "hdf5"
            monitor_raw = (context.getProperty(self.MONITOR_SIGNAL.name).getValue() or "OFF").strip()
            monitor_signal = None if monitor_raw in ("", "OFF") else monitor_raw

            rng = random.Random(seed) if seed is not None else None
            asset = resolve_cable_asset(asset_sel, rng=rng)

            segments, batch_meta = generate_das_batch(
                asset,
                n_segments=min(24, max(1, n_seg)),
                n_channels=min(128, max(8, n_ch)),
                window_s=win_s,
                fs_hz=fs,
                seed=seed,
                monitor_signal=monitor_signal,
            )
            meta = cable_meta(asset)

            if fmt == "json_preview":
                preview = {
                    **batch_meta,
                    "features": [s.summary_features() | {"threat_class_gt": s.threat_class} for s in segments],
                }
                body = json.dumps(preview, indent=2).encode("utf-8")
                mime = "application/json"
            else:
                body = write_hdf5_batch(segments, batch_meta)
                mime = "application/x-hdf5"

            threats = ",".join(sorted({s.threat_class for s in segments}))
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            batch_id = uuid.uuid4().hex[:12]
            base = f"das_{asset.lower()}_{len(segments)}seg_{stamp}_{batch_id}"
            file_name = f"{base}.h5" if fmt == "hdf5" else f"{base}_preview.json"
            attrs = {
                "mime.type": mime,
                "das.schema": "triton/das-cable/v1",
                "das.batch.id": batch_id,
                "das.cable.asset.selection": asset_sel,
                "das.cable.asset": asset,
                "das.cable.id": meta["id"],
                "das.cable.name": meta["name"],
                "das.segment.count": str(len(segments)),
                "das.channel.count": str(n_ch),
                "das.window.seconds": str(win_s),
                "das.fs_hz": str(int(fs)),
                "das.threat.classes": threats,
                "das.threat.catalog": ",".join(THREAT_CLASSES),
                "filename": file_name,
            }
            attrs.update(batch_geolocation_attributes(segments))
            attrs.update(monitor_signal_attributes(batch_meta))
            return FlowFileTransformResult(contents=body, attributes=attrs, relationship="success")
        except Exception as e:
            self.logger.error(f"DAS simulation failed: {e}")
            return FlowFileTransformResult(
                contents=str(e).encode("utf-8"),
                attributes={"error": str(e)},
                relationship="failure",
            )