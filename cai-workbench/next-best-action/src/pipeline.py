"""Phase 0 NBA pipeline: alert → snapshot bundle → COA scoring → artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .alert_rules import NbaAlert, next_alert
from .config import Settings
from .persistence import insert_coa_scores, insert_incident, insert_snapshots
from .scoring import EnvironmentContext, rank_coas
from .snapshot_bundle import SnapshotBundle, capture_bundle


@dataclass
class PipelineResult:
    alert: NbaAlert
    bundle: SnapshotBundle
    ranked_coas: list
    best_coa_id: str | None
    artifact_dir: Path


def run_phase0(settings: Settings | None = None) -> PipelineResult:
    settings = settings or Settings()
    settings.artifacts_dir = Path(settings.artifacts_dir)
    alert = next_alert(settings)
    bundle = capture_bundle(alert.flow_uuid, settings)
    env = EnvironmentContext.from_alert_payload(alert.payload)
    ranked = rank_coas(env)
    best = next((s.coa.coa_id for s in ranked if s.gates_passed), None)

    out_dir = settings.artifacts_dir / alert.flow_uuid
    out_dir.mkdir(parents=True, exist_ok=True)
    audit: dict[str, Any] = {
        "phase": 0,
        "alert": alert.to_dict(),
        "snapshot_bundle": bundle.to_dict(),
        "environment": {
            "cloud_cover_pct": env.cloud_cover_pct,
            "night": env.night,
            "seastate": env.seastate,
            "turbidity_high": env.turbidity_high,
            "water_depth_m": env.water_depth_m,
        },
        "coa_scores": [s.to_dict() for s in ranked],
        "best_coa_id": best,
    }
    (out_dir / "audit.json").write_text(
        json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    (out_dir / "coa_matrix.json").write_text(
        json.dumps([s.to_dict() for s in ranked], indent=2), encoding="utf-8"
    )

    if settings.persist_trino and best is not None:
        insert_incident(alert, "SCORED", best, settings)
        insert_snapshots(bundle, settings)
        insert_coa_scores(alert.flow_uuid, ranked, settings)

    return PipelineResult(
        alert=alert,
        bundle=bundle,
        ranked_coas=ranked,
        best_coa_id=best,
        artifact_dir=out_dir,
    )