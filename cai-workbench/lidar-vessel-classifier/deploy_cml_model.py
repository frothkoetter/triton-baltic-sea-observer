#!/usr/bin/env python3
"""
Register and deploy the LiDAR vessel classifier on Cloudera AI (CML).

Prerequisites:
  - Run train.py first (artifacts/ + model-deployment/ populated).
  - Run inside a CML project session/job (CDSW_PROJECT_ID set).
  - API access: cmlapi.default_client() or CDSW_API_KEY.

Environment:
  CML_MODEL_NAME          default: lidar-vessel-classifier
  CML_DEPLOYMENT_NAME     default: lidar-vessel-classifier-deployment
  CML_DEPLOYMENT_CPU      default: 1
  CML_DEPLOYMENT_MEMORY_GB default: 2
  CML_DEPLOYMENT_REPLICAS default: 1
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import cmlapi
from cmlapi.rest import ApiException

from project_root import resolve_project_root

ROOT = resolve_project_root()
DEPLOY_DIR = ROOT / "model-deployment"


def _client() -> cmlapi.CMLServiceApi:
    return cmlapi.default_client()


def register_model(client: cmlapi.CMLServiceApi, project_id: str) -> cmlapi.Model:
    name = os.environ.get("CML_MODEL_NAME", "lidar-vessel-classifier")
    description = (
        "RandomForest on point-cloud shape features; classifies Baltic vessel LiDAR (.las)."
    )

    try:
        models = client.list_models(project_id=project_id, search_filter=name)
        for m in models.models:
            if m.name == name:
                print(f"Reusing existing model id={m.id} name={m.name}")
                return m
    except ApiException as exc:
        print(f"list_models: {exc}")

    created = client.create_model(
        project_id=project_id,
        create_model_request=cmlapi.CreateModelRequest(
            name=name,
            description=description,
            visibility="private",
        ),
    )
    print(f"Created model id={created.id}")
    return created


def create_model_build(
    client: cmlapi.CMLServiceApi,
    project_id: str,
    model_id: str,
) -> cmlapi.ModelBuild:
    """Build from model-deployment/ in this repo (path relative to project root)."""
    rel_path = str(DEPLOY_DIR.relative_to(ROOT))
    build = client.create_model_build(
        project_id=project_id,
        model_id=model_id,
        create_model_build_request=cmlapi.CreateModelBuildRequest(
            comment="LAS vessel classifier",
            parent_model_build_id=None,
            files=[rel_path],
            kernel="python3",
        ),
    )
    print(f"Started model build id={build.id} status={build.status}")
    return build


def create_deployment(
    client: cmlapi.CMLServiceApi,
    project_id: str,
    model_id: str,
    model_build_id: str,
) -> cmlapi.ModelDeployment:
    dep_name = os.environ.get("CML_DEPLOYMENT_NAME", "lidar-vessel-classifier-deployment")
    cpu = float(os.environ.get("CML_DEPLOYMENT_CPU", "1"))
    memory = float(os.environ.get("CML_DEPLOYMENT_MEMORY_GB", "2"))
    replicas = int(os.environ.get("CML_DEPLOYMENT_REPLICAS", "1"))

    deployment = client.create_model_deployment(
        project_id=project_id,
        model_id=model_id,
        create_model_deployment_request=cmlapi.CreateModelDeploymentRequest(
            name=dep_name,
            description="REST inference for LiDAR vessel type",
            model_build_id=model_build_id,
            cpu=cpu,
            memory=memory,
            replicas=replicas,
            auth_enabled=True,
        ),
    )
    print(f"Deployment id={deployment.id} status={deployment.status}")
    if deployment.url:
        print(f"Endpoint URL: {deployment.url}")
    return deployment


def main() -> int:
    project_id = os.environ.get("CDSW_PROJECT_ID")
    if not project_id:
        print("CDSW_PROJECT_ID is not set. Run this script from a CML Workbench job/session.")
        return 1

    if not (DEPLOY_DIR / "model.joblib").is_file():
        print(f"Missing model.joblib under {DEPLOY_DIR} — run train.py first.")
        return 1

    client = _client()
    model = register_model(client, project_id)
    build = create_model_build(client, project_id, model.id)

    wait = os.environ.get("CML_WAIT_FOR_BUILD", "true").lower() in ("1", "true", "yes")
    if wait:
        build = client.wait_for_model_build(
            project_id=project_id,
            model_id=model.id,
            build_id=build.id,
            timeout=3600,
        )
        print(f"Build finished: {build.status}")

    if build.status != "success":
        print("Model build did not succeed; fix build logs before deploying.")
        return 2

    create_deployment(client, project_id, model.id, build.id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
