from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from src.config import get_settings

logger = logging.getLogger(__name__)


def _get_container_client() -> ContainerClient:
    s = get_settings()
    credential = DefaultAzureCredential()
    service = BlobServiceClient(account_url=s.blob_account_url, credential=credential)
    return service.get_container_client(s.blob_responses_container)


def _response_blob_path(team_name: str, date_str: str, user_upn: str) -> str:
    safe_team = team_name.lower().replace(" ", "-")
    return f"{safe_team}/{date_str}/{user_upn}.json"


def _summary_blob_path(team_name: str, date_str: str) -> str:
    safe_team = team_name.lower().replace(" ", "-")
    return f"{safe_team}/{date_str}/_summary.json"


def _status_blob_path(team_name: str, date_str: str) -> str:
    safe_team = team_name.lower().replace(" ", "-")
    return f"{safe_team}/{date_str}/_status.json"


def _weekly_blob_path(team_name: str, week_label: str) -> str:
    safe_team = team_name.lower().replace(" ", "-")
    return f"{safe_team}/weekly/{week_label}.json"


async def store_response(
    team_name: str,
    date_str: str,
    user_upn: str,
    yesterday: str,
    today: str,
    blockers: str,
    skipped: bool = False,
) -> None:
    container = _get_container_client()
    blob_path = _response_blob_path(team_name, date_str, user_upn)
    payload = {
        "team": team_name,
        "date": date_str,
        "user": user_upn,
        "yesterday": yesterday,
        "today": today,
        "blockers": blockers,
        "skipped": skipped,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    container.upload_blob(blob_path, json.dumps(payload, indent=2), overwrite=True)
    logger.info("Stored response for %s / %s / %s", team_name, date_str, user_upn)


async def get_responses(team_name: str, date_str: str) -> list[dict[str, Any]]:
    container = _get_container_client()
    safe_team = team_name.lower().replace(" ", "-")
    prefix = f"{safe_team}/{date_str}/"
    results = []
    for blob in container.list_blobs(name_starts_with=prefix):
        if blob.name.endswith(".json") and not blob.name.split("/")[-1].startswith("_"):
            data = container.download_blob(blob.name).readall().decode("utf-8")
            results.append(json.loads(data))
    return results


async def store_summary(team_name: str, date_str: str, summary: dict[str, Any]) -> None:
    container = _get_container_client()
    blob_path = _summary_blob_path(team_name, date_str)
    container.upload_blob(blob_path, json.dumps(summary, indent=2), overwrite=True)


async def store_status(team_name: str, date_str: str, status: dict[str, Any]) -> None:
    container = _get_container_client()
    blob_path = _status_blob_path(team_name, date_str)
    container.upload_blob(blob_path, json.dumps(status, indent=2), overwrite=True)


async def store_weekly_rollup(team_name: str, week_label: str, rollup: dict[str, Any]) -> None:
    container = _get_container_client()
    blob_path = _weekly_blob_path(team_name, week_label)
    container.upload_blob(blob_path, json.dumps(rollup, indent=2), overwrite=True)


async def get_user_history(team_name: str, user_upn: str, limit: int = 5) -> list[dict[str, Any]]:
    """Retrieve a user's recent standup responses across dates (newest first)."""
    container = _get_container_client()
    safe_team = team_name.lower().replace(" ", "-")
    prefix = f"{safe_team}/"

    matching = []
    for blob in container.list_blobs(name_starts_with=prefix):
        if blob.name.endswith(f"/{user_upn}.json"):
            matching.append(blob.name)

    matching.sort(reverse=True)
    results = []
    for blob_name in matching[:limit]:
        data = container.download_blob(blob_name).readall().decode("utf-8")
        results.append(json.loads(data))
    return results
