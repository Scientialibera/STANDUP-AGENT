from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from src.config import get_settings

logger = logging.getLogger(__name__)

BLOB_NAME = "teams_config.json"


@dataclass
class TeamMember:
    upn: str
    display_name: str


@dataclass
class TeamDefinition:
    name: str
    team_id: str
    summary_channel_id: str
    prompt_time: str
    summary_time: str
    weekly_rollup_day: str
    weekly_rollup_time: str
    timezone: str
    members: list[TeamMember] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TeamDefinition:
        members = [TeamMember(upn=m["upn"], display_name=m["display_name"]) for m in d.get("members", [])]
        return cls(
            name=d["name"],
            team_id=d.get("team_id", ""),
            summary_channel_id=d.get("summary_channel_id", ""),
            prompt_time=d.get("prompt_time", "09:00"),
            summary_time=d.get("summary_time", "10:00"),
            weekly_rollup_day=d.get("weekly_rollup_day", "Friday"),
            weekly_rollup_time=d.get("weekly_rollup_time", "16:00"),
            timezone=d.get("timezone", "UTC"),
            members=members,
        )


_cached: list[TeamDefinition] | None = None


def _get_blob_client():
    s = get_settings()
    credential = DefaultAzureCredential()
    service = BlobServiceClient(account_url=s.blob_account_url, credential=credential)
    return service.get_blob_client(container=s.blob_config_container, blob=BLOB_NAME)


async def load_team_config(force_refresh: bool = False) -> list[TeamDefinition]:
    global _cached
    if _cached is not None and not force_refresh:
        return _cached

    blob_client = _get_blob_client()
    download = blob_client.download_blob()
    raw = download.readall().decode("utf-8")
    data = json.loads(raw)
    _cached = [TeamDefinition.from_dict(t) for t in data]
    logger.info("Loaded %d team definitions from blob.", len(_cached))
    return _cached


def invalidate_cache() -> None:
    global _cached
    _cached = None
