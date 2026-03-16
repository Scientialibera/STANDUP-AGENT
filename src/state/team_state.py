"""In-memory tracking of daily standup state per team + user-to-team resolution."""

from __future__ import annotations

import logging
from typing import Any

from src.services.team_config import load_team_config

logger = logging.getLogger(__name__)

_conversation_refs: dict[str, dict] = {}


def store_conversation_reference(user_key: str, ref: dict) -> None:
    _conversation_refs[user_key] = ref


def get_conversation_reference(user_key: str) -> dict | None:
    return _conversation_refs.get(user_key)


def get_all_conversation_references() -> dict[str, dict]:
    return dict(_conversation_refs)


async def get_team_for_user(user_identifier: str) -> dict[str, Any] | None:
    """Look up which team a user belongs to (by UPN or AAD object ID)."""
    teams = await load_team_config()
    for team in teams:
        for member in team.members:
            if member.upn.lower() == user_identifier.lower():
                return {
                    "name": team.name,
                    "team_id": team.team_id,
                    "summary_channel_id": team.summary_channel_id,
                    "members": [{"upn": m.upn, "display_name": m.display_name} for m in team.members],
                }
    return None
