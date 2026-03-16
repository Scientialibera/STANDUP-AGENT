from __future__ import annotations

import logging
from typing import Any

from src.graph.auth import graph_get

logger = logging.getLogger(__name__)


async def resolve_user(upn: str) -> dict[str, Any]:
    """Resolve a UPN to a Graph user profile (id, displayName, mail)."""
    try:
        data = await graph_get(f"/users/{upn}", params={"$select": "id,displayName,mail,userPrincipalName"})
        return data
    except Exception:
        logger.warning("Failed to resolve user %s", upn, exc_info=True)
        return {"userPrincipalName": upn, "id": "", "displayName": upn}


async def install_bot_for_user(user_id: str, app_id: str) -> None:
    """Proactively install the bot for a user via Graph API."""
    from src.graph.auth import graph_post

    body = {"teamsApp@odata.bind": f"https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/{app_id}"}
    try:
        await graph_post(f"/users/{user_id}/teamwork/installedApps", body)
        logger.info("Installed bot for user %s", user_id)
    except Exception:
        logger.debug("Bot install for user %s may already exist or failed", user_id, exc_info=True)
