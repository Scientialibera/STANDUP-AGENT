from __future__ import annotations

import logging
from typing import Any

from src.graph.auth import graph_post

logger = logging.getLogger(__name__)


async def post_adaptive_card_to_channel(team_id: str, channel_id: str, card: dict) -> Any:
    """Post an Adaptive Card as a message to a Teams channel."""
    path = f"/teams/{team_id}/channels/{channel_id}/messages"
    body = {
        "body": {
            "contentType": "html",
            "content": '<attachment id="card"></attachment>',
        },
        "attachments": [
            {
                "id": "card",
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": str(card),
            }
        ],
    }
    return await graph_post(path, body)
