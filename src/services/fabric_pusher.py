"""Optional: push standup response data to Fabric lakehouse via ABFSS or REST."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def push_responses_to_landing(team_name: str, date_str: str, responses: list[dict]) -> None:
    """
    Placeholder for direct Fabric push.
    In the current architecture Fabric notebooks pull from blob storage,
    so this is only needed if real-time ingestion is desired.
    """
    logger.debug(
        "Fabric push stub: %d responses for %s on %s. "
        "Fabric notebooks ingest from blob storage on schedule.",
        len(responses),
        team_name,
        date_str,
    )
