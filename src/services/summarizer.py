from __future__ import annotations

import json
import logging
from typing import Any

from azure.identity import DefaultAzureCredential
from openai import AzureOpenAI

from src.config import get_settings

logger = logging.getLogger(__name__)

_client: AzureOpenAI | None = None


def _get_client() -> AzureOpenAI:
    global _client
    if _client is None:
        s = get_settings()
        _client = AzureOpenAI(
            azure_endpoint=s.aoai_endpoint,
            azure_ad_token_provider=_token_provider,
            api_version=s.aoai_api_version,
        )
    return _client


def _token_provider() -> str:
    credential = DefaultAzureCredential()
    token = credential.get_token("https://cognitiveservices.azure.com/.default")
    return token.token


DAILY_SUMMARY_SYSTEM = (
    "You are a team standup summarizer. Given individual standup updates, produce a concise team summary. "
    "Structure your response as JSON with keys: themes (array of key work themes), "
    "cross_dependencies (array of cross-team or cross-member dependencies), "
    "blockers (array of blockers with member attribution), highlights (array of notable achievements), "
    "and narrative (2-3 sentence overall summary)."
)

WEEKLY_ROLLUP_SYSTEM = (
    "You are a team retrospective analyst. Given a week's worth of daily standup summaries, produce a weekly rollup. "
    "Structure your response as JSON with keys: recurring_blockers (array with frequency counts), "
    "completed_themes (array of work themes that were resolved), velocity_patterns (text description), "
    "team_health_signals (text assessment), recommendations (array of actionable suggestions), "
    "and narrative (3-4 sentence executive summary)."
)


async def generate_daily_summary(team_name: str, responses: list[dict[str, Any]]) -> dict[str, Any]:
    s = get_settings()
    client = _get_client()

    standup_text = "\n\n".join(
        f"**{r.get('user', 'Unknown')}**\n"
        f"- Yesterday: {r.get('yesterday', 'N/A')}\n"
        f"- Today: {r.get('today', 'N/A')}\n"
        f"- Blockers: {r.get('blockers', 'None')}"
        for r in responses
        if not r.get("skipped", False)
    )

    if not standup_text.strip():
        return {"narrative": "No standup responses to summarize.", "themes": [], "blockers": [], "cross_dependencies": [], "highlights": []}

    completion = client.chat.completions.create(
        model=s.aoai_chat_deployment,
        messages=[
            {"role": "system", "content": DAILY_SUMMARY_SYSTEM},
            {"role": "user", "content": f"Team: {team_name}\n\nStandup Updates:\n{standup_text}"},
        ],
        response_format={"type": "json_object"},
        temperature=0.3,
    )

    raw = completion.choices[0].message.content
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Failed to parse summary JSON, returning raw text.")
        return {"narrative": raw, "themes": [], "blockers": [], "cross_dependencies": [], "highlights": []}


async def generate_weekly_rollup(team_name: str, daily_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    s = get_settings()
    client = _get_client()

    summaries_text = "\n\n---\n\n".join(
        f"**{ds.get('date', 'Unknown date')}**\n{json.dumps(ds, indent=2)}" for ds in daily_summaries
    )

    if not summaries_text.strip():
        return {"narrative": "No daily summaries available for weekly rollup.", "recurring_blockers": [], "completed_themes": [], "recommendations": []}

    completion = client.chat.completions.create(
        model=s.aoai_chat_deployment,
        messages=[
            {"role": "system", "content": WEEKLY_ROLLUP_SYSTEM},
            {"role": "user", "content": f"Team: {team_name}\n\nDaily Summaries:\n{summaries_text}"},
        ],
        response_format={"type": "json_object"},
        temperature=0.3,
    )

    raw = completion.choices[0].message.content
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Failed to parse weekly rollup JSON, returning raw text.")
        return {"narrative": raw, "recurring_blockers": [], "completed_themes": [], "recommendations": []}
