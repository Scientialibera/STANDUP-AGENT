from __future__ import annotations

import json
import logging
from datetime import datetime

from botbuilder.core import ActivityHandler, CardFactory, MessageFactory, TurnContext
from botbuilder.schema import Activity, ActivityTypes

from src.cards.prompt_card import build_prompt_card
from src.services.standup_collector import (
    get_responses,
    get_user_history,
    store_response,
)
from src.state.team_state import get_team_for_user

logger = logging.getLogger(__name__)


class StandupBot(ActivityHandler):
    """Handles card submissions and text commands for the standup bot."""

    async def on_message_activity(self, turn_context: TurnContext) -> None:
        if turn_context.activity.value:
            await self._handle_card_action(turn_context)
            return

        text = (turn_context.activity.text or "").strip().lower()
        upn = self._get_upn(turn_context)
        team = await get_team_for_user(upn)

        if text == "status":
            await self._handle_status(turn_context, team, upn)
        elif text == "standup":
            await self._handle_manual_standup(turn_context, team)
        elif text == "skip":
            await self._handle_skip(turn_context, team, upn)
        elif text == "history":
            await self._handle_history(turn_context, team, upn)
        else:
            await turn_context.send_activity(
                MessageFactory.text(
                    "Available commands: **status**, **standup**, **skip**, **history**"
                )
            )

    async def on_members_added_activity(self, members_added, turn_context: TurnContext) -> None:
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity(
                    MessageFactory.text(
                        "Welcome to Standup Agent! I'll send you a daily standup prompt. "
                        "Commands: **status**, **standup**, **skip**, **history**"
                    )
                )

    # -- Card Actions --

    async def _handle_card_action(self, turn_context: TurnContext) -> None:
        data = turn_context.activity.value or {}
        action = data.get("action", "")
        upn = self._get_upn(turn_context)
        team_name = data.get("team_name", "")
        date_str = data.get("date", datetime.utcnow().strftime("%Y-%m-%d"))

        if action == "submit_standup":
            await store_response(
                team_name=team_name,
                date_str=date_str,
                user_upn=upn,
                yesterday=data.get("yesterday", ""),
                today=data.get("today", ""),
                blockers=data.get("blockers", ""),
            )
            await turn_context.send_activity(
                MessageFactory.text(f"Standup submitted for **{team_name}** ({date_str}).")
            )
        elif action == "skip_standup":
            await store_response(
                team_name=team_name,
                date_str=date_str,
                user_upn=upn,
                yesterday="",
                today="",
                blockers="",
                skipped=True,
            )
            await turn_context.send_activity(
                MessageFactory.text(f"Marked as skipped (OOO) for **{team_name}** ({date_str}).")
            )
        else:
            logger.warning("Unknown card action: %s", action)

    # -- Text Commands --

    async def _handle_status(self, turn_context: TurnContext, team: dict | None, upn: str) -> None:
        if not team:
            await turn_context.send_activity(MessageFactory.text("You're not in any configured team."))
            return

        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        responses = await get_responses(team["name"], date_str)
        respondents = {r["user"] for r in responses}
        members = {m["upn"] for m in team.get("members", [])}

        responded = [u for u in members if u in respondents and not any(r.get("skipped") for r in responses if r["user"] == u)]
        skipped = [u for u in members if any(r.get("skipped") for r in responses if r.get("user") == u)]
        missing = [u for u in members if u not in respondents]

        lines = [f"**{team['name']}** standup status ({date_str}):"]
        if responded:
            lines.append(f"Responded: {', '.join(responded)}")
        if skipped:
            lines.append(f"Skipped: {', '.join(skipped)}")
        if missing:
            lines.append(f"Pending: {', '.join(missing)}")

        await turn_context.send_activity(MessageFactory.text("\n\n".join(lines)))

    async def _handle_manual_standup(self, turn_context: TurnContext, team: dict | None) -> None:
        if not team:
            await turn_context.send_activity(MessageFactory.text("You're not in any configured team."))
            return

        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        card = build_prompt_card(team["name"], date_str)
        attachment = CardFactory.adaptive_card(card)
        await turn_context.send_activity(MessageFactory.attachment(attachment))

    async def _handle_skip(self, turn_context: TurnContext, team: dict | None, upn: str) -> None:
        if not team:
            await turn_context.send_activity(MessageFactory.text("You're not in any configured team."))
            return

        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        await store_response(
            team_name=team["name"],
            date_str=date_str,
            user_upn=upn,
            yesterday="",
            today="",
            blockers="",
            skipped=True,
        )
        await turn_context.send_activity(
            MessageFactory.text(f"Marked as skipped (OOO) for **{team['name']}** ({date_str}).")
        )

    async def _handle_history(self, turn_context: TurnContext, team: dict | None, upn: str) -> None:
        if not team:
            await turn_context.send_activity(MessageFactory.text("You're not in any configured team."))
            return

        entries = await get_user_history(team["name"], upn, limit=5)
        if not entries:
            await turn_context.send_activity(MessageFactory.text("No standup history found."))
            return

        lines = [f"**Your last {len(entries)} standups ({team['name']}):**"]
        for e in entries:
            if e.get("skipped"):
                lines.append(f"\n**{e['date']}** -- Skipped (OOO)")
            else:
                lines.append(
                    f"\n**{e['date']}**\n"
                    f"- Yesterday: {e.get('yesterday', 'N/A')}\n"
                    f"- Today: {e.get('today', 'N/A')}\n"
                    f"- Blockers: {e.get('blockers', 'None')}"
                )
        await turn_context.send_activity(MessageFactory.text("\n".join(lines)))

    @staticmethod
    def _get_upn(turn_context: TurnContext) -> str:
        """Extract user principal name from the activity."""
        from_obj = turn_context.activity.from_property
        return (
            getattr(from_obj, "aad_object_id", None)
            or getattr(from_obj, "id", None)
            or "unknown"
        )
