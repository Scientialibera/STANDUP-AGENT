"""APScheduler-based background jobs for per-team standup prompts, summaries, and weekly rollups."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from botbuilder.core import BotFrameworkAdapter, TurnContext, CardFactory, MessageFactory

from src.cards.prompt_card import build_prompt_card
from src.cards.summary_card import build_summary_card
from src.cards.status_card import build_status_card
from src.cards.weekly_card import build_weekly_card
from src.graph.channels import post_adaptive_card_to_channel
from src.services.standup_collector import (
    get_responses,
    store_summary,
    store_status,
    store_weekly_rollup,
)
from src.services.summarizer import generate_daily_summary, generate_weekly_rollup
from src.services.team_config import TeamDefinition, load_team_config
from src.state.team_state import get_all_conversation_references

logger = logging.getLogger(__name__)

DAY_MAP = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6}


class StandupScheduler:
    def __init__(self, adapter: BotFrameworkAdapter):
        self.adapter = adapter
        self.scheduler = AsyncIOScheduler()

    async def start(self) -> None:
        teams = await load_team_config()
        for team in teams:
            self._register_team_jobs(team)
        self.scheduler.start()
        logger.info("Scheduler started with %d teams.", len(teams))

    def _register_team_jobs(self, team: TeamDefinition) -> None:
        tz = team.timezone or "UTC"

        prompt_h, prompt_m = map(int, team.prompt_time.split(":"))
        self.scheduler.add_job(
            self._send_prompts,
            CronTrigger(hour=prompt_h, minute=prompt_m, timezone=tz),
            args=[team],
            id=f"prompt_{team.name}",
            replace_existing=True,
        )

        summary_h, summary_m = map(int, team.summary_time.split(":"))
        self.scheduler.add_job(
            self._collect_and_summarize,
            CronTrigger(hour=summary_h, minute=summary_m, timezone=tz),
            args=[team],
            id=f"summary_{team.name}",
            replace_existing=True,
        )

        rollup_dow = DAY_MAP.get(team.weekly_rollup_day.lower(), 4)
        rollup_h, rollup_m = map(int, team.weekly_rollup_time.split(":"))
        self.scheduler.add_job(
            self._weekly_rollup,
            CronTrigger(day_of_week=rollup_dow, hour=rollup_h, minute=rollup_m, timezone=tz),
            args=[team],
            id=f"weekly_{team.name}",
            replace_existing=True,
        )

    async def _send_prompts(self, team: TeamDefinition) -> None:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        card = build_prompt_card(team.name, date_str)
        attachment = CardFactory.adaptive_card(card)
        refs = get_all_conversation_references()

        sent_count = 0
        for member in team.members:
            ref = refs.get(member.upn)
            if not ref:
                logger.warning("No conversation reference for %s -- cannot send prompt.", member.upn)
                continue
            try:
                await self.adapter.continue_conversation(
                    ref,
                    lambda tc: tc.send_activity(MessageFactory.attachment(attachment)),
                    app_id=None,
                )
                sent_count += 1
            except Exception:
                logger.error("Failed to send prompt to %s", member.upn, exc_info=True)

        logger.info("Sent %d/%d prompts for team %s.", sent_count, len(team.members), team.name)

    async def _collect_and_summarize(self, team: TeamDefinition) -> None:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        responses = await get_responses(team.name, date_str)

        member_upns = {m.upn for m in team.members}
        respondent_upns = {r["user"] for r in responses}
        responded = [m.display_name for m in team.members if m.upn in respondent_upns and not any(r.get("skipped") for r in responses if r["user"] == m.upn)]
        skipped = [m.display_name for m in team.members if any(r.get("skipped") for r in responses if r.get("user") == m.upn)]
        missing = [m.display_name for m in team.members if m.upn not in respondent_upns]

        summary = await generate_daily_summary(team.name, responses)
        summary["date"] = date_str
        await store_summary(team.name, date_str, summary)

        status = {"date": date_str, "responded": responded, "skipped": skipped, "missing": missing}
        await store_status(team.name, date_str, status)

        if team.team_id and team.summary_channel_id:
            summary_card = build_summary_card(team.name, date_str, summary)
            await post_adaptive_card_to_channel(team.team_id, team.summary_channel_id, summary_card)

            status_card = build_status_card(team.name, date_str, responded, skipped, missing)
            await post_adaptive_card_to_channel(team.team_id, team.summary_channel_id, status_card)

        logger.info("Summary generated for team %s on %s.", team.name, date_str)

    async def _weekly_rollup(self, team: TeamDefinition) -> None:
        today = datetime.utcnow()
        week_start = today - timedelta(days=today.weekday())
        daily_summaries = []

        for i in range(5):
            day = week_start + timedelta(days=i)
            date_str = day.strftime("%Y-%m-%d")
            responses = await get_responses(team.name, date_str)
            if responses:
                summary = await generate_daily_summary(team.name, responses)
                summary["date"] = date_str
                daily_summaries.append(summary)

        if not daily_summaries:
            logger.info("No data for weekly rollup for team %s.", team.name)
            return

        week_label = f"{today.year}-W{today.isocalendar()[1]:02d}"
        rollup = await generate_weekly_rollup(team.name, daily_summaries)
        rollup["week"] = week_label
        await store_weekly_rollup(team.name, week_label, rollup)

        if team.team_id and team.summary_channel_id:
            weekly_card = build_weekly_card(team.name, week_label, rollup)
            await post_adaptive_card_to_channel(team.team_id, team.summary_channel_id, weekly_card)

        logger.info("Weekly rollup generated for team %s (%s).", team.name, week_label)

    async def reload_config(self) -> None:
        """Reload team config and re-register all jobs."""
        from src.services.team_config import invalidate_cache
        invalidate_cache()
        for job in self.scheduler.get_jobs():
            job.remove()
        teams = await load_team_config()
        for team in teams:
            self._register_team_jobs(team)
        logger.info("Reloaded scheduler config with %d teams.", len(teams))
