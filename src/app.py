"""aiohttp entry point for the Standup Agent bot."""

from __future__ import annotations

import logging
import sys
import traceback

from aiohttp import web
from aiohttp.web import Request, Response
from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity

from src.background.scheduler import StandupScheduler
from src.bot import StandupBot
from src.config import get_settings
from src.state.team_state import store_conversation_reference

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


async def _on_error(context: TurnContext, error: Exception) -> None:
    logger.error("Unhandled bot error: %s", error, exc_info=True)
    await context.send_activity("Sorry, something went wrong. Please try again later.")


def _create_adapter() -> BotFrameworkAdapter:
    s = get_settings()
    settings = BotFrameworkAdapterSettings(
        app_id=s.microsoft_app_id,
        app_password=s.microsoft_app_password,
        channel_auth_tenant=s.microsoft_app_tenant_id,
    )
    adapter = BotFrameworkAdapter(settings)
    adapter.on_turn_error = _on_error
    return adapter


async def _messages(req: Request) -> Response:
    if "application/json" not in (req.content_type or ""):
        return Response(status=415)
    body = await req.json()
    activity = Activity().deserialize(body)

    adapter: BotFrameworkAdapter = req.app["adapter"]
    bot: StandupBot = req.app["bot"]

    async def _turn_callback(turn_context: TurnContext) -> None:
        _save_ref(turn_context)
        await bot.on_turn(turn_context)

    auth_header = req.headers.get("Authorization", "")
    response = await adapter.process_activity(activity, auth_header, _turn_callback)
    if response:
        return Response(body=response.body, status=response.status)
    return Response(status=201)


def _save_ref(turn_context: TurnContext) -> None:
    """Persist conversation reference for proactive messaging."""
    ref = TurnContext.get_conversation_reference(turn_context.activity)
    from_prop = turn_context.activity.from_property
    key = getattr(from_prop, "aad_object_id", None) or getattr(from_prop, "id", None)
    if key:
        store_conversation_reference(key, ref.as_dict() if hasattr(ref, "as_dict") else ref.__dict__)


async def _health(req: Request) -> Response:
    return Response(text="OK")


async def init_app() -> web.Application:
    app = web.Application()
    adapter = _create_adapter()
    bot = StandupBot()
    scheduler = StandupScheduler(adapter)

    app["adapter"] = adapter
    app["bot"] = bot
    app["scheduler"] = scheduler

    app.router.add_post("/api/messages", _messages)
    app.router.add_get("/health", _health)

    async def on_startup(_app: web.Application) -> None:
        try:
            await scheduler.start()
        except Exception:
            logger.error("Scheduler start failed -- will retry on config reload.", exc_info=True)

    app.on_startup.append(on_startup)
    return app


if __name__ == "__main__":
    web.run_app(init_app(), port=get_settings().port)
