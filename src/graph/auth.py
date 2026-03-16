from __future__ import annotations

import logging
from typing import Any

import aiohttp
import msal

from src.config import get_settings

logger = logging.getLogger(__name__)

_GRAPH_BASE = "https://graph.microsoft.com/v1.0"
_SCOPE = ["https://graph.microsoft.com/.default"]

_cca: msal.ConfidentialClientApplication | None = None


def _get_cca() -> msal.ConfidentialClientApplication:
    global _cca
    if _cca is None:
        s = get_settings()
        authority = f"https://login.microsoftonline.com/{s.microsoft_app_tenant_id}"
        _cca = msal.ConfidentialClientApplication(
            client_id=s.microsoft_app_id,
            client_credential=s.microsoft_app_password,
            authority=authority,
        )
    return _cca


async def get_graph_token() -> str:
    cca = _get_cca()
    result = cca.acquire_token_silent(_SCOPE, account=None)
    if not result:
        result = cca.acquire_token_for_client(scopes=_SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"Graph token acquisition failed: {result.get('error_description', result)}")
    return result["access_token"]


async def graph_get(path: str, params: dict | None = None) -> Any:
    token = await get_graph_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{_GRAPH_BASE}{path}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()


async def graph_post(path: str, body: dict) -> Any:
    token = await get_graph_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{_GRAPH_BASE}{path}"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=body) as resp:
            resp.raise_for_status()
            if resp.content_length and resp.content_length > 0:
                return await resp.json()
            return None
