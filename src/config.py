from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    microsoft_app_id: str = ""
    microsoft_app_password: str = ""
    microsoft_app_tenant_id: str = ""
    microsoft_app_type: str = "SingleTenant"

    blob_account_url: str = ""
    blob_responses_container: str = "standup-responses"
    blob_config_container: str = "bot-config"

    aoai_endpoint: str = ""
    aoai_api_version: str = "2024-12-01-preview"
    aoai_chat_deployment: str = "gpt-4-1"

    port: int = 8000

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            microsoft_app_id=os.getenv("MICROSOFT_APP_ID", ""),
            microsoft_app_password=os.getenv("MICROSOFT_APP_PASSWORD", ""),
            microsoft_app_tenant_id=os.getenv("MICROSOFT_APP_TENANT_ID", ""),
            microsoft_app_type=os.getenv("MICROSOFT_APP_TYPE", "SingleTenant"),
            blob_account_url=os.getenv("BLOB_ACCOUNT_URL", ""),
            blob_responses_container=os.getenv("BLOB_RESPONSES_CONTAINER", "standup-responses"),
            blob_config_container=os.getenv("BLOB_CONFIG_CONTAINER", "bot-config"),
            aoai_endpoint=os.getenv("AOAI_ENDPOINT", ""),
            aoai_api_version=os.getenv("AOAI_API_VERSION", "2024-12-01-preview"),
            aoai_chat_deployment=os.getenv("AOAI_CHAT_DEPLOYMENT", "gpt-4-1"),
            port=int(os.getenv("PORT", "8000")),
        )


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
    return _settings
