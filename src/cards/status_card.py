from __future__ import annotations

from typing import Any


def build_status_card(
    team_name: str,
    date_str: str,
    responded: list[str],
    skipped: list[str],
    missing: list[str],
) -> dict[str, Any]:
    """Adaptive Card showing who responded, skipped, or didn't respond."""
    body: list[dict[str, Any]] = [
        {"type": "TextBlock", "text": f"Standup Status -- {team_name}", "weight": "Bolder", "size": "Large"},
        {"type": "TextBlock", "text": date_str, "isSubtle": True, "spacing": "None"},
    ]

    total = len(responded) + len(skipped) + len(missing)
    rate = (len(responded) + len(skipped)) / total * 100 if total > 0 else 0
    body.append(
        {"type": "TextBlock", "text": f"Response rate: {rate:.0f}% ({len(responded) + len(skipped)}/{total})", "spacing": "Medium"}
    )

    if responded:
        body.append({"type": "TextBlock", "text": "Responded", "weight": "Bolder", "spacing": "Medium", "color": "Good"})
        body.append({"type": "TextBlock", "text": ", ".join(responded), "wrap": True, "spacing": "None"})

    if skipped:
        body.append({"type": "TextBlock", "text": "Skipped (OOO)", "weight": "Bolder", "spacing": "Medium"})
        body.append({"type": "TextBlock", "text": ", ".join(skipped), "wrap": True, "spacing": "None"})

    if missing:
        body.append({"type": "TextBlock", "text": "No Response", "weight": "Bolder", "spacing": "Medium", "color": "Attention"})
        body.append({"type": "TextBlock", "text": ", ".join(missing), "wrap": True, "spacing": "None"})

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
    }
