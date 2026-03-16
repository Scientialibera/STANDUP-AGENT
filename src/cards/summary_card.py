from __future__ import annotations

from typing import Any


def build_summary_card(team_name: str, date_str: str, summary: dict[str, Any]) -> dict[str, Any]:
    """Adaptive Card for the daily team summary posted to a channel."""
    body: list[dict[str, Any]] = [
        {"type": "TextBlock", "text": f"Team Summary -- {team_name}", "weight": "Bolder", "size": "Large"},
        {"type": "TextBlock", "text": date_str, "isSubtle": True, "spacing": "None"},
    ]

    narrative = summary.get("narrative", "")
    if narrative:
        body.append({"type": "TextBlock", "text": narrative, "wrap": True, "spacing": "Medium"})

    themes = summary.get("themes", [])
    if themes:
        body.append({"type": "TextBlock", "text": "Key Themes", "weight": "Bolder", "spacing": "Medium"})
        for theme in themes:
            body.append({"type": "TextBlock", "text": f"- {theme}", "wrap": True, "spacing": "None"})

    blockers = summary.get("blockers", [])
    if blockers:
        body.append({"type": "TextBlock", "text": "Blockers", "weight": "Bolder", "spacing": "Medium", "color": "Attention"})
        for b in blockers:
            text = b if isinstance(b, str) else f"{b.get('member', '?')}: {b.get('description', str(b))}"
            body.append({"type": "TextBlock", "text": f"- {text}", "wrap": True, "spacing": "None"})

    deps = summary.get("cross_dependencies", [])
    if deps:
        body.append({"type": "TextBlock", "text": "Cross-Dependencies", "weight": "Bolder", "spacing": "Medium"})
        for d in deps:
            body.append({"type": "TextBlock", "text": f"- {d}", "wrap": True, "spacing": "None"})

    highlights = summary.get("highlights", [])
    if highlights:
        body.append({"type": "TextBlock", "text": "Highlights", "weight": "Bolder", "spacing": "Medium", "color": "Good"})
        for h in highlights:
            body.append({"type": "TextBlock", "text": f"- {h}", "wrap": True, "spacing": "None"})

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
    }
