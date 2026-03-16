from __future__ import annotations

from typing import Any


def build_weekly_card(team_name: str, week_label: str, rollup: dict[str, Any]) -> dict[str, Any]:
    """Adaptive Card for the weekly rollup posted to a channel."""
    body: list[dict[str, Any]] = [
        {"type": "TextBlock", "text": f"Weekly Rollup -- {team_name}", "weight": "Bolder", "size": "Large"},
        {"type": "TextBlock", "text": week_label, "isSubtle": True, "spacing": "None"},
    ]

    narrative = rollup.get("narrative", "")
    if narrative:
        body.append({"type": "TextBlock", "text": narrative, "wrap": True, "spacing": "Medium"})

    recurring = rollup.get("recurring_blockers", [])
    if recurring:
        body.append({"type": "TextBlock", "text": "Recurring Blockers", "weight": "Bolder", "spacing": "Medium", "color": "Attention"})
        for rb in recurring:
            text = rb if isinstance(rb, str) else f"{rb.get('blocker', '?')} (x{rb.get('count', '?')})"
            body.append({"type": "TextBlock", "text": f"- {text}", "wrap": True, "spacing": "None"})

    completed = rollup.get("completed_themes", [])
    if completed:
        body.append({"type": "TextBlock", "text": "Completed Themes", "weight": "Bolder", "spacing": "Medium", "color": "Good"})
        for c in completed:
            body.append({"type": "TextBlock", "text": f"- {c}", "wrap": True, "spacing": "None"})

    velocity = rollup.get("velocity_patterns", "")
    if velocity:
        body.append({"type": "TextBlock", "text": "Velocity Patterns", "weight": "Bolder", "spacing": "Medium"})
        body.append({"type": "TextBlock", "text": velocity, "wrap": True, "spacing": "None"})

    health = rollup.get("team_health_signals", "")
    if health:
        body.append({"type": "TextBlock", "text": "Team Health", "weight": "Bolder", "spacing": "Medium"})
        body.append({"type": "TextBlock", "text": health, "wrap": True, "spacing": "None"})

    recommendations = rollup.get("recommendations", [])
    if recommendations:
        body.append({"type": "TextBlock", "text": "Recommendations", "weight": "Bolder", "spacing": "Medium"})
        for r in recommendations:
            body.append({"type": "TextBlock", "text": f"- {r}", "wrap": True, "spacing": "None"})

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
    }
