from __future__ import annotations

from typing import Any


def build_prompt_card(team_name: str, date_str: str) -> dict[str, Any]:
    """Adaptive Card form for daily standup collection."""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": [
            {
                "type": "TextBlock",
                "text": f"Daily Standup -- {team_name}",
                "weight": "Bolder",
                "size": "Large",
            },
            {
                "type": "TextBlock",
                "text": date_str,
                "isSubtle": True,
                "spacing": "None",
            },
            {"type": "TextBlock", "text": "What did you do yesterday?", "weight": "Bolder", "spacing": "Medium"},
            {
                "type": "Input.Text",
                "id": "yesterday",
                "placeholder": "Describe yesterday's work...",
                "isMultiline": True,
                "maxLength": 2000,
            },
            {"type": "TextBlock", "text": "What's planned for today?", "weight": "Bolder", "spacing": "Medium"},
            {
                "type": "Input.Text",
                "id": "today",
                "placeholder": "Describe today's plan...",
                "isMultiline": True,
                "maxLength": 2000,
            },
            {"type": "TextBlock", "text": "Any blockers?", "weight": "Bolder", "spacing": "Medium"},
            {
                "type": "Input.Text",
                "id": "blockers",
                "placeholder": "Describe any blockers or type 'None'...",
                "isMultiline": True,
                "maxLength": 2000,
            },
        ],
        "actions": [
            {
                "type": "Action.Submit",
                "title": "Submit Standup",
                "data": {"action": "submit_standup", "team_name": team_name, "date": date_str},
            },
            {
                "type": "Action.Submit",
                "title": "Skip (OOO)",
                "style": "destructive",
                "data": {"action": "skip_standup", "team_name": team_name, "date": date_str},
            },
        ],
    }
