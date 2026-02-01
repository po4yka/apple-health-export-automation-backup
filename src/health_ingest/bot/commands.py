"""Bot command parsing and validation."""

from dataclasses import dataclass
from enum import Enum


class BotCommand(str, Enum):
    """Supported bot commands."""

    NOW = "now"
    HEART = "heart"
    SLEEP = "sleep"
    WEIGHT = "weight"
    TODAY = "today"
    YESTERDAY = "yesterday"
    WEEK = "week"
    STEPS = "steps"
    WORKOUTS = "workouts"
    TRENDS = "trends"
    HELP = "help"


COMMAND_DESCRIPTIONS: dict[BotCommand, str] = {
    BotCommand.NOW: "Quick snapshot: steps, calories, exercise, HR, HRV, weight",
    BotCommand.HEART: "Resting HR, HRV with 7-day comparison",
    BotCommand.SLEEP: "Last night: duration, stages, quality score",
    BotCommand.WEIGHT: "Latest weight with 7d and 30d trend",
    BotCommand.TODAY: "Full today: activity + heart + workouts",
    BotCommand.YESTERDAY: "Full yesterday summary",
    BotCommand.WEEK: "This week aggregated summary",
    BotCommand.STEPS: "Steps with daily breakdown (7d/14d/30d)",
    BotCommand.WORKOUTS: "Recent workout list (7d/14d/30d)",
    BotCommand.TRENDS: "Key metrics this week vs last week",
    BotCommand.HELP: "List available commands",
}

VALID_PERIODS = {"7d", "14d", "30d"}
DEFAULT_PERIOD = "7d"

COMMANDS_WITH_PERIOD = {BotCommand.STEPS, BotCommand.WORKOUTS}


@dataclass
class ParsedCommand:
    """Successfully parsed bot command."""

    command: BotCommand
    period: str
    raw_text: str


@dataclass
class ParseError:
    """Failed command parse result."""

    message: str
    raw_text: str


def parse_command(text: str) -> ParsedCommand | ParseError:
    """Parse a bot command string into a ParsedCommand or ParseError.

    Args:
        text: Raw message text, e.g. "/now" or "/steps 30d".

    Returns:
        ParsedCommand on success, ParseError on failure.
    """
    raw_text = text
    text = text.strip()

    if not text.startswith("/"):
        return ParseError(
            message="Commands must start with /. Use /help for available commands.",
            raw_text=raw_text,
        )

    parts = text[1:].lower().split(None, 1)
    if not parts:
        return ParseError(
            message="Empty command. Use /help for available commands.",
            raw_text=raw_text,
        )

    cmd_str = parts[0]

    try:
        command = BotCommand(cmd_str)
    except ValueError:
        return ParseError(
            message=f"Unknown command: /{cmd_str}. Use /help for available commands.",
            raw_text=raw_text,
        )

    period = DEFAULT_PERIOD
    if len(parts) > 1:
        arg = parts[1].strip()
        if command in COMMANDS_WITH_PERIOD:
            if arg not in VALID_PERIODS:
                valid = ", ".join(sorted(VALID_PERIODS))
                return ParseError(
                    message=f"Invalid period '{arg}'. Use one of: {valid}",
                    raw_text=raw_text,
                )
            period = arg
        # Ignore extra args for commands that don't take them

    return ParsedCommand(command=command, period=period, raw_text=raw_text)
