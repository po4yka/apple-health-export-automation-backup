"""Tests for bot command parsing."""

from health_ingest.bot.commands import (
    DEFAULT_PERIOD,
    BotCommand,
    ParsedCommand,
    ParseError,
    parse_command,
)


class TestParseCommand:
    """Tests for parse_command()."""

    def test_parse_now(self):
        result = parse_command("/health_now")
        assert isinstance(result, ParsedCommand)
        assert result.command == BotCommand.NOW
        assert result.period == DEFAULT_PERIOD

    def test_parse_help(self):
        result = parse_command("/health_help")
        assert isinstance(result, ParsedCommand)
        assert result.command == BotCommand.HELP

    def test_parse_steps_with_period(self):
        result = parse_command("/health_steps 30d")
        assert isinstance(result, ParsedCommand)
        assert result.command == BotCommand.STEPS
        assert result.period == "30d"

    def test_parse_steps_default_period(self):
        result = parse_command("/health_steps")
        assert isinstance(result, ParsedCommand)
        assert result.command == BotCommand.STEPS
        assert result.period == DEFAULT_PERIOD

    def test_parse_workouts_with_period(self):
        result = parse_command("/health_workouts 14d")
        assert isinstance(result, ParsedCommand)
        assert result.command == BotCommand.WORKOUTS
        assert result.period == "14d"

    def test_case_insensitive(self):
        result = parse_command("/HEALTH_NOW")
        assert isinstance(result, ParsedCommand)
        assert result.command == BotCommand.NOW

    def test_mixed_case(self):
        result = parse_command("/Health_Steps 7d")
        assert isinstance(result, ParsedCommand)
        assert result.command == BotCommand.STEPS
        assert result.period == "7d"

    def test_leading_trailing_whitespace(self):
        result = parse_command("  /health_now  ")
        assert isinstance(result, ParsedCommand)
        assert result.command == BotCommand.NOW

    def test_all_commands_parse(self):
        for cmd in BotCommand:
            result = parse_command(f"/{cmd.value}")
            assert isinstance(result, ParsedCommand)
            assert result.command == cmd

    def test_unknown_command(self):
        result = parse_command("/foobar")
        assert isinstance(result, ParseError)
        assert "Unknown command" in result.message
        assert "/foobar" in result.raw_text

    def test_missing_slash(self):
        result = parse_command("now")
        assert isinstance(result, ParseError)
        assert "must start with /" in result.message

    def test_empty_command(self):
        result = parse_command("/")
        assert isinstance(result, ParseError)
        assert "Empty command" in result.message

    def test_invalid_period(self):
        result = parse_command("/health_steps 5d")
        assert isinstance(result, ParseError)
        assert "Invalid period" in result.message

    def test_extra_args_ignored_for_non_period_commands(self):
        result = parse_command("/health_now extra stuff")
        assert isinstance(result, ParsedCommand)
        assert result.command == BotCommand.NOW

    def test_raw_text_preserved(self):
        result = parse_command("/health_steps 30d")
        assert isinstance(result, ParsedCommand)
        assert result.raw_text == "/health_steps 30d"

    def test_raw_text_preserved_on_error(self):
        result = parse_command("bad")
        assert isinstance(result, ParseError)
        assert result.raw_text == "bad"
