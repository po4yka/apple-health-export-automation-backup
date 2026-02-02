"""Tests for bot insight engine."""

import asyncio
import json
from unittest.mock import MagicMock, patch

from health_ingest.bot.insights import BotInsightEngine
from health_ingest.config import AnthropicSettings, GrokSettings, InsightSettings, OpenAISettings


def _make_engine(api_key: str | None = "test-key") -> BotInsightEngine:
    return BotInsightEngine(
        anthropic_settings=AnthropicSettings(_env_file=None, api_key=api_key),
        openai_settings=OpenAISettings(_env_file=None),
        grok_settings=GrokSettings(_env_file=None),
        insight_settings=InsightSettings(_env_file=None, ai_timeout_seconds=5.0),
    )


class TestBotInsightEngine:
    async def test_generate_returns_insights(self):
        engine = _make_engine()
        mock_response = MagicMock()
        mock_response.content = [
            MagicMock(
                text=json.dumps(
                    [
                        {"text": "Your resting HR is lower than average."},
                        {"text": "Consider more deep sleep."},
                    ]
                )
            )
        ]

        with patch("health_ingest.bot.insights.anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.return_value = mock_response
            result = await engine.generate("HR: 60 bpm", "health_heart")

        assert len(result) == 2
        assert "resting HR" in result[0]

    async def test_generate_handles_json_error(self):
        engine = _make_engine()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="not valid json at all")]

        with patch("health_ingest.bot.insights.anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.return_value = mock_response
            result = await engine.generate("HR: 60 bpm", "health_heart")

        assert result == []

    async def test_generate_handles_timeout(self):
        engine = _make_engine()

        async def slow_thread(fn):
            await asyncio.sleep(10.0)

        with patch("health_ingest.bot.insights.asyncio.to_thread", side_effect=slow_thread):
            result = await engine.generate("HR: 60 bpm", "health_heart")

        assert result == []

    async def test_generate_handles_no_api_key(self):
        engine = _make_engine(api_key=None)
        result = await engine.generate("HR: 60 bpm", "health_heart")
        assert result == []

    async def test_generate_handles_markdown_code_block(self):
        engine = _make_engine()
        json_body = json.dumps([{"text": "Good HRV trend."}])
        wrapped = f"```json\n{json_body}\n```"
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=wrapped)]

        with patch("health_ingest.bot.insights.anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.return_value = mock_response
            result = await engine.generate("HRV: 45 ms", "health_heart")

        assert len(result) == 1
        assert "HRV" in result[0]
