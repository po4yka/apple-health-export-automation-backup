"""AI insight generation for bot command responses."""

import asyncio
import json

import anthropic
import structlog
from openai import OpenAI

from ..config import AnthropicSettings, GrokSettings, InsightSettings, OpenAISettings

logger = structlog.get_logger(__name__)

BOT_INSIGHT_PROMPT = """You are a health assistant providing quick insights in a chat.

HEALTH DATA:
{data_text}

COMMAND: {command}

Generate {max_insights} brief insights. Each should be:
- 1-2 sentences maximum
- Actionable when possible
- Focused on what stands out or matters most

Return a JSON array: [{{"text": "insight text"}}]
Only return the JSON array, no other text."""


class BotInsightEngine:
    """Generates short AI insights for bot command responses."""

    def __init__(
        self,
        anthropic_settings: AnthropicSettings,
        openai_settings: OpenAISettings,
        grok_settings: GrokSettings,
        insight_settings: InsightSettings,
    ) -> None:
        self._anthropic_settings = anthropic_settings
        self._openai_settings = openai_settings
        self._grok_settings = grok_settings
        self._insight_settings = insight_settings

    def _provider_configured(self, provider: str) -> bool:
        """Check if the requested provider has credentials configured."""
        if provider == "anthropic":
            return bool(self._anthropic_settings.api_key)
        if provider == "openai":
            return bool(self._openai_settings.api_key)
        if provider == "grok":
            return bool(self._grok_settings.api_key)
        return False

    async def generate(self, data_text: str, command: str, max_insights: int = 2) -> list[str]:
        """Return list of insight strings. Empty list on any failure."""
        provider = self._insight_settings.ai_provider
        if not self._provider_configured(provider):
            logger.debug("bot_insight_no_api_key", provider=provider)
            return []

        prompt = BOT_INSIGHT_PROMPT.format(
            data_text=data_text,
            command=command,
            max_insights=max_insights,
        )

        try:
            response_text = await self._call_provider(provider, prompt)
        except Exception as e:
            logger.warning("bot_insight_api_error", error=str(e), provider=provider)
            return []

        return self._parse_response(response_text)

    async def _call_provider(self, provider: str, prompt: str) -> str:
        """Call the AI provider and return raw response text."""
        timeout = self._insight_settings.ai_timeout_seconds

        def do_request():
            if provider == "anthropic":
                client = anthropic.Anthropic(
                    api_key=self._anthropic_settings.api_key,
                    timeout=timeout,
                )
                return client.messages.create(
                    model=self._anthropic_settings.model,
                    max_tokens=512,
                    messages=[{"role": "user", "content": prompt}],
                )

            if provider == "openai":
                client = OpenAI(
                    api_key=self._openai_settings.api_key,
                    base_url=self._openai_settings.base_url,
                    timeout=timeout,
                )
                return client.chat.completions.create(
                    model=self._openai_settings.model,
                    max_tokens=512,
                    messages=[{"role": "user", "content": prompt}],
                )

            # grok
            client = OpenAI(
                api_key=self._grok_settings.api_key,
                base_url=self._grok_settings.base_url,
                timeout=timeout,
            )
            return client.chat.completions.create(
                model=self._grok_settings.model,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )

        message = await asyncio.wait_for(
            asyncio.to_thread(do_request),
            timeout=timeout,
        )

        if provider == "anthropic":
            return message.content[0].text.strip()
        return (message.choices[0].message.content or "").strip()

    def _parse_response(self, response_text: str) -> list[str]:
        """Parse JSON response into list of insight strings."""
        try:
            # Handle markdown code blocks
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                response_text = "\n".join(lines[1:-1])

            data = json.loads(response_text)
            return [item["text"] for item in data if isinstance(item, dict) and "text" in item]
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning("bot_insight_parse_error", error=str(e))
            return []
