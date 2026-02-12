"""AI insight generation for bot command responses."""

import asyncio
import json
import time

import anthropic
import structlog
from openai import OpenAI

from ..config import AnthropicSettings, GrokSettings, InsightSettings, OpenAISettings
from ..reports.analysis_contract import (
    AnalysisProvenance,
    AnalysisRequestType,
    dataset_version_for_text,
    get_analysis_profile,
    load_prompt_template,
)
from ..reports.analysis_monitoring import record_analysis_observation

logger = structlog.get_logger(__name__)

BOT_INSIGHT_PROMPT = load_prompt_template("bot_command").text

_PRICING_USD_PER_MILLION: dict[str, tuple[float, float]] = {
    "claude-sonnet-4": (3.0, 15.0),
    "gpt-4o-mini": (0.15, 0.6),
    "gpt-4o": (2.5, 10.0),
    "grok-2": (2.0, 10.0),
}
_DEFAULT_PROVIDER_PRICING_USD_PER_MILLION: dict[str, tuple[float, float]] = {
    "anthropic": (3.0, 15.0),
    "openai": (1.0, 4.0),
    "grok": (2.0, 10.0),
}


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
        self._last_provenance: AnalysisProvenance | None = None

    @property
    def last_provenance(self) -> AnalysisProvenance | None:
        """Most recent bot insight run provenance."""
        return self._last_provenance

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
        run_start = time.perf_counter()
        provider = self._insight_settings.ai_provider
        profile = get_analysis_profile(AnalysisRequestType.BOT_COMMAND_INSIGHT)
        prompt_template = load_prompt_template(profile.prompt_id)
        dataset_version = dataset_version_for_text(data_text)
        model = self._model_for_provider(provider)

        if not self._provider_configured(provider):
            provenance = AnalysisProvenance(
                request_type=AnalysisRequestType.BOT_COMMAND_INSIGHT.value,
                source="disabled",
                provider=provider,
                model=model,
                dataset_version=dataset_version,
                prompt_id=prompt_template.prompt_id,
                prompt_version=prompt_template.version,
                prompt_hash=prompt_template.sha256,
            )
            self._last_provenance = provenance
            record_analysis_observation(
                provenance=provenance,
                status="disabled",
                latency_seconds=time.perf_counter() - run_start,
                estimated_cost_usd=0.0,
                quality_score=0.0,
            )
            logger.debug("bot_insight_no_api_key", provider=provider)
            return []

        prompt = prompt_template.text.format(
            analysis_objective=profile.objective,
            expected_outcome=profile.expected_outcome,
            dataset_version=dataset_version,
            data_text=data_text,
            command=command,
            max_insights=max_insights,
        )

        try:
            response_text, prompt_tokens, completion_tokens = await self._call_provider(
                provider,
                prompt,
            )
        except Exception as e:
            provenance = AnalysisProvenance(
                request_type=AnalysisRequestType.BOT_COMMAND_INSIGHT.value,
                source="ai",
                provider=provider,
                model=model,
                dataset_version=dataset_version,
                prompt_id=prompt_template.prompt_id,
                prompt_version=prompt_template.version,
                prompt_hash=prompt_template.sha256,
            )
            self._last_provenance = provenance
            record_analysis_observation(
                provenance=provenance,
                status="error",
                latency_seconds=time.perf_counter() - run_start,
                estimated_cost_usd=0.0,
                quality_score=0.0,
            )
            logger.warning("bot_insight_api_error", error=str(e), provider=provider)
            return []

        insights = self._parse_response(response_text)
        provenance = AnalysisProvenance(
            request_type=AnalysisRequestType.BOT_COMMAND_INSIGHT.value,
            source="ai",
            provider=provider,
            model=model,
            dataset_version=dataset_version,
            prompt_id=prompt_template.prompt_id,
            prompt_version=prompt_template.version,
            prompt_hash=prompt_template.sha256,
        )
        self._last_provenance = provenance
        record_analysis_observation(
            provenance=provenance,
            status="success" if insights else "empty",
            latency_seconds=time.perf_counter() - run_start,
            estimated_cost_usd=self._estimate_cost_usd(
                provider=provider,
                model=model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            ),
            quality_score=self._evaluate_quality(insights, max_insights=max_insights),
        )
        return insights

    async def _call_provider(
        self,
        provider: str,
        prompt: str,
    ) -> tuple[str, int | None, int | None]:
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
            usage = getattr(message, "usage", None)
            prompt_tokens = getattr(usage, "input_tokens", None) if usage else None
            completion_tokens = getattr(usage, "output_tokens", None) if usage else None
            return message.content[0].text.strip(), prompt_tokens, completion_tokens
        usage = getattr(message, "usage", None)
        prompt_tokens = getattr(usage, "prompt_tokens", None) if usage else None
        completion_tokens = getattr(usage, "completion_tokens", None) if usage else None
        return (message.choices[0].message.content or "").strip(), prompt_tokens, completion_tokens

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

    def _model_for_provider(self, provider: str) -> str:
        if provider == "anthropic":
            return self._anthropic_settings.model
        if provider == "openai":
            return self._openai_settings.model
        if provider == "grok":
            return self._grok_settings.model
        return "unknown"

    def _estimate_cost_usd(
        self,
        *,
        provider: str,
        model: str,
        prompt_tokens: int | None,
        completion_tokens: int | None,
    ) -> float:
        input_tokens = self._token_count(prompt_tokens)
        output_tokens = self._token_count(completion_tokens)
        if input_tokens is None and output_tokens is None:
            return 0.0
        input_tokens = input_tokens or 0
        output_tokens = output_tokens or 0

        pricing = None
        model_lower = model.lower()
        for model_hint, candidate in _PRICING_USD_PER_MILLION.items():
            if model_hint in model_lower:
                pricing = candidate
                break
        if pricing is None:
            pricing = _DEFAULT_PROVIDER_PRICING_USD_PER_MILLION.get(provider, (1.0, 4.0))

        in_rate, out_rate = pricing
        return (input_tokens / 1_000_000 * in_rate) + (output_tokens / 1_000_000 * out_rate)

    def _evaluate_quality(self, insights: list[str], max_insights: int) -> float:
        if not insights:
            return 0.0
        coverage = min(len(insights), max(max_insights, 1)) / max(max_insights, 1)
        average_length_ok = sum(1 for item in insights if len(item) <= 220) / len(insights)
        return (0.6 * coverage) + (0.4 * average_length_ok)

    def _token_count(self, value: object) -> int | None:
        """Return normalized token count or None when unavailable."""
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return max(value, 0)
        if isinstance(value, float):
            return max(int(value), 0)
        return None
