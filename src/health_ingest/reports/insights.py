"""AI insight generation with rule-based fallback."""

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path

import anthropic
import openai
import structlog
from openai import OpenAI

from ..circuit_breaker import CircuitBreaker
from ..config import AnthropicSettings, GrokSettings, InsightSettings, OpenAISettings
from .analysis_contract import (
    AnalysisProvenance,
    AnalysisRequestType,
    PromptTemplate,
    dataset_version_for_text,
    get_analysis_profile,
    load_prompt_template,
)
from .analysis_monitoring import record_analysis_observation
from .models import InsightResult, PrivacySafeDailyMetrics, PrivacySafeMetrics
from .rules import RuleEngine

logger = structlog.get_logger(__name__)

INSIGHT_PROMPT = load_prompt_template("weekly_insight").text
DAILY_MORNING_PROMPT = load_prompt_template("daily_morning").text
DAILY_EVENING_PROMPT = load_prompt_template("daily_evening").text

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


@dataclass(frozen=True)
class _AIGenerationResult:
    insights: list[InsightResult]
    prompt_tokens: int | None
    completion_tokens: int | None
    model: str
    prompt_template: PromptTemplate
    dataset_version: str


class InsightEngine:
    """Generates health insights with AI primary, rule-based fallback."""

    def __init__(
        self,
        anthropic_settings: AnthropicSettings,
        insight_settings: InsightSettings,
        openai_settings: OpenAISettings | None = None,
        grok_settings: GrokSettings | None = None,
    ) -> None:
        self._anthropic_settings = anthropic_settings
        self._openai_settings = openai_settings or OpenAISettings()
        self._grok_settings = grok_settings or GrokSettings()
        self._insight_settings = insight_settings
        self._rule_engine = RuleEngine()
        self._circuit_breaker = CircuitBreaker(
            name="ai_insights",
            failure_threshold=5,
            recovery_timeout=60.0,
        )
        self._last_provenance: AnalysisProvenance | None = None

    @property
    def last_provenance(self) -> AnalysisProvenance | None:
        """Most recent run provenance."""
        return self._last_provenance

    async def generate(
        self,
        metrics: PrivacySafeMetrics | PrivacySafeDailyMetrics,
        prompt_template: str | None = None,
        max_insights_override: int | None = None,
        request_type: AnalysisRequestType = AnalysisRequestType.WEEKLY_SUMMARY,
    ) -> list[InsightResult]:
        """Generate insights from metrics with explicit request profile."""
        run_start = time.perf_counter()
        profile = get_analysis_profile(request_type)
        max_insights = max_insights_override or self._insight_settings.max_insights
        prompt_tpl = self._resolve_prompt_template(profile.prompt_id, prompt_template)
        metrics_text = metrics.to_summary_text()
        dataset_version = dataset_version_for_text(metrics_text)
        provider = self._insight_settings.ai_provider
        ai_attempted = False

        if self._insight_settings.prefer_ai and self._provider_configured(provider):
            if self._circuit_breaker.is_open:
                logger.warning("ai_circuit_open_fallback_to_rules", request_type=request_type.value)
            else:
                ai_attempted = True
                try:
                    ai_result = await self._generate_ai_insights(
                        provider=provider,
                        metrics_text=metrics_text,
                        prompt_template=prompt_tpl,
                        dataset_version=dataset_version,
                        request_type=request_type,
                        max_insights=max_insights,
                    )
                    if isinstance(ai_result, list) and ai_result:
                        self._circuit_breaker.record_success()
                        provenance = AnalysisProvenance(
                            request_type=request_type.value,
                            source="ai",
                            provider=provider,
                            model=self._model_for_provider(provider),
                            dataset_version=dataset_version,
                            prompt_id=prompt_tpl.prompt_id,
                            prompt_version=prompt_tpl.version,
                            prompt_hash=prompt_tpl.sha256,
                        )
                        self._last_provenance = provenance
                        record_analysis_observation(
                            provenance=provenance,
                            status="success",
                            latency_seconds=time.perf_counter() - run_start,
                            estimated_cost_usd=0.0,
                            quality_score=1.0,
                        )
                        logger.info(
                            "insights_generated",
                            source="ai",
                            count=len(ai_result),
                            request_type=request_type.value,
                        )
                        return ai_result
                    if ai_result.insights:
                        self._circuit_breaker.record_success()
                        quality_score = self._evaluate_quality(ai_result.insights, max_insights)
                        estimated_cost = self._estimate_cost_usd(
                            provider=provider,
                            model=ai_result.model,
                            prompt_tokens=ai_result.prompt_tokens,
                            completion_tokens=ai_result.completion_tokens,
                        )
                        provenance = AnalysisProvenance(
                            request_type=request_type.value,
                            source="ai",
                            provider=provider,
                            model=ai_result.model,
                            dataset_version=ai_result.dataset_version,
                            prompt_id=ai_result.prompt_template.prompt_id,
                            prompt_version=ai_result.prompt_template.version,
                            prompt_hash=ai_result.prompt_template.sha256,
                        )
                        self._last_provenance = provenance
                        record_analysis_observation(
                            provenance=provenance,
                            status="success",
                            latency_seconds=time.perf_counter() - run_start,
                            estimated_cost_usd=estimated_cost,
                            quality_score=quality_score,
                        )
                        logger.info(
                            "insights_generated",
                            source="ai",
                            count=len(ai_result.insights),
                            request_type=request_type.value,
                        )
                        return ai_result.insights
                except anthropic.APIConnectionError as e:
                    self._circuit_breaker.record_failure()
                    logger.warning("ai_connection_error", error=str(e))
                except anthropic.RateLimitError as e:
                    self._circuit_breaker.record_failure()
                    logger.warning("ai_rate_limit", error=str(e))
                except anthropic.APIStatusError as e:
                    if e.status_code >= 500:
                        self._circuit_breaker.record_failure()
                    logger.warning("ai_api_error", status=e.status_code, error=str(e))
                except anthropic.APITimeoutError as e:
                    self._circuit_breaker.record_failure()
                    logger.warning(
                        "ai_timeout",
                        error=str(e),
                        timeout=self._insight_settings.ai_timeout_seconds,
                    )
                except openai.APIConnectionError as e:
                    self._circuit_breaker.record_failure()
                    logger.warning("ai_connection_error", error=str(e))
                except openai.RateLimitError as e:
                    self._circuit_breaker.record_failure()
                    logger.warning("ai_rate_limit", error=str(e))
                except openai.APIStatusError as e:
                    if e.status_code >= 500:
                        self._circuit_breaker.record_failure()
                    logger.warning("ai_api_error", status=e.status_code, error=str(e))
                except openai.APITimeoutError as e:
                    self._circuit_breaker.record_failure()
                    logger.warning(
                        "ai_timeout",
                        error=str(e),
                        timeout=self._insight_settings.ai_timeout_seconds,
                    )
                except Exception as e:
                    self._circuit_breaker.record_failure()
                    logger.warning(
                        "ai_unexpected_error",
                        error=str(e),
                        error_type=type(e).__name__,
                    )

        if isinstance(metrics, PrivacySafeDailyMetrics):
            insights = self._generate_daily_rule_based(metrics, max_insights)
        else:
            insights = self._generate_rule_based(metrics, max_insights)

        provenance = AnalysisProvenance(
            request_type=request_type.value,
            source="rule",
            provider="rule",
            model="rule-engine.v1",
            dataset_version=dataset_version,
            prompt_id=prompt_tpl.prompt_id,
            prompt_version=prompt_tpl.version,
            prompt_hash=prompt_tpl.sha256,
        )
        self._last_provenance = provenance
        record_analysis_observation(
            provenance=provenance,
            status="fallback" if ai_attempted else "success",
            latency_seconds=time.perf_counter() - run_start,
            estimated_cost_usd=0.0,
            quality_score=self._evaluate_quality(insights, max_insights),
        )
        logger.info(
            "insights_generated",
            source="rule",
            count=len(insights),
            request_type=request_type.value,
        )
        return insights

    def _provider_configured(self, provider: str) -> bool:
        """Check if the requested provider has credentials configured."""
        if provider == "anthropic":
            return bool(self._anthropic_settings.api_key)
        if provider == "openai":
            return bool(self._openai_settings.api_key)
        if provider == "grok":
            return bool(self._grok_settings.api_key)
        return False

    async def _generate_ai_insights(
        self,
        *,
        provider: str,
        metrics_text: str,
        prompt_template: PromptTemplate,
        dataset_version: str,
        request_type: AnalysisRequestType,
        max_insights: int,
    ) -> _AIGenerationResult:
        """Generate insights using the configured AI provider."""
        profile = get_analysis_profile(request_type)
        prompt = prompt_template.text.format(
            analysis_objective=profile.objective,
            expected_outcome=profile.expected_outcome,
            dataset_version=dataset_version,
            metrics_text=metrics_text,
            max_insights=max_insights,
        )

        def do_request():
            if provider == "anthropic":
                client = anthropic.Anthropic(
                    api_key=self._anthropic_settings.api_key,
                    timeout=self._insight_settings.ai_timeout_seconds,
                )
                return client.messages.create(
                    model=self._anthropic_settings.model,
                    max_tokens=2048,
                    messages=[{"role": "user", "content": prompt}],
                )
            if provider == "openai":
                client = OpenAI(
                    api_key=self._openai_settings.api_key,
                    base_url=self._openai_settings.base_url,
                    timeout=self._insight_settings.ai_timeout_seconds,
                )
                return client.chat.completions.create(
                    model=self._openai_settings.model,
                    max_tokens=2048,
                    messages=[{"role": "user", "content": prompt}],
                )
            client = OpenAI(
                api_key=self._grok_settings.api_key,
                base_url=self._grok_settings.base_url,
                timeout=self._insight_settings.ai_timeout_seconds,
            )
            return client.chat.completions.create(
                model=self._grok_settings.model,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
            )

        try:
            message = await asyncio.wait_for(
                asyncio.to_thread(do_request),
                timeout=self._insight_settings.ai_timeout_seconds,
            )
        except TimeoutError:
            logger.warning("ai_timeout", timeout=self._insight_settings.ai_timeout_seconds)
            return _AIGenerationResult(
                insights=[],
                prompt_tokens=None,
                completion_tokens=None,
                model=self._model_for_provider(provider),
                prompt_template=prompt_template,
                dataset_version=dataset_version,
            )

        if provider == "anthropic":
            response_text = message.content[0].text.strip()
            usage = getattr(message, "usage", None)
            prompt_tokens = getattr(usage, "input_tokens", None) if usage else None
            completion_tokens = getattr(usage, "output_tokens", None) if usage else None
        else:
            response_text = (message.choices[0].message.content or "").strip()
            usage = getattr(message, "usage", None)
            prompt_tokens = getattr(usage, "prompt_tokens", None) if usage else None
            completion_tokens = getattr(usage, "completion_tokens", None) if usage else None

        insights = self._parse_insights(response_text, max_insights)
        return _AIGenerationResult(
            insights=insights,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            model=self._model_for_provider(provider),
            prompt_template=prompt_template,
            dataset_version=dataset_version,
        )

    def _parse_insights(self, response_text: str, max_insights: int) -> list[InsightResult]:
        """Parse AI JSON response into structured insights."""
        try:
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                response_text = "\n".join(lines[1:-1])

            insights_data = json.loads(response_text)
            insights = []
            for item in insights_data[:max_insights]:
                insights.append(
                    InsightResult(
                        category=item.get("category", "general"),
                        headline=item.get("headline", "")[:60],
                        reasoning=item.get("reasoning", ""),
                        recommendation=item.get("recommendation", ""),
                        confidence=1.0,
                        source="ai",
                    )
                )
            return insights
        except json.JSONDecodeError as e:
            logger.warning("ai_response_parse_error", error=str(e), response=response_text[:200])
            return []

    def _generate_rule_based(
        self,
        metrics: PrivacySafeMetrics,
        max_insights: int | None = None,
    ) -> list[InsightResult]:
        """Generate insights using predefined rules."""
        return self._rule_engine.evaluate(
            metrics,
            max_insights=max_insights or self._insight_settings.max_insights,
        )

    def _generate_daily_rule_based(
        self,
        metrics: PrivacySafeDailyMetrics,
        max_insights: int | None = None,
    ) -> list[InsightResult]:
        """Generate daily insights using predefined rules."""
        return self._rule_engine.evaluate_daily(
            metrics,
            max_insights=max_insights or 3,
        )

    def _resolve_prompt_template(
        self,
        default_prompt_id: str,
        prompt_template: str | None,
    ) -> PromptTemplate:
        """Resolve default or inline override prompt into a versioned contract object."""
        if prompt_template is None:
            return load_prompt_template(default_prompt_id)
        digest = hashlib.sha256(prompt_template.encode("utf-8")).hexdigest()
        return PromptTemplate(
            prompt_id=f"{default_prompt_id}_override",
            version="inline",
            path=Path("<inline>"),
            text=prompt_template,
            sha256=digest,
        )

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
        """Estimate request cost from token usage and static pricing table."""
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

    def _evaluate_quality(self, insights: list[InsightResult], max_insights: int) -> float:
        """Compute a simple quality score for observability dashboards."""
        if not insights:
            return 0.0
        completeness = sum(
            1
            for insight in insights
            if insight.headline and insight.reasoning and insight.recommendation
        ) / len(insights)
        confidence = sum(insight.confidence for insight in insights) / len(insights)
        coverage = min(len(insights), max(max_insights, 1)) / max(max_insights, 1)
        return (0.5 * completeness) + (0.35 * confidence) + (0.15 * coverage)

    def _token_count(self, value: object) -> int | None:
        """Return normalized token count or None when unavailable."""
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return max(value, 0)
        if isinstance(value, float):
            return max(int(value), 0)
        return None
