"""AI insight generation with rule-based fallback."""

import json

import anthropic
import structlog

from ..config import AnthropicSettings, InsightSettings
from .models import InsightResult, PrivacySafeMetrics
from .rules import RuleEngine

logger = structlog.get_logger(__name__)

# AI prompt for structured insight generation
INSIGHT_PROMPT = """You are a health insights analyst. Analyze these AGGREGATED weekly \
health metrics and provide personalized insights with clear reasoning.

IMPORTANT: These are pre-aggregated summaries only - no raw data or timestamps. \
Focus on trends, patterns, and actionable recommendations.

WEEKLY METRICS:
{metrics_text}

Generate {max_insights} health insights. For each insight, provide:
1. category: One of "activity", "heart", "sleep", "workouts", "body", or "correlation"
2. headline: A concise summary (max 60 characters)
3. reasoning: WHY this insight matters, referencing specific numbers from the metrics
4. recommendation: One specific, actionable suggestion for the coming week

Focus on:
- Celebrating achievements and positive trends
- Flagging concerning patterns that need attention
- Cross-metric correlations (e.g., how sleep affects HRV)
- Practical, achievable recommendations

Return your response as a JSON array of objects with these exact fields: \
category, headline, reasoning, recommendation.
Example format:
[
  {{"category": "activity", "headline": "Step goal achieved", \
"reasoning": "Averaged 10,500 steps/day...", "recommendation": "Try adding..."}}
]

Only return the JSON array, no other text."""


class InsightEngine:
    """Generates health insights with AI primary, rule-based fallback."""

    def __init__(
        self,
        anthropic_settings: AnthropicSettings,
        insight_settings: InsightSettings,
    ) -> None:
        """Initialize the insight engine.

        Args:
            anthropic_settings: Anthropic API configuration.
            insight_settings: Insight generation settings.
        """
        self._anthropic_settings = anthropic_settings
        self._insight_settings = insight_settings
        self._rule_engine = RuleEngine()

    async def generate(self, metrics: PrivacySafeMetrics) -> list[InsightResult]:
        """Generate insights from metrics.

        Uses AI when available and configured, falls back to rules otherwise.

        Args:
            metrics: Privacy-safe aggregated metrics.

        Returns:
            List of InsightResult objects.
        """
        # Try AI first if configured and preferred
        if (
            self._insight_settings.prefer_ai
            and self._anthropic_settings.api_key
        ):
            try:
                insights = await self._generate_ai_insights(metrics)
                if insights:
                    logger.info("insights_generated", source="ai", count=len(insights))
                    return insights
            except anthropic.APIConnectionError as e:
                logger.warning("ai_connection_error", error=str(e))
            except anthropic.RateLimitError as e:
                logger.warning("ai_rate_limit", error=str(e))
            except anthropic.APIStatusError as e:
                logger.warning("ai_api_error", status=e.status_code, error=str(e))
            except Exception as e:
                logger.warning("ai_unexpected_error", error=str(e), error_type=type(e).__name__)

        # Fall back to rule-based insights
        insights = self._generate_rule_based(metrics)
        logger.info("insights_generated", source="rule", count=len(insights))
        return insights

    async def _generate_ai_insights(self, metrics: PrivacySafeMetrics) -> list[InsightResult]:
        """Generate insights using Claude API.

        Args:
            metrics: Privacy-safe metrics.

        Returns:
            List of AI-generated insights.
        """
        metrics_text = metrics.to_summary_text()
        prompt = INSIGHT_PROMPT.format(
            metrics_text=metrics_text,
            max_insights=self._insight_settings.max_insights,
        )

        client = anthropic.Anthropic(api_key=self._anthropic_settings.api_key)

        message = client.messages.create(
            model=self._anthropic_settings.model,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = message.content[0].text.strip()

        # Parse JSON response
        try:
            # Handle potential markdown code blocks
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                # Remove first and last lines (code block markers)
                response_text = "\n".join(lines[1:-1])

            insights_data = json.loads(response_text)

            insights = []
            for item in insights_data[: self._insight_settings.max_insights]:
                insights.append(
                    InsightResult(
                        category=item.get("category", "general"),
                        headline=item.get("headline", "")[:60],
                        reasoning=item.get("reasoning", ""),
                        recommendation=item.get("recommendation", ""),
                        confidence=1.0,  # AI confidence is 1.0
                        source="ai",
                    )
                )

            return insights

        except json.JSONDecodeError as e:
            logger.warning("ai_response_parse_error", error=str(e), response=response_text[:200])
            return []

    def _generate_rule_based(self, metrics: PrivacySafeMetrics) -> list[InsightResult]:
        """Generate insights using predefined rules.

        Args:
            metrics: Privacy-safe metrics.

        Returns:
            List of rule-based insights.
        """
        return self._rule_engine.evaluate(
            metrics,
            max_insights=self._insight_settings.max_insights,
        )
