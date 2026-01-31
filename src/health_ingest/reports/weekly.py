"""Weekly health report generator using configurable AI providers."""

import argparse
import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import anthropic
import openai
import structlog
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from openai import OpenAI

from ..config import (
    AnthropicSettings,
    GrokSettings,
    InfluxDBSettings,
    OpenAISettings,
    get_settings,
)
from .delivery import ClawdbotDelivery
from .formatter import TelegramFormatter
from .insights import InsightEngine
from .models import DeliveryResult, PrivacySafeMetrics

logger = structlog.get_logger(__name__)


@dataclass
class WeeklyMetrics:
    """Aggregated weekly health metrics."""

    # Activity
    total_steps: int = 0
    avg_daily_steps: float = 0
    total_active_calories: float = 0
    total_exercise_minutes: float = 0

    # Heart
    avg_resting_hr: float | None = None
    min_resting_hr: float | None = None
    max_resting_hr: float | None = None
    avg_hrv: float | None = None

    # Sleep
    avg_sleep_duration: float | None = None
    avg_deep_sleep: float | None = None
    avg_rem_sleep: float | None = None
    avg_sleep_quality: float | None = None

    # Workouts
    workout_count: int = 0
    total_workout_duration: float = 0
    workout_types: dict[str, int] | None = None

    # Body
    latest_weight: float | None = None
    weight_change: float | None = None

    # Comparison to previous week
    steps_change_pct: float | None = None
    sleep_change_pct: float | None = None
    exercise_change_pct: float | None = None


class WeeklyReportGenerator:
    """Generates weekly health reports with AI-powered insights."""

    def __init__(
        self,
        influxdb_settings: InfluxDBSettings,
        anthropic_settings: AnthropicSettings,
        openai_settings: OpenAISettings,
        grok_settings: GrokSettings,
        ai_provider: str,
        ai_timeout_seconds: float = 30.0,
    ) -> None:
        """Initialize the report generator.

        Args:
            influxdb_settings: InfluxDB connection settings.
            anthropic_settings: Anthropic API settings.
            openai_settings: OpenAI API settings.
            grok_settings: Grok API settings.
            ai_provider: AI provider to use.
        """
        self._influxdb_settings = influxdb_settings
        self._anthropic_settings = anthropic_settings
        self._openai_settings = openai_settings
        self._grok_settings = grok_settings
        self._ai_provider = ai_provider
        self._ai_timeout_seconds = ai_timeout_seconds
        self._influx_client: InfluxDBClientAsync | None = None

    async def connect(self) -> None:
        """Connect to InfluxDB."""
        self._influx_client = InfluxDBClientAsync(
            url=self._influxdb_settings.url,
            token=self._influxdb_settings.token,
            org=self._influxdb_settings.org,
        )

    async def disconnect(self) -> None:
        """Disconnect from InfluxDB."""
        if self._influx_client:
            await self._influx_client.close()

    async def generate_report(self, end_date: datetime | None = None) -> str:
        """Generate a weekly health report.

        Args:
            end_date: End date for the report week. Defaults to now.

        Returns:
            Formatted health report with AI insights.
        """
        if end_date is None:
            end_date = datetime.now(UTC)

        start_date = end_date - timedelta(days=7)
        prev_start = start_date - timedelta(days=7)

        logger.info(
            "generating_weekly_report",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

        # Fetch metrics for current and previous week
        current_metrics = await self._fetch_weekly_metrics(start_date, end_date)
        previous_metrics = await self._fetch_weekly_metrics(prev_start, start_date)

        # Calculate week-over-week changes
        current_metrics = self._calculate_changes(current_metrics, previous_metrics)

        # Generate AI insights
        insights = await self._generate_ai_insights(current_metrics)

        # Format final report
        report = self._format_report(current_metrics, insights, start_date, end_date)

        logger.info("weekly_report_generated")

        return report

    async def _fetch_weekly_metrics(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> WeeklyMetrics:
        """Fetch aggregated metrics for a week.

        Args:
            start_date: Start of the week.
            end_date: End of the week.

        Returns:
            WeeklyMetrics with aggregated data.
        """
        if not self._influx_client:
            raise RuntimeError("Not connected to InfluxDB")

        metrics = WeeklyMetrics()
        query_api = self._influx_client.query_api()

        # Fetch activity metrics
        activity_query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start_date.isoformat()}, stop: {end_date.isoformat()})
            |> filter(fn: (r) => r._measurement == "activity")
            |> filter(fn: (r) => r._field == "steps" or r._field == "active_calories" or r._field == "exercise_min")
            |> group(columns: ["_field"])
            |> sum()
        """
        try:
            tables = await query_api.query(activity_query)
            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "steps":
                        metrics.total_steps = int(value)
                        metrics.avg_daily_steps = value / 7
                    elif field == "active_calories":
                        metrics.total_active_calories = float(value)
                    elif field == "exercise_min":
                        metrics.total_exercise_minutes = float(value)
        except Exception as e:
            logger.warning("activity_query_failed", error=str(e))

        # Fetch heart rate metrics
        heart_query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start_date.isoformat()}, stop: {end_date.isoformat()})
            |> filter(fn: (r) => r._measurement == "heart")
            |> filter(fn: (r) => r._field == "resting_bpm" or r._field == "hrv_ms")
        """
        try:
            tables = await query_api.query(heart_query)
            resting_hrs = []
            hrvs = []
            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "resting_bpm":
                        resting_hrs.append(value)
                    elif field == "hrv_ms":
                        hrvs.append(value)

            if resting_hrs:
                metrics.avg_resting_hr = sum(resting_hrs) / len(resting_hrs)
                metrics.min_resting_hr = min(resting_hrs)
                metrics.max_resting_hr = max(resting_hrs)
            if hrvs:
                metrics.avg_hrv = sum(hrvs) / len(hrvs)
        except Exception as e:
            logger.warning("heart_query_failed", error=str(e))

        # Fetch sleep metrics
        sleep_query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start_date.isoformat()}, stop: {end_date.isoformat()})
            |> filter(fn: (r) => r._measurement == "sleep")
            |> filter(fn: (r) => r._field == "duration_min" or r._field == "deep_min" or r._field == "rem_min" or r._field == "quality_score")
        """
        try:
            tables = await query_api.query(sleep_query)
            durations = []
            deep_sleeps = []
            rem_sleeps = []
            qualities = []
            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "duration_min":
                        durations.append(value)
                    elif field == "deep_min":
                        deep_sleeps.append(value)
                    elif field == "rem_min":
                        rem_sleeps.append(value)
                    elif field == "quality_score":
                        qualities.append(value)

            if durations:
                metrics.avg_sleep_duration = sum(durations) / len(durations)
            if deep_sleeps:
                metrics.avg_deep_sleep = sum(deep_sleeps) / len(deep_sleeps)
            if rem_sleeps:
                metrics.avg_rem_sleep = sum(rem_sleeps) / len(rem_sleeps)
            if qualities:
                metrics.avg_sleep_quality = sum(qualities) / len(qualities)
        except Exception as e:
            logger.warning("sleep_query_failed", error=str(e))

        # Fetch workout metrics
        workout_query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start_date.isoformat()}, stop: {end_date.isoformat()})
            |> filter(fn: (r) => r._measurement == "workout")
            |> filter(fn: (r) => r._field == "duration_min")
        """
        try:
            tables = await query_api.query(workout_query)
            workout_types: dict[str, int] = {}
            total_duration = 0
            count = 0
            for table in tables:
                for record in table.records:
                    count += 1
                    total_duration += record.get_value()
                    workout_type = record.values.get("workout_type", "unknown")
                    workout_types[workout_type] = workout_types.get(workout_type, 0) + 1

            metrics.workout_count = count
            metrics.total_workout_duration = total_duration
            metrics.workout_types = workout_types if workout_types else None
        except Exception as e:
            logger.warning("workout_query_failed", error=str(e))

        # Fetch body metrics (latest weight)
        body_query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start_date.isoformat()}, stop: {end_date.isoformat()})
            |> filter(fn: (r) => r._measurement == "body")
            |> filter(fn: (r) => r._field == "weight_kg")
            |> last()
        """
        try:
            tables = await query_api.query(body_query)
            for table in tables:
                for record in table.records:
                    metrics.latest_weight = record.get_value()
        except Exception as e:
            logger.warning("body_query_failed", error=str(e))

        return metrics

    def _calculate_changes(
        self,
        current: WeeklyMetrics,
        previous: WeeklyMetrics,
    ) -> WeeklyMetrics:
        """Calculate week-over-week changes.

        Args:
            current: Current week's metrics.
            previous: Previous week's metrics.

        Returns:
            Current metrics with change percentages populated.
        """
        if previous.total_steps > 0:
            current.steps_change_pct = (
                (current.total_steps - previous.total_steps) / previous.total_steps * 100
            )

        if previous.avg_sleep_duration and previous.avg_sleep_duration > 0:
            current.sleep_change_pct = (
                (current.avg_sleep_duration - previous.avg_sleep_duration)
                / previous.avg_sleep_duration
                * 100
            )

        if previous.total_exercise_minutes > 0:
            current.exercise_change_pct = (
                (current.total_exercise_minutes - previous.total_exercise_minutes)
                / previous.total_exercise_minutes
                * 100
            )

        if previous.latest_weight and current.latest_weight:
            current.weight_change = current.latest_weight - previous.latest_weight

        return current

    async def _generate_ai_insights(self, metrics: WeeklyMetrics) -> str:
        """Generate AI-powered insights from health metrics.

        Args:
            metrics: Weekly health metrics.

        Returns:
            AI-generated health insights.
        """
        provider = self._ai_provider
        if not self._provider_configured(provider):
            return f"AI insights not available (no {provider} API key configured)."

        # Build metrics summary for the prompt
        metrics_text = self._format_metrics_for_ai(metrics)

        prompt = f"""Analyze the following weekly health metrics and provide personalized insights and recommendations. Be concise, actionable, and encouraging. Focus on:
1. Key achievements and positive trends
2. Areas that might need attention
3. Specific, actionable recommendations for the coming week

Health Metrics Summary:
{metrics_text}

Provide your analysis in 3-4 paragraphs. Be specific and reference the actual numbers."""

        def do_request():
            if provider == "anthropic":
                client = anthropic.Anthropic(
                    api_key=self._anthropic_settings.api_key,
                    timeout=self._ai_timeout_seconds,
                )
                return client.messages.create(
                    model=self._anthropic_settings.model,
                    max_tokens=1024,
                    messages=[{"role": "user", "content": prompt}],
                )

            if provider == "openai":
                client = OpenAI(
                    api_key=self._openai_settings.api_key,
                    base_url=self._openai_settings.base_url,
                    timeout=self._ai_timeout_seconds,
                )
                return client.chat.completions.create(
                    model=self._openai_settings.model,
                    max_tokens=1024,
                    messages=[{"role": "user", "content": prompt}],
                )

            client = OpenAI(
                api_key=self._grok_settings.api_key,
                base_url=self._grok_settings.base_url,
                timeout=self._ai_timeout_seconds,
            )
            return client.chat.completions.create(
                model=self._grok_settings.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )

        try:
            message = await asyncio.wait_for(
                asyncio.to_thread(do_request),
                timeout=self._ai_timeout_seconds,
            )
            if provider == "anthropic":
                return message.content[0].text
            return message.choices[0].message.content or ""
        except TimeoutError:
            logger.error("ai_insight_timeout", timeout=self._ai_timeout_seconds)
            return "AI insights unavailable: request timed out"
        except anthropic.APIConnectionError as e:
            logger.error("ai_insight_connection_failed", error=str(e))
            return f"AI insights unavailable: {e}"
        except anthropic.RateLimitError as e:
            logger.error("ai_insight_rate_limited", error=str(e))
            return "AI insights unavailable: rate limited"
        except anthropic.APIStatusError as e:
            logger.error("ai_insight_status_error", status=e.status_code, error=str(e))
            return f"AI insights unavailable: {e}"
        except openai.APIConnectionError as e:
            logger.error("ai_insight_connection_failed", error=str(e))
            return f"AI insights unavailable: {e}"
        except openai.RateLimitError as e:
            logger.error("ai_insight_rate_limited", error=str(e))
            return "AI insights unavailable: rate limited"
        except openai.APIStatusError as e:
            logger.error("ai_insight_status_error", status=e.status_code, error=str(e))
            return f"AI insights unavailable: {e}"
        except Exception as e:
            logger.error("ai_insight_generation_failed", error=str(e))
            return f"AI insights unavailable: {e}"

    def _provider_configured(self, provider: str) -> bool:
        """Check if the requested provider has credentials configured."""
        if provider == "anthropic":
            return bool(self._anthropic_settings.api_key)
        if provider == "openai":
            return bool(self._openai_settings.api_key)
        if provider == "grok":
            return bool(self._grok_settings.api_key)
        return False

    def _format_metrics_for_ai(self, metrics: WeeklyMetrics) -> str:
        """Format metrics into a text summary for AI analysis.

        Args:
            metrics: Weekly health metrics.

        Returns:
            Formatted text summary.
        """
        lines = []

        # Activity
        lines.append("ACTIVITY:")
        lines.append(f"  - Total steps: {metrics.total_steps:,}")
        lines.append(f"  - Average daily steps: {metrics.avg_daily_steps:,.0f}")
        lines.append(f"  - Total active calories: {metrics.total_active_calories:,.0f}")
        lines.append(f"  - Total exercise minutes: {metrics.total_exercise_minutes:.0f}")
        if metrics.steps_change_pct is not None:
            direction = "up" if metrics.steps_change_pct > 0 else "down"
            lines.append(f"  - Steps vs last week: {direction} {abs(metrics.steps_change_pct):.1f}%")

        # Heart
        lines.append("\nHEART:")
        if metrics.avg_resting_hr:
            lines.append(f"  - Average resting heart rate: {metrics.avg_resting_hr:.0f} bpm")
            lines.append(
                f"  - Resting HR range: {metrics.min_resting_hr:.0f}-{metrics.max_resting_hr:.0f} bpm"
            )
        if metrics.avg_hrv:
            lines.append(f"  - Average HRV: {metrics.avg_hrv:.0f} ms")

        # Sleep
        lines.append("\nSLEEP:")
        if metrics.avg_sleep_duration:
            hours = metrics.avg_sleep_duration / 60
            lines.append(f"  - Average sleep duration: {hours:.1f} hours")
        if metrics.avg_deep_sleep:
            lines.append(f"  - Average deep sleep: {metrics.avg_deep_sleep:.0f} min")
        if metrics.avg_rem_sleep:
            lines.append(f"  - Average REM sleep: {metrics.avg_rem_sleep:.0f} min")
        if metrics.avg_sleep_quality:
            lines.append(f"  - Average sleep quality: {metrics.avg_sleep_quality:.0f}%")
        if metrics.sleep_change_pct is not None:
            direction = "up" if metrics.sleep_change_pct > 0 else "down"
            lines.append(f"  - Sleep vs last week: {direction} {abs(metrics.sleep_change_pct):.1f}%")

        # Workouts
        lines.append("\nWORKOUTS:")
        lines.append(f"  - Total workouts: {metrics.workout_count}")
        lines.append(f"  - Total workout duration: {metrics.total_workout_duration:.0f} min")
        if metrics.workout_types:
            types_str = ", ".join(f"{k}: {v}" for k, v in metrics.workout_types.items())
            lines.append(f"  - Workout types: {types_str}")
        if metrics.exercise_change_pct is not None:
            direction = "up" if metrics.exercise_change_pct > 0 else "down"
            lines.append(
                f"  - Exercise vs last week: {direction} {abs(metrics.exercise_change_pct):.1f}%"
            )

        # Body
        if metrics.latest_weight:
            lines.append("\nBODY:")
            lines.append(f"  - Current weight: {metrics.latest_weight:.1f} kg")
            if metrics.weight_change:
                direction = "gained" if metrics.weight_change > 0 else "lost"
                lines.append(f"  - Weight change: {direction} {abs(metrics.weight_change):.1f} kg")

        return "\n".join(lines)

    def _format_report(
        self,
        metrics: WeeklyMetrics,
        insights: str,
        start_date: datetime,
        end_date: datetime,
    ) -> str:
        """Format the final weekly report.

        Args:
            metrics: Weekly health metrics.
            insights: AI-generated insights.
            start_date: Report start date.
            end_date: Report end date.

        Returns:
            Formatted report string.
        """
        date_range = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"

        report = f"""
Weekly Health Report
{date_range}
{'=' * 50}

{self._format_metrics_for_ai(metrics)}

{'=' * 50}
AI INSIGHTS & RECOMMENDATIONS:
{'=' * 50}

{insights}

{'=' * 50}
Generated on {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}
"""
        return report.strip()


def convert_to_privacy_safe(
    metrics: WeeklyMetrics,
    previous_metrics: WeeklyMetrics | None = None,
) -> PrivacySafeMetrics:
    """Convert WeeklyMetrics to PrivacySafeMetrics.

    Args:
        metrics: Current week's metrics.
        previous_metrics: Previous week's metrics for HRV comparison.

    Returns:
        Privacy-safe aggregated metrics.
    """
    # Calculate HRV change if we have previous data
    hrv_change_pct = None
    if previous_metrics and previous_metrics.avg_hrv and metrics.avg_hrv:
        hrv_change_pct = (
            (metrics.avg_hrv - previous_metrics.avg_hrv) / previous_metrics.avg_hrv * 100
        )

    return PrivacySafeMetrics(
        # Activity
        avg_daily_steps=int(metrics.avg_daily_steps),
        steps_change_pct=metrics.steps_change_pct,
        total_exercise_min=int(metrics.total_exercise_minutes),
        exercise_change_pct=metrics.exercise_change_pct,
        total_active_calories=int(metrics.total_active_calories),
        # Heart
        avg_resting_hr=metrics.avg_resting_hr,
        resting_hr_range=(
            (metrics.min_resting_hr, metrics.max_resting_hr)
            if metrics.min_resting_hr and metrics.max_resting_hr
            else None
        ),
        avg_hrv=metrics.avg_hrv,
        hrv_change_pct=hrv_change_pct,
        # Sleep
        avg_duration_hours=(
            metrics.avg_sleep_duration / 60 if metrics.avg_sleep_duration else None
        ),
        avg_quality_pct=metrics.avg_sleep_quality,
        sleep_change_pct=metrics.sleep_change_pct,
        avg_deep_sleep_min=metrics.avg_deep_sleep,
        avg_rem_sleep_min=metrics.avg_rem_sleep,
        # Workouts
        workout_count=metrics.workout_count,
        total_workout_duration_min=int(metrics.total_workout_duration),
        workout_types=metrics.workout_types or {},
        # Body
        weight_kg=metrics.latest_weight,
        weight_change_kg=metrics.weight_change,
    )


async def generate_and_send_report(
    dry_run: bool = False,
    stdout: bool = False,
) -> DeliveryResult | None:
    """Generate weekly report with insights and optionally send via Telegram.

    Args:
        dry_run: If True, generate but don't send.
        stdout: If True, print report to stdout.

    Returns:
        DeliveryResult if sent, None if dry_run or delivery disabled.
    """
    settings = get_settings()

    generator = WeeklyReportGenerator(
        influxdb_settings=settings.influxdb,
        anthropic_settings=settings.anthropic,
        openai_settings=settings.openai,
        grok_settings=settings.grok,
        ai_provider=settings.insight.ai_provider,
        ai_timeout_seconds=settings.insight.ai_timeout_seconds,
    )

    await generator.connect()

    try:
        # Calculate date range
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=7)
        prev_start = start_date - timedelta(days=7)

        logger.info(
            "generating_smart_report",
            start=start_date.isoformat(),
            end=end_date.isoformat(),
        )

        # Fetch metrics for current and previous week
        current_metrics = await generator._fetch_weekly_metrics(start_date, end_date)
        previous_metrics = await generator._fetch_weekly_metrics(prev_start, start_date)

        # Calculate week-over-week changes
        current_metrics = generator._calculate_changes(current_metrics, previous_metrics)

        # Convert to privacy-safe format
        privacy_safe = convert_to_privacy_safe(current_metrics, previous_metrics)

        # Generate insights using InsightEngine
        insight_engine = InsightEngine(
            anthropic_settings=settings.anthropic,
            openai_settings=settings.openai,
            grok_settings=settings.grok,
            insight_settings=settings.insight,
        )
        insights = await insight_engine.generate(privacy_safe)

        # Format for Telegram
        formatter = TelegramFormatter()
        report = formatter.format(
            metrics=privacy_safe,
            insights=insights,
            week_start=start_date,
            week_end=end_date,
        )

        if stdout:
            print(report)
            print("\n" + "=" * 50)
            print(f"Report length: {len(report)} characters")
            print(f"Insights source: {insights[0].source if insights else 'none'}")

        if dry_run:
            logger.info(
                "dry_run_complete",
                report_length=len(report),
                insight_count=len(insights),
                insight_source=insights[0].source if insights else "none",
            )
            return None

        # Send via Clawdbot
        if settings.clawdbot.enabled and settings.clawdbot.hooks_token:
            delivery = ClawdbotDelivery(settings.clawdbot)
            week_id = start_date.strftime("%Y-W%W")
            result = await delivery.send_report(report, week_id)

            if result.success:
                logger.info(
                    "report_delivered",
                    run_id=result.run_id,
                    attempts=result.attempt,
                )
            else:
                logger.error(
                    "report_delivery_failed",
                    error=result.error,
                    attempts=result.attempt,
                )

            return result
        else:
            logger.warning("clawdbot_not_configured")
            return None

    finally:
        await generator.disconnect()


async def main():
    """CLI entry point for generating a weekly report."""
    settings = get_settings()

    generator = WeeklyReportGenerator(
        influxdb_settings=settings.influxdb,
        anthropic_settings=settings.anthropic,
        openai_settings=settings.openai,
        grok_settings=settings.grok,
        ai_provider=settings.insight.ai_provider,
        ai_timeout_seconds=settings.insight.ai_timeout_seconds,
    )

    await generator.connect()
    try:
        report = await generator.generate_report()
        print(report)
    finally:
        await generator.disconnect()


def run_report() -> None:
    """Synchronous entry point for CLI."""
    asyncio.run(main())


def run_report_and_send() -> None:
    """Generate weekly report and send via Telegram.

    CLI entry point with options:
        --dry-run: Generate but don't send
        --stdout: Print report to stdout
    """
    parser = argparse.ArgumentParser(
        description="Generate and send weekly health report via Telegram"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate report but don't send",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print report to stdout",
    )

    args = parser.parse_args()

    # Setup logging
    from ..logging import setup_logging

    settings = get_settings()
    setup_logging(settings.app)

    result = asyncio.run(generate_and_send_report(
        dry_run=args.dry_run,
        stdout=args.stdout,
    ))

    if result and not result.success:
        raise SystemExit(1)


if __name__ == "__main__":
    run_report()
