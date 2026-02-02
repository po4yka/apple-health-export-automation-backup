"""Daily health summary generator — morning and evening Telegram summaries."""

import argparse
import asyncio
from datetime import UTC, datetime, timedelta, timezone

import structlog
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from ..config import (
    AnthropicSettings,
    GrokSettings,
    InfluxDBSettings,
    OpenAISettings,
    get_settings,
)
from .delivery import OpenClawDelivery
from .formatter import DailyTelegramFormatter
from .insights import DAILY_EVENING_PROMPT, DAILY_MORNING_PROMPT, InsightEngine
from .models import (
    DailyMetrics,
    DeliveryResult,
    PrivacySafeDailyMetrics,
    SummaryMode,
)

logger = structlog.get_logger(__name__)

TZ_TBILISI = timezone(timedelta(hours=4))


class DailyReportGenerator:
    """Generates daily health summaries with AI-powered insights."""

    def __init__(
        self,
        influxdb_settings: InfluxDBSettings,
        anthropic_settings: AnthropicSettings,
        openai_settings: OpenAISettings,
        grok_settings: GrokSettings,
        ai_provider: str,
        ai_timeout_seconds: float = 30.0,
    ) -> None:
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

    async def generate_summary(
        self,
        mode: SummaryMode,
        reference_time: datetime | None = None,
    ) -> str:
        """Generate a daily health summary.

        Args:
            mode: Morning or evening summary.
            reference_time: Reference time. Defaults to now in Tbilisi timezone.

        Returns:
            Formatted summary string.
        """
        if reference_time is None:
            reference_time = datetime.now(TZ_TBILISI)

        logger.info(
            "generating_daily_summary",
            mode=mode.value,
            reference_time=reference_time.isoformat(),
        )

        # Fetch raw metrics
        raw_metrics = await self._fetch_daily_metrics(mode, reference_time)

        # Convert to privacy-safe format
        privacy_safe = self._to_privacy_safe(raw_metrics, mode)

        # Generate insights
        insight_settings = get_settings().insight
        engine = InsightEngine(
            anthropic_settings=self._anthropic_settings,
            openai_settings=self._openai_settings,
            grok_settings=self._grok_settings,
            insight_settings=insight_settings,
        )

        prompt = DAILY_MORNING_PROMPT if mode == SummaryMode.MORNING else DAILY_EVENING_PROMPT
        insights = await engine.generate(
            privacy_safe,
            prompt_template=prompt,
            max_insights_override=3,
        )

        # Format for Telegram
        formatter = DailyTelegramFormatter()
        report = formatter.format(privacy_safe, insights, reference_time)

        logger.info(
            "daily_summary_generated",
            mode=mode.value,
            report_length=len(report),
            insight_count=len(insights),
        )

        return report

    async def _fetch_daily_metrics(
        self,
        mode: SummaryMode,
        reference_time: datetime,
    ) -> DailyMetrics:
        """Fetch daily metrics from InfluxDB based on mode and reference time."""
        if not self._influx_client:
            raise RuntimeError("Not connected to InfluxDB")

        metrics = DailyMetrics()

        # Calculate time ranges based on mode
        ref_utc = reference_time.astimezone(UTC)
        today_start = ref_utc.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
            hours=4
        )  # midnight Tbilisi in UTC
        yesterday_start = today_start - timedelta(days=1)

        if mode == SummaryMode.MORNING:
            sleep_start = yesterday_start + timedelta(hours=18)  # yesterday 18:00 Tbilisi
            sleep_stop = today_start + timedelta(hours=12)  # today 12:00 Tbilisi
            activity_start = yesterday_start
            activity_stop = today_start
            heart_start = today_start
            heart_stop = ref_utc
            body_start = today_start
            body_stop = ref_utc
        else:
            activity_start = today_start
            activity_stop = ref_utc
            heart_start = today_start
            heart_stop = ref_utc
            sleep_start = None
            sleep_stop = None
            body_start = None
            body_stop = None

        # Fetch all metric categories
        if sleep_start and sleep_stop:
            metrics = await self._fetch_sleep(metrics, sleep_start, sleep_stop)

        metrics = await self._fetch_activity(metrics, activity_start, activity_stop)
        metrics = await self._fetch_heart(metrics, heart_start, heart_stop)
        metrics = await self._fetch_workouts(
            metrics, activity_start, activity_stop if mode == SummaryMode.EVENING else activity_stop
        )

        if body_start and body_stop:
            metrics = await self._fetch_body(metrics, body_start, body_stop)

        metrics = await self._fetch_vitals(metrics, heart_start, heart_stop)
        metrics = await self._fetch_7day_averages(metrics, ref_utc)

        return metrics

    async def _fetch_sleep(
        self, metrics: DailyMetrics, start: datetime, stop: datetime
    ) -> DailyMetrics:
        """Fetch sleep metrics."""
        query_api = self._influx_client.query_api()
        query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
            |> filter(fn: (r) => r._measurement == "sleep")
            |> filter(fn: (r) => r._field == "duration_min" or r._field == "deep_min" or r._field == "rem_min" or r._field == "core_min" or r._field == "awake_min" or r._field == "quality_score")
            |> max()
        """
        try:
            tables = await query_api.query(query)
            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "duration_min":
                        metrics.sleep_duration_min = float(value)
                    elif field == "deep_min":
                        metrics.sleep_deep_min = float(value)
                    elif field == "rem_min":
                        metrics.sleep_rem_min = float(value)
                    elif field == "core_min":
                        metrics.sleep_core_min = float(value)
                    elif field == "awake_min":
                        metrics.sleep_awake_min = float(value)
                    elif field == "quality_score":
                        metrics.sleep_quality_score = float(value)
        except Exception as e:
            logger.warning("daily_sleep_query_failed", error=str(e))
        return metrics

    async def _fetch_activity(
        self, metrics: DailyMetrics, start: datetime, stop: datetime
    ) -> DailyMetrics:
        """Fetch activity metrics."""
        query_api = self._influx_client.query_api()
        query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
            |> filter(fn: (r) => r._measurement == "activity")
            |> filter(fn: (r) => r._field == "steps" or r._field == "active_calories" or r._field == "exercise_min" or r._field == "stand_hours" or r._field == "distance_m" or r._field == "flights_climbed")
            |> group(columns: ["_field"])
            |> sum()
        """
        try:
            tables = await query_api.query(query)
            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "steps":
                        metrics.steps = int(value)
                    elif field == "active_calories":
                        metrics.active_calories = int(value)
                    elif field == "exercise_min":
                        metrics.exercise_min = int(value)
                    elif field == "stand_hours":
                        metrics.stand_hours = int(value)
                    elif field == "distance_m":
                        metrics.distance_m = float(value)
                    elif field == "flights_climbed":
                        metrics.floors_climbed = int(value)
        except Exception as e:
            logger.warning("daily_activity_query_failed", error=str(e))
        return metrics

    async def _fetch_heart(
        self, metrics: DailyMetrics, start: datetime, stop: datetime
    ) -> DailyMetrics:
        """Fetch heart rate metrics."""
        query_api = self._influx_client.query_api()
        query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
            |> filter(fn: (r) => r._measurement == "heart")
            |> filter(fn: (r) => r._field == "resting_bpm" or r._field == "hrv_ms")
        """
        try:
            tables = await query_api.query(query)
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
                metrics.resting_hr = sum(resting_hrs) / len(resting_hrs)
            if hrvs:
                metrics.hrv_ms = sum(hrvs) / len(hrvs)
        except Exception as e:
            logger.warning("daily_heart_query_failed", error=str(e))
        return metrics

    async def _fetch_workouts(
        self, metrics: DailyMetrics, start: datetime, stop: datetime
    ) -> DailyMetrics:
        """Fetch workout metrics."""
        query_api = self._influx_client.query_api()
        query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
            |> filter(fn: (r) => r._measurement == "workout")
            |> filter(fn: (r) => r._field == "duration_min" or r._field == "calories" or r._field == "distance_m" or r._field == "avg_hr" or r._field == "max_hr")
        """
        try:
            tables = await query_api.query(query)
            # Group records by time to reconstruct individual workouts
            workout_data: dict[str, dict] = {}
            for table in tables:
                for record in table.records:
                    time_key = str(record.get_time())
                    if time_key not in workout_data:
                        workout_data[time_key] = {
                            "type": record.values.get("workout_type", "unknown"),
                        }
                    field = record.get_field()
                    value = record.get_value()
                    workout_data[time_key][field] = value

            metrics.workouts = list(workout_data.values())
        except Exception as e:
            logger.warning("daily_workout_query_failed", error=str(e))
        return metrics

    async def _fetch_body(
        self, metrics: DailyMetrics, start: datetime, stop: datetime
    ) -> DailyMetrics:
        """Fetch body metrics (weight)."""
        query_api = self._influx_client.query_api()
        query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
            |> filter(fn: (r) => r._measurement == "body")
            |> filter(fn: (r) => r._field == "weight_kg")
            |> last()
        """
        try:
            tables = await query_api.query(query)
            for table in tables:
                for record in table.records:
                    metrics.weight_kg = float(record.get_value())
        except Exception as e:
            logger.warning("daily_body_query_failed", error=str(e))
        return metrics

    async def _fetch_vitals(
        self, metrics: DailyMetrics, start: datetime, stop: datetime
    ) -> DailyMetrics:
        """Fetch additional vitals (SpO2, respiratory rate)."""
        query_api = self._influx_client.query_api()
        query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
            |> filter(fn: (r) => r._measurement == "vitals")
            |> filter(fn: (r) => r._field == "spo2_pct" or r._field == "respiratory_rate")
            |> last()
        """
        try:
            tables = await query_api.query(query)
            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "spo2_pct":
                        metrics.spo2_pct = float(value)
                    elif field == "respiratory_rate":
                        metrics.respiratory_rate = float(value)
        except Exception as e:
            logger.warning("daily_vitals_query_failed", error=str(e))
        return metrics

    async def _fetch_7day_averages(self, metrics: DailyMetrics, ref_utc: datetime) -> DailyMetrics:
        """Fetch 7-day averages for comparison."""
        query_api = self._influx_client.query_api()
        seven_days_ago = ref_utc - timedelta(days=7)

        # Steps average
        steps_query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {seven_days_ago.isoformat()}, stop: {ref_utc.isoformat()})
            |> filter(fn: (r) => r._measurement == "activity")
            |> filter(fn: (r) => r._field == "steps")
            |> group(columns: ["_field"])
            |> sum()
        """
        try:
            tables = await query_api.query(steps_query)
            for table in tables:
                for record in table.records:
                    metrics.avg_7d_steps = float(record.get_value()) / 7
        except Exception as e:
            logger.warning("daily_7d_steps_query_failed", error=str(e))

        # Exercise average
        exercise_query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {seven_days_ago.isoformat()}, stop: {ref_utc.isoformat()})
            |> filter(fn: (r) => r._measurement == "activity")
            |> filter(fn: (r) => r._field == "exercise_min")
            |> group(columns: ["_field"])
            |> sum()
        """
        try:
            tables = await query_api.query(exercise_query)
            for table in tables:
                for record in table.records:
                    metrics.avg_7d_exercise_min = float(record.get_value()) / 7
        except Exception as e:
            logger.warning("daily_7d_exercise_query_failed", error=str(e))

        # Heart averages
        heart_query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {seven_days_ago.isoformat()}, stop: {ref_utc.isoformat()})
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
                metrics.avg_7d_resting_hr = sum(resting_hrs) / len(resting_hrs)
            if hrvs:
                metrics.avg_7d_hrv_ms = sum(hrvs) / len(hrvs)
        except Exception as e:
            logger.warning("daily_7d_heart_query_failed", error=str(e))

        # Sleep average
        sleep_query = f"""
        from(bucket: "{self._influxdb_settings.bucket}")
            |> range(start: {seven_days_ago.isoformat()}, stop: {ref_utc.isoformat()})
            |> filter(fn: (r) => r._measurement == "sleep")
            |> filter(fn: (r) => r._field == "duration_min")
        """
        try:
            tables = await query_api.query(sleep_query)
            durations = []
            for table in tables:
                for record in table.records:
                    durations.append(record.get_value())
            if durations:
                metrics.avg_7d_sleep_duration_min = sum(durations) / len(durations)
        except Exception as e:
            logger.warning("daily_7d_sleep_query_failed", error=str(e))

        return metrics

    def _to_privacy_safe(
        self,
        metrics: DailyMetrics,
        mode: SummaryMode,
    ) -> PrivacySafeDailyMetrics:
        """Convert raw daily metrics to privacy-safe format."""
        # Calculate comparison percentages
        steps_vs_7d = None
        if metrics.avg_7d_steps and metrics.avg_7d_steps > 0:
            steps_vs_7d = ((metrics.steps - metrics.avg_7d_steps) / metrics.avg_7d_steps) * 100

        exercise_vs_7d = None
        if metrics.avg_7d_exercise_min and metrics.avg_7d_exercise_min > 0:
            exercise_vs_7d = (
                (metrics.exercise_min - metrics.avg_7d_exercise_min) / metrics.avg_7d_exercise_min
            ) * 100

        hrv_vs_7d = None
        if metrics.hrv_ms and metrics.avg_7d_hrv_ms and metrics.avg_7d_hrv_ms > 0:
            hrv_vs_7d = ((metrics.hrv_ms - metrics.avg_7d_hrv_ms) / metrics.avg_7d_hrv_ms) * 100

        # Build workout summaries
        workout_summaries = []
        for w in metrics.workouts:
            parts = [w.get("type", "Workout")]
            if "duration_min" in w:
                parts.append(f"{w['duration_min']:.0f}min")
            if "calories" in w:
                parts.append(f"{w['calories']:.0f}cal")
            if "distance_m" in w and w["distance_m"]:
                km = w["distance_m"] / 1000
                parts.append(f"{km:.1f}km")
            workout_summaries.append(
                ": ".join([parts[0], ", ".join(parts[1:])]) if len(parts) > 1 else parts[0]
            )

        return PrivacySafeDailyMetrics(
            mode=mode,
            sleep_duration_min=metrics.sleep_duration_min,
            sleep_deep_min=metrics.sleep_deep_min,
            sleep_rem_min=metrics.sleep_rem_min,
            sleep_core_min=metrics.sleep_core_min,
            sleep_awake_min=metrics.sleep_awake_min,
            sleep_quality_score=metrics.sleep_quality_score,
            resting_hr=metrics.resting_hr,
            hrv_ms=metrics.hrv_ms,
            steps=metrics.steps,
            active_calories=metrics.active_calories,
            exercise_min=metrics.exercise_min,
            stand_hours=metrics.stand_hours,
            workout_summaries=workout_summaries,
            weight_kg=metrics.weight_kg,
            steps_vs_7d_avg=steps_vs_7d,
            exercise_vs_7d_avg=exercise_vs_7d,
            hrv_vs_7d_avg=hrv_vs_7d,
        )


async def generate_and_send_daily(
    mode: SummaryMode,
    dry_run: bool = False,
    stdout: bool = False,
) -> DeliveryResult | None:
    """Full pipeline: generate -> format -> deliver.

    Args:
        mode: Morning or evening summary mode.
        dry_run: If True, generate but don't send.
        stdout: If True, print report to stdout.

    Returns:
        DeliveryResult if sent, None if dry_run or delivery disabled.
    """
    settings = get_settings()

    generator = DailyReportGenerator(
        influxdb_settings=settings.influxdb,
        anthropic_settings=settings.anthropic,
        openai_settings=settings.openai,
        grok_settings=settings.grok,
        ai_provider=settings.insight.ai_provider,
        ai_timeout_seconds=settings.insight.ai_timeout_seconds,
    )

    await generator.connect()

    try:
        reference_time = datetime.now(TZ_TBILISI)
        report = await generator.generate_summary(mode, reference_time)

        if stdout:
            print(report)
            print("\n" + "=" * 50)
            print(f"Report length: {len(report)} characters")

        if dry_run:
            logger.info("daily_dry_run_complete", mode=mode.value, report_length=len(report))
            return None

        # Send via OpenClaw
        if settings.openclaw.enabled and settings.openclaw.hooks_token:
            delivery = OpenClawDelivery(settings.openclaw)
            date_str = reference_time.strftime("%Y-%m-%d")
            session_key = f"health-daily-{mode.value}:{date_str}"
            delivery_name = (
                "Morning Health Summary" if mode == SummaryMode.MORNING else "Evening Health Recap"
            )

            # Override delivery payload name and session key
            payload = {
                "message": report,
                "channel": "telegram",
                "to": str(settings.openclaw.telegram_user_id),
                "deliver": True,
                "name": delivery_name,
                "sessionKey": session_key,
            }
            result = await delivery._send_with_retries(payload)

            if result.success:
                logger.info("daily_report_delivered", mode=mode.value, run_id=result.run_id)
            else:
                logger.error("daily_report_delivery_failed", mode=mode.value, error=result.error)

            return result
        else:
            logger.warning("openclaw_not_configured")
            return None

    finally:
        await generator.disconnect()


def run_daily_report() -> None:
    """CLI: health-daily -- print daily summary to stdout."""
    parser = argparse.ArgumentParser(description="Generate daily health summary")
    parser.add_argument(
        "mode",
        choices=["morning", "evening"],
        help="Summary mode: morning or evening",
    )

    args = parser.parse_args()
    mode = SummaryMode(args.mode)

    settings = get_settings()
    generator = DailyReportGenerator(
        influxdb_settings=settings.influxdb,
        anthropic_settings=settings.anthropic,
        openai_settings=settings.openai,
        grok_settings=settings.grok,
        ai_provider=settings.insight.ai_provider,
        ai_timeout_seconds=settings.insight.ai_timeout_seconds,
    )

    async def _run():
        await generator.connect()
        try:
            report = await generator.generate_summary(mode)
            print(report)
        finally:
            await generator.disconnect()

    asyncio.run(_run())


def run_daily_report_and_send() -> None:
    """CLI: health-daily-send -- generate and send daily summary via Telegram."""
    parser = argparse.ArgumentParser(
        description="Generate and send daily health summary via Telegram"
    )
    parser.add_argument(
        "mode",
        choices=["morning", "evening"],
        help="Summary mode: morning or evening",
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
    mode = SummaryMode(args.mode)

    from ..logging import setup_logging

    settings = get_settings()
    setup_logging(settings.app)

    result = asyncio.run(
        generate_and_send_daily(
            mode=mode,
            dry_run=args.dry_run,
            stdout=args.stdout,
        )
    )

    if result and not result.success:
        raise SystemExit(1)
