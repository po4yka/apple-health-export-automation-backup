"""InfluxDB query service for bot commands."""

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone

import structlog
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from ..config import InfluxDBSettings

logger = structlog.get_logger(__name__)

TZ_TBILISI = timezone(timedelta(hours=4))


@dataclass
class SnapshotData:
    """Quick snapshot: steps, calories, exercise, HR, HRV, weight."""

    steps: int = 0
    active_calories: int = 0
    exercise_min: int = 0
    resting_hr: float | None = None
    hrv_ms: float | None = None
    weight_kg: float | None = None


@dataclass
class HeartData:
    """Heart rate and HRV data with 7-day comparison."""

    resting_hr: float | None = None
    hrv_ms: float | None = None
    hr_min: float | None = None
    hr_max: float | None = None
    avg_7d_resting_hr: float | None = None
    avg_7d_hrv_ms: float | None = None


@dataclass
class SleepData:
    """Sleep data for last night."""

    duration_min: float | None = None
    deep_min: float | None = None
    rem_min: float | None = None
    core_min: float | None = None
    awake_min: float | None = None
    quality_score: float | None = None


@dataclass
class WeightData:
    """Weight data with trends."""

    latest_kg: float | None = None
    latest_date: str | None = None
    avg_7d: float | None = None
    avg_30d: float | None = None
    change_7d: float | None = None
    change_30d: float | None = None


@dataclass
class DaySummaryData:
    """Full day summary: activity + heart + workouts."""

    steps: int = 0
    active_calories: int = 0
    exercise_min: int = 0
    stand_hours: int = 0
    distance_km: float = 0
    resting_hr: float | None = None
    hrv_ms: float | None = None
    workout_summaries: list[str] = field(default_factory=list)


@dataclass
class StepsDailyBreakdown:
    """Steps data with daily breakdown."""

    total: int = 0
    daily_avg: int = 0
    daily: list[tuple[str, int]] = field(default_factory=list)


@dataclass
class WorkoutEntry:
    """Single workout entry."""

    workout_type: str = "Unknown"
    date: str = ""
    duration_min: float = 0
    calories: float = 0
    distance_km: float | None = None
    avg_hr: float | None = None
    max_hr: float | None = None


@dataclass
class TrendsData:
    """This week vs last week comparison."""

    this_week_steps: int = 0
    last_week_steps: int = 0
    this_week_exercise: int = 0
    last_week_exercise: int = 0
    this_week_sleep_min: float | None = None
    last_week_sleep_min: float | None = None
    this_week_resting_hr: float | None = None
    last_week_resting_hr: float | None = None
    this_week_hrv: float | None = None
    last_week_hrv: float | None = None
    this_week_weight: float | None = None
    last_week_weight: float | None = None


def _period_days(period: str) -> int:
    """Convert period string to number of days."""
    return int(period.rstrip("d"))


class BotQueryService:
    """Queries InfluxDB for bot command data."""

    def __init__(self, settings: InfluxDBSettings) -> None:
        self._settings = settings

    def _make_client(self) -> InfluxDBClientAsync:
        return InfluxDBClientAsync(
            url=self._settings.url,
            token=self._settings.token,
            org=self._settings.org,
        )

    def _now_tbilisi(self) -> datetime:
        return datetime.now(TZ_TBILISI)

    def _today_midnight_utc(self, ref: datetime | None = None) -> datetime:
        """Get today's midnight in Tbilisi, converted to UTC."""
        if ref is None:
            ref = self._now_tbilisi()
        local_midnight = ref.replace(hour=0, minute=0, second=0, microsecond=0)
        return local_midnight.astimezone(UTC)

    async def fetch_snapshot(self) -> SnapshotData:
        """Fetch quick snapshot data for /now command."""
        data = SnapshotData()
        now_utc = datetime.now(UTC)
        start = self._today_midnight_utc()
        logger.info("bot_snapshot_query", start=start.isoformat(), stop=now_utc.isoformat())

        client = self._make_client()
        try:
            query_api = client.query_api()

            activity_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {start.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "activity")
                |> filter(fn: (r) => r._field == "steps" or r._field == "active_calories" or r._field == "exercise_min")
                |> group(columns: ["_field"])
                |> sum()
            """

            heart_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {start.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "heart")
                |> filter(fn: (r) => r._field == "resting_bpm" or r._field == "hrv_ms")
                |> group(columns: ["_field"])
                |> mean()
            """

            body_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {start.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "body")
                |> filter(fn: (r) => r._field == "weight_kg")
                |> last()
            """

            activity_tables, heart_tables, body_tables = await asyncio.gather(
                query_api.query(activity_query),
                query_api.query(heart_query),
                query_api.query(body_query),
            )

            for table in activity_tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "steps":
                        data.steps = int(value)
                    elif field == "active_calories":
                        data.active_calories = int(value)
                    elif field == "exercise_min":
                        data.exercise_min = int(value)

            for table in heart_tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "resting_bpm":
                        data.resting_hr = float(value)
                    elif field == "hrv_ms":
                        data.hrv_ms = float(value)

            for table in body_tables:
                for record in table.records:
                    data.weight_kg = float(record.get_value())

            logger.info(
                "bot_snapshot_result",
                steps=data.steps,
                resting_hr=data.resting_hr,
                weight_kg=data.weight_kg,
            )
        finally:
            await client.close()

        return data

    async def fetch_heart(self) -> HeartData:
        """Fetch heart data for /heart command."""
        data = HeartData()
        now_utc = datetime.now(UTC)
        start = self._today_midnight_utc()
        seven_days_ago = now_utc - timedelta(days=7)
        logger.info("bot_heart_query", start=start.isoformat(), stop=now_utc.isoformat())

        client = self._make_client()
        try:
            query_api = client.query_api()

            today_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {start.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "heart")
                |> filter(fn: (r) => r._field == "resting_bpm" or r._field == "hrv_ms")
            """

            avg_7d_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {seven_days_ago.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "heart")
                |> filter(fn: (r) => r._field == "resting_bpm" or r._field == "hrv_ms")
                |> group(columns: ["_field"])
                |> mean()
            """

            today_tables, avg_tables = await asyncio.gather(
                query_api.query(today_query),
                query_api.query(avg_7d_query),
            )

            resting_hrs: list[float] = []
            hrvs: list[float] = []
            for table in today_tables:
                for record in table.records:
                    field = record.get_field()
                    value = float(record.get_value())
                    if field == "resting_bpm":
                        resting_hrs.append(value)
                    elif field == "hrv_ms":
                        hrvs.append(value)

            if resting_hrs:
                data.resting_hr = sum(resting_hrs) / len(resting_hrs)
                data.hr_min = min(resting_hrs)
                data.hr_max = max(resting_hrs)
            if hrvs:
                data.hrv_ms = sum(hrvs) / len(hrvs)

            for table in avg_tables:
                for record in table.records:
                    field = record.get_field()
                    value = float(record.get_value())
                    if field == "resting_bpm":
                        data.avg_7d_resting_hr = value
                    elif field == "hrv_ms":
                        data.avg_7d_hrv_ms = value

            logger.info(
                "bot_heart_result",
                resting_hr=data.resting_hr,
                hrv_ms=data.hrv_ms,
            )
        finally:
            await client.close()

        return data

    async def fetch_sleep(self) -> SleepData:
        """Fetch sleep data for /sleep command."""
        data = SleepData()
        ref = self._now_tbilisi()
        today_midnight = ref.replace(hour=0, minute=0, second=0, microsecond=0)
        sleep_start = (today_midnight - timedelta(days=1)).replace(hour=18).astimezone(UTC)
        sleep_stop = today_midnight.replace(hour=12).astimezone(UTC)
        logger.info("bot_sleep_query", start=sleep_start.isoformat(), stop=sleep_stop.isoformat())

        client = self._make_client()
        try:
            query_api = client.query_api()

            query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {sleep_start.isoformat()}, stop: {sleep_stop.isoformat()})
                |> filter(fn: (r) => r._measurement == "sleep")
                |> filter(fn: (r) => r._field == "duration_min" or r._field == "deep_min" or r._field == "rem_min" or r._field == "core_min" or r._field == "awake_min" or r._field == "quality_score")
                |> last()
            """

            tables = await query_api.query(query)
            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = float(record.get_value())
                    if field == "duration_min":
                        data.duration_min = value
                    elif field == "deep_min":
                        data.deep_min = value
                    elif field == "rem_min":
                        data.rem_min = value
                    elif field == "core_min":
                        data.core_min = value
                    elif field == "awake_min":
                        data.awake_min = value
                    elif field == "quality_score":
                        data.quality_score = value

            logger.info("bot_sleep_result", duration_min=data.duration_min)
        finally:
            await client.close()

        return data

    async def fetch_weight(self) -> WeightData:
        """Fetch weight data for /weight command."""
        data = WeightData()
        now_utc = datetime.now(UTC)
        thirty_days_ago = now_utc - timedelta(days=30)
        seven_days_ago = now_utc - timedelta(days=7)
        logger.info("bot_weight_query", start=thirty_days_ago.isoformat(), stop=now_utc.isoformat())

        client = self._make_client()
        try:
            query_api = client.query_api()

            latest_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {thirty_days_ago.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "body")
                |> filter(fn: (r) => r._field == "weight_kg")
                |> last()
            """

            avg_7d_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {seven_days_ago.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "body")
                |> filter(fn: (r) => r._field == "weight_kg")
                |> group(columns: ["_field"])
                |> mean()
            """

            avg_30d_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {thirty_days_ago.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "body")
                |> filter(fn: (r) => r._field == "weight_kg")
                |> group(columns: ["_field"])
                |> mean()
            """

            first_7d_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {seven_days_ago.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "body")
                |> filter(fn: (r) => r._field == "weight_kg")
                |> first()
            """

            first_30d_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {thirty_days_ago.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "body")
                |> filter(fn: (r) => r._field == "weight_kg")
                |> first()
            """

            latest_t, avg_7d_t, avg_30d_t, first_7d_t, first_30d_t = await asyncio.gather(
                query_api.query(latest_query),
                query_api.query(avg_7d_query),
                query_api.query(avg_30d_query),
                query_api.query(first_7d_query),
                query_api.query(first_30d_query),
            )

            first_7d_val = None
            first_30d_val = None

            for table in latest_t:
                for record in table.records:
                    data.latest_kg = float(record.get_value())
                    ts = record.get_time()
                    if ts:
                        data.latest_date = ts.strftime("%Y-%m-%d %H:%M")

            for table in avg_7d_t:
                for record in table.records:
                    data.avg_7d = float(record.get_value())

            for table in avg_30d_t:
                for record in table.records:
                    data.avg_30d = float(record.get_value())

            for table in first_7d_t:
                for record in table.records:
                    first_7d_val = float(record.get_value())

            for table in first_30d_t:
                for record in table.records:
                    first_30d_val = float(record.get_value())

            if data.latest_kg is not None and first_7d_val is not None:
                data.change_7d = data.latest_kg - first_7d_val
            if data.latest_kg is not None and first_30d_val is not None:
                data.change_30d = data.latest_kg - first_30d_val

            logger.info("bot_weight_result", latest_kg=data.latest_kg)
        finally:
            await client.close()

        return data

    async def fetch_day_summary(self, day_offset: int = 0) -> DaySummaryData:
        """Fetch full day summary for /today or /yesterday.

        Args:
            day_offset: 0 for today, -1 for yesterday.
        """
        data = DaySummaryData()
        ref = self._now_tbilisi()
        day_start = (ref + timedelta(days=day_offset)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        day_stop = ref if day_offset == 0 else day_start + timedelta(days=1)

        start_utc = day_start.astimezone(UTC)
        stop_utc = day_stop.astimezone(UTC)
        logger.info(
            "bot_day_summary_query",
            day_offset=day_offset,
            start=start_utc.isoformat(),
            stop=stop_utc.isoformat(),
        )

        client = self._make_client()
        try:
            query_api = client.query_api()

            activity_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {start_utc.isoformat()}, stop: {stop_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "activity")
                |> filter(fn: (r) => r._field == "steps" or r._field == "active_calories" or r._field == "exercise_min" or r._field == "stand_hours" or r._field == "distance_m")
                |> group(columns: ["_field"])
                |> sum()
            """

            heart_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {start_utc.isoformat()}, stop: {stop_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "heart")
                |> filter(fn: (r) => r._field == "resting_bpm" or r._field == "hrv_ms")
                |> group(columns: ["_field"])
                |> mean()
            """

            workout_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {start_utc.isoformat()}, stop: {stop_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "workout")
                |> filter(fn: (r) => r._field == "duration_min" or r._field == "calories" or r._field == "distance_m")
            """

            activity_t, heart_t, workout_t = await asyncio.gather(
                query_api.query(activity_query),
                query_api.query(heart_query),
                query_api.query(workout_query),
            )

            for table in activity_t:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "steps":
                        data.steps = int(value)
                    elif field == "active_calories":
                        data.active_calories = int(value)
                    elif field == "exercise_min":
                        data.exercise_min = int(value)
                    elif field == "stand_hours":
                        data.stand_hours = int(value)
                    elif field == "distance_m":
                        data.distance_km = float(value) / 1000

            for table in heart_t:
                for record in table.records:
                    field = record.get_field()
                    value = float(record.get_value())
                    if field == "resting_bpm":
                        data.resting_hr = value
                    elif field == "hrv_ms":
                        data.hrv_ms = value

            workout_data: dict[str, dict] = {}
            for table in workout_t:
                for record in table.records:
                    time_key = str(record.get_time())
                    if time_key not in workout_data:
                        workout_data[time_key] = {
                            "type": record.values.get("workout_type", "Workout")
                        }
                    workout_data[time_key][record.get_field()] = record.get_value()

            for w in workout_data.values():
                parts = [w.get("type", "Workout")]
                if "duration_min" in w:
                    parts.append(f"{w['duration_min']:.0f}min")
                if "calories" in w:
                    parts.append(f"{w['calories']:.0f}cal")
                if "distance_m" in w and w["distance_m"]:
                    parts.append(f"{w['distance_m'] / 1000:.1f}km")
                summary = (
                    ": ".join([parts[0], ", ".join(parts[1:])]) if len(parts) > 1 else parts[0]
                )
                data.workout_summaries.append(summary)

            logger.info(
                "bot_day_summary_result",
                steps=data.steps,
                workouts=len(data.workout_summaries),
            )
        finally:
            await client.close()

        return data

    async def fetch_week_summary(self) -> DaySummaryData:
        """Fetch aggregated data for the last 7 days for /week command."""
        data = DaySummaryData()
        now_utc = datetime.now(UTC)
        seven_days_ago = self._today_midnight_utc() - timedelta(days=6)
        logger.info(
            "bot_week_summary_query",
            start=seven_days_ago.isoformat(),
            stop=now_utc.isoformat(),
        )

        client = self._make_client()
        try:
            query_api = client.query_api()

            activity_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {seven_days_ago.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "activity")
                |> filter(fn: (r) => r._field == "steps" or r._field == "active_calories" or r._field == "exercise_min" or r._field == "stand_hours" or r._field == "distance_m")
                |> group(columns: ["_field"])
                |> sum()
            """

            heart_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {seven_days_ago.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "heart")
                |> filter(fn: (r) => r._field == "resting_bpm" or r._field == "hrv_ms")
                |> group(columns: ["_field"])
                |> mean()
            """

            workout_query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {seven_days_ago.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "workout")
                |> filter(fn: (r) => r._field == "duration_min" or r._field == "calories" or r._field == "distance_m")
            """

            activity_t, heart_t, workout_t = await asyncio.gather(
                query_api.query(activity_query),
                query_api.query(heart_query),
                query_api.query(workout_query),
            )

            for table in activity_t:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    if field == "steps":
                        data.steps = int(value)
                    elif field == "active_calories":
                        data.active_calories = int(value)
                    elif field == "exercise_min":
                        data.exercise_min = int(value)
                    elif field == "stand_hours":
                        data.stand_hours = int(value)
                    elif field == "distance_m":
                        data.distance_km = float(value) / 1000

            for table in heart_t:
                for record in table.records:
                    field = record.get_field()
                    value = float(record.get_value())
                    if field == "resting_bpm":
                        data.resting_hr = value
                    elif field == "hrv_ms":
                        data.hrv_ms = value

            workout_data: dict[str, dict] = {}
            for table in workout_t:
                for record in table.records:
                    time_key = str(record.get_time())
                    if time_key not in workout_data:
                        workout_data[time_key] = {
                            "type": record.values.get("workout_type", "Workout")
                        }
                    workout_data[time_key][record.get_field()] = record.get_value()

            for w in workout_data.values():
                parts = [w.get("type", "Workout")]
                if "duration_min" in w:
                    parts.append(f"{w['duration_min']:.0f}min")
                if "calories" in w:
                    parts.append(f"{w['calories']:.0f}cal")
                summary = (
                    ": ".join([parts[0], ", ".join(parts[1:])]) if len(parts) > 1 else parts[0]
                )
                data.workout_summaries.append(summary)

            logger.info(
                "bot_week_summary_result",
                steps=data.steps,
                workouts=len(data.workout_summaries),
            )
        finally:
            await client.close()

        return data

    async def fetch_steps(self, period: str) -> StepsDailyBreakdown:
        """Fetch steps with daily breakdown for /steps command."""
        data = StepsDailyBreakdown()
        days = _period_days(period)
        now_utc = datetime.now(UTC)
        start = self._today_midnight_utc() - timedelta(days=days - 1)
        logger.info("bot_steps_query", period=period, start=start.isoformat())

        client = self._make_client()
        try:
            query_api = client.query_api()

            query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {start.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "activity")
                |> filter(fn: (r) => r._field == "steps")
                |> group(columns: ["_field"])
                |> aggregateWindow(every: 1d, fn: sum, createEmpty: true)
            """

            tables = await query_api.query(query)
            total = 0
            daily: list[tuple[str, int]] = []
            for table in tables:
                for record in table.records:
                    value = record.get_value()
                    steps = int(value) if value else 0
                    ts = record.get_time()
                    date_str = ts.strftime("%Y-%m-%d") if ts else "unknown"
                    daily.append((date_str, steps))
                    total += steps

            data.total = total
            data.daily = daily
            data.daily_avg = total // max(len(daily), 1)

            logger.info("bot_steps_result", total=data.total, days=len(daily))
        finally:
            await client.close()

        return data

    async def fetch_workouts(self, period: str) -> list[WorkoutEntry]:
        """Fetch workout list for /workouts command."""
        entries: list[WorkoutEntry] = []
        days = _period_days(period)
        now_utc = datetime.now(UTC)
        start = now_utc - timedelta(days=days)
        logger.info("bot_workouts_query", period=period, start=start.isoformat())

        client = self._make_client()
        try:
            query_api = client.query_api()

            query = f"""
            from(bucket: "{self._settings.bucket}")
                |> range(start: {start.isoformat()}, stop: {now_utc.isoformat()})
                |> filter(fn: (r) => r._measurement == "workout")
                |> filter(fn: (r) => r._field == "duration_min" or r._field == "calories" or r._field == "distance_m" or r._field == "avg_hr" or r._field == "max_hr")
            """

            tables = await query_api.query(query)
            workout_data: dict[str, dict] = {}
            for table in tables:
                for record in table.records:
                    time_key = str(record.get_time())
                    if time_key not in workout_data:
                        workout_data[time_key] = {
                            "type": record.values.get("workout_type", "Unknown"),
                            "time": record.get_time(),
                        }
                    workout_data[time_key][record.get_field()] = record.get_value()

            for w in workout_data.values():
                ts = w.get("time")
                entry = WorkoutEntry(
                    workout_type=w.get("type", "Unknown"),
                    date=ts.strftime("%Y-%m-%d %H:%M") if ts else "",
                    duration_min=float(w.get("duration_min", 0)),
                    calories=float(w.get("calories", 0)),
                )
                if "distance_m" in w and w["distance_m"]:
                    entry.distance_km = float(w["distance_m"]) / 1000
                if "avg_hr" in w:
                    entry.avg_hr = float(w["avg_hr"])
                if "max_hr" in w:
                    entry.max_hr = float(w["max_hr"])
                entries.append(entry)

            logger.info("bot_workouts_result", count=len(entries))
        finally:
            await client.close()

        return entries

    async def fetch_trends(self) -> TrendsData:
        """Fetch this week vs last week comparison for /trends command."""
        data = TrendsData()
        now_utc = datetime.now(UTC)
        one_week_ago = self._today_midnight_utc() - timedelta(days=6)
        two_weeks_ago = one_week_ago - timedelta(days=7)
        logger.info("bot_trends_query", start=two_weeks_ago.isoformat(), stop=now_utc.isoformat())

        client = self._make_client()
        try:
            query_api = client.query_api()

            async def _query_activity_sum(start: datetime, stop: datetime) -> dict[str, int]:
                q = f"""
                from(bucket: "{self._settings.bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => r._measurement == "activity")
                    |> filter(fn: (r) => r._field == "steps" or r._field == "exercise_min")
                    |> group(columns: ["_field"])
                    |> sum()
                """
                result: dict[str, int] = {}
                tables = await query_api.query(q)
                for table in tables:
                    for record in table.records:
                        result[record.get_field()] = int(record.get_value())
                return result

            async def _query_heart_mean(start: datetime, stop: datetime) -> dict[str, float]:
                q = f"""
                from(bucket: "{self._settings.bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => r._measurement == "heart")
                    |> filter(fn: (r) => r._field == "resting_bpm" or r._field == "hrv_ms")
                    |> group(columns: ["_field"])
                    |> mean()
                """
                result: dict[str, float] = {}
                tables = await query_api.query(q)
                for table in tables:
                    for record in table.records:
                        result[record.get_field()] = float(record.get_value())
                return result

            async def _query_sleep_mean(start: datetime, stop: datetime) -> float | None:
                q = f"""
                from(bucket: "{self._settings.bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => r._measurement == "sleep")
                    |> filter(fn: (r) => r._field == "duration_min")
                    |> group(columns: ["_field"])
                    |> mean()
                """
                tables = await query_api.query(q)
                for table in tables:
                    for record in table.records:
                        return float(record.get_value())
                return None

            async def _query_weight_mean(start: datetime, stop: datetime) -> float | None:
                q = f"""
                from(bucket: "{self._settings.bucket}")
                    |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                    |> filter(fn: (r) => r._measurement == "body")
                    |> filter(fn: (r) => r._field == "weight_kg")
                    |> group(columns: ["_field"])
                    |> mean()
                """
                tables = await query_api.query(q)
                for table in tables:
                    for record in table.records:
                        return float(record.get_value())
                return None

            (
                this_activity,
                last_activity,
                this_heart,
                last_heart,
                this_sleep,
                last_sleep,
                this_weight,
                last_weight,
            ) = await asyncio.gather(
                _query_activity_sum(one_week_ago, now_utc),
                _query_activity_sum(two_weeks_ago, one_week_ago),
                _query_heart_mean(one_week_ago, now_utc),
                _query_heart_mean(two_weeks_ago, one_week_ago),
                _query_sleep_mean(one_week_ago, now_utc),
                _query_sleep_mean(two_weeks_ago, one_week_ago),
                _query_weight_mean(one_week_ago, now_utc),
                _query_weight_mean(two_weeks_ago, one_week_ago),
            )

            data.this_week_steps = this_activity.get("steps", 0)
            data.last_week_steps = last_activity.get("steps", 0)
            data.this_week_exercise = this_activity.get("exercise_min", 0)
            data.last_week_exercise = last_activity.get("exercise_min", 0)
            data.this_week_resting_hr = this_heart.get("resting_bpm")
            data.last_week_resting_hr = last_heart.get("resting_bpm")
            data.this_week_hrv = this_heart.get("hrv_ms")
            data.last_week_hrv = last_heart.get("hrv_ms")
            data.this_week_sleep_min = this_sleep
            data.last_week_sleep_min = last_sleep
            data.this_week_weight = this_weight
            data.last_week_weight = last_weight

            logger.info(
                "bot_trends_result",
                this_week_steps=data.this_week_steps,
                last_week_steps=data.last_week_steps,
            )
        finally:
            await client.close()

        return data
