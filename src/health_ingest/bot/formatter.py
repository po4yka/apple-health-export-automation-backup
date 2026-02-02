"""Telegram Markdown formatters for bot command responses."""

from .commands import COMMAND_DESCRIPTIONS
from .queries import (
    DaySummaryData,
    HeartData,
    SleepData,
    SnapshotData,
    StepsDailyBreakdown,
    TrendsData,
    WeightData,
    WorkoutEntry,
)

MAX_MESSAGE_LENGTH = 4000


def _truncate(text: str) -> str:
    """Truncate message to Telegram limit."""
    if len(text) <= MAX_MESSAGE_LENGTH:
        return text
    return text[: MAX_MESSAGE_LENGTH - 15] + "\n...truncated"


def _trend_arrow(current: float | None, previous: float | None) -> str:
    """Return trend arrow with percentage change."""
    if current is None or previous is None or previous == 0:
        return ""
    pct = ((current - previous) / abs(previous)) * 100
    if pct > 2:
        return f" ↑{pct:+.0f}%"
    if pct < -2:
        return f" ↓{pct:+.0f}%"
    return " →"


def format_snapshot(data: SnapshotData) -> str:
    """Format /health_now quick snapshot."""
    lines = ["*📊 Quick Snapshot*", ""]
    lines.append(f"🚶 Steps: *{data.steps:,}*")
    lines.append(f"🔥 Calories: *{data.active_calories:,}* kcal")
    lines.append(f"🏃 Exercise: *{data.exercise_min}* min")
    if data.resting_hr is not None:
        lines.append(f"❤️ Resting HR: *{data.resting_hr:.0f}* bpm")
    if data.hrv_ms is not None:
        lines.append(f"💓 HRV: *{data.hrv_ms:.0f}* ms")
    if data.weight_kg is not None:
        lines.append(f"⚖️ Weight: *{data.weight_kg:.1f}* kg")
    return _truncate("\n".join(lines))


def format_heart(data: HeartData) -> str:
    """Format /health_heart response."""
    lines = ["*❤️ Heart Rate & HRV*", ""]

    if data.resting_hr is not None:
        line = f"Resting HR: *{data.resting_hr:.0f}* bpm"
        line += _trend_arrow(data.resting_hr, data.avg_7d_resting_hr)
        lines.append(line)
    if data.hr_min is not None and data.hr_max is not None:
        lines.append(f"Range: {data.hr_min:.0f}–{data.hr_max:.0f} bpm")
    if data.hrv_ms is not None:
        line = f"HRV: *{data.hrv_ms:.0f}* ms"
        line += _trend_arrow(data.hrv_ms, data.avg_7d_hrv_ms)
        lines.append(line)

    lines.append("")
    lines.append("_7-day averages:_")
    if data.avg_7d_resting_hr is not None:
        lines.append(f"  HR: {data.avg_7d_resting_hr:.0f} bpm")
    if data.avg_7d_hrv_ms is not None:
        lines.append(f"  HRV: {data.avg_7d_hrv_ms:.0f} ms")

    return _truncate("\n".join(lines))


def format_sleep(data: SleepData) -> str:
    """Format /health_sleep response."""
    lines = ["*🌙 Last Night's Sleep*", ""]

    if data.duration_min is not None:
        hours = data.duration_min / 60
        lines.append(f"Duration: *{hours:.1f}h* ({data.duration_min:.0f} min)")
    if data.deep_min is not None:
        lines.append(f"Deep: {data.deep_min:.0f} min")
    if data.rem_min is not None:
        lines.append(f"REM: {data.rem_min:.0f} min")
    if data.core_min is not None:
        lines.append(f"Core: {data.core_min:.0f} min")
    if data.awake_min is not None:
        lines.append(f"Awake: {data.awake_min:.0f} min")
    if data.quality_score is not None:
        lines.append(f"Quality: *{data.quality_score:.0f}%*")

    return _truncate("\n".join(lines))


def format_weight(data: WeightData) -> str:
    """Format /health_weight response."""
    lines = ["*⚖️ Weight*", ""]

    if data.latest_kg is not None:
        lines.append(f"Current: *{data.latest_kg:.1f}* kg")
        if data.latest_date:
            lines.append(f"Measured: {data.latest_date}")

    if data.avg_7d is not None:
        lines.append(f"7-day avg: {data.avg_7d:.1f} kg")
    if data.change_7d is not None:
        lines.append(f"7-day change: {data.change_7d:+.1f} kg")

    if data.avg_30d is not None:
        lines.append(f"30-day avg: {data.avg_30d:.1f} kg")
    if data.change_30d is not None:
        lines.append(f"30-day change: {data.change_30d:+.1f} kg")

    return _truncate("\n".join(lines))


def format_day_summary(data: DaySummaryData, label: str) -> str:
    """Format /health_today, /health_yesterday, or /health_week response."""
    lines = [f"*📋 {label}*", ""]
    lines.append(f"🚶 Steps: *{data.steps:,}*")
    lines.append(f"🔥 Calories: *{data.active_calories:,}* kcal")
    lines.append(f"🏃 Exercise: *{data.exercise_min}* min")
    if data.stand_hours:
        lines.append(f"🧍 Stand hours: {data.stand_hours}")
    if data.distance_km:
        lines.append(f"📏 Distance: {data.distance_km:.1f} km")
    if data.resting_hr is not None:
        lines.append(f"❤️ Resting HR: {data.resting_hr:.0f} bpm")
    if data.hrv_ms is not None:
        lines.append(f"💓 HRV: {data.hrv_ms:.0f} ms")

    if data.workout_summaries:
        lines.append("")
        lines.append("*Workouts:*")
        for w in data.workout_summaries:
            lines.append(f"  • {w}")

    return _truncate("\n".join(lines))


def format_steps(data: StepsDailyBreakdown, period: str) -> str:
    """Format /health_steps response."""
    lines = [f"*🚶 Steps ({period})*", ""]
    lines.append(f"Total: *{data.total:,}*")
    lines.append(f"Daily avg: *{data.daily_avg:,}*")

    if data.daily:
        lines.append("")
        for date_str, steps in data.daily:
            bar = "█" * min(steps // 1000, 20)
            lines.append(f"`{date_str}` {steps:>6,} {bar}")

    return _truncate("\n".join(lines))


def format_workouts(entries: list[WorkoutEntry], period: str) -> str:
    """Format /health_workouts response."""
    lines = [f"*🏋️ Workouts ({period})*", ""]

    if not entries:
        lines.append("No workouts recorded.")
        return "\n".join(lines)

    lines.append(f"Total: {len(entries)} workouts")
    lines.append("")

    for entry in entries:
        parts = [f"*{entry.workout_type}*"]
        if entry.date:
            parts.append(f"📅 {entry.date}")
        details = []
        if entry.duration_min:
            details.append(f"{entry.duration_min:.0f}min")
        if entry.calories:
            details.append(f"{entry.calories:.0f}cal")
        if entry.distance_km is not None:
            details.append(f"{entry.distance_km:.1f}km")
        if entry.avg_hr is not None:
            details.append(f"avg HR {entry.avg_hr:.0f}")
        if details:
            parts.append(", ".join(details))
        lines.append("\n".join(parts))
        lines.append("")

    return _truncate("\n".join(lines))


def format_trends(data: TrendsData) -> str:
    """Format /health_trends response."""
    lines = ["*📈 Trends (This Week vs Last)*", ""]

    def _row(
        label: str, this: float | int | None, last: float | int | None, fmt: str = ",.0f"
    ) -> str:
        this_str = f"{this:{fmt}}" if this is not None else "—"
        last_str = f"{last:{fmt}}" if last is not None else "—"
        arrow = _trend_arrow(
            float(this) if this is not None else None, float(last) if last is not None else None
        )
        return f"{label}: {this_str} vs {last_str}{arrow}"

    lines.append(_row("🚶 Steps", data.this_week_steps, data.last_week_steps))
    lines.append(
        _row("🏃 Exercise", data.this_week_exercise, data.last_week_exercise, ",.0f") + " min"
    )
    if data.this_week_sleep_min is not None or data.last_week_sleep_min is not None:
        this_h = data.this_week_sleep_min / 60 if data.this_week_sleep_min else None
        last_h = data.last_week_sleep_min / 60 if data.last_week_sleep_min else None
        lines.append(_row("🌙 Avg sleep", this_h, last_h, ".1f") + " h")
    if data.this_week_resting_hr is not None or data.last_week_resting_hr is not None:
        lines.append(
            _row("❤️ Resting HR", data.this_week_resting_hr, data.last_week_resting_hr, ".0f")
            + " bpm"
        )
    if data.this_week_hrv is not None or data.last_week_hrv is not None:
        lines.append(_row("💓 HRV", data.this_week_hrv, data.last_week_hrv, ".0f") + " ms")
    if data.this_week_weight is not None or data.last_week_weight is not None:
        lines.append(_row("⚖️ Weight", data.this_week_weight, data.last_week_weight, ".1f") + " kg")

    return _truncate("\n".join(lines))


def format_help() -> str:
    """Format /health_help response."""
    lines = ["*🤖 Health Bot Commands*", ""]
    for cmd, desc in COMMAND_DESCRIPTIONS.items():
        lines.append(f"/{cmd.value} — {desc}")
    return "\n".join(lines)


def format_error(message: str) -> str:
    """Format an error message."""
    return f"⚠️ {message}"


def format_no_data(command: str) -> str:
    """Format a friendly no-data message."""
    return f"📭 No {command} data found. Data may not have been recorded yet."


def append_insights(formatted_text: str, insights: list[str]) -> str:
    """Append AI insights section to formatted response."""
    if not insights:
        return formatted_text
    bullets = "\n".join(f"  - {insight}" for insight in insights)
    section = f"\n\n*Insights:*\n{bullets}\n"
    return _truncate(formatted_text + section)
