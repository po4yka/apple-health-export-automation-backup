"""Data models for health insights and privacy-safe metrics."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal


class SummaryMode(str, Enum):
    """Daily summary mode."""

    MORNING = "morning"
    EVENING = "evening"


@dataclass
class PrivacySafeMetrics:
    """Aggregated metrics safe for AI consumption.

    Contains only pre-aggregated values with no timestamps,
    individual readings, or personally identifiable information.
    """

    # Activity (aggregates only)
    avg_daily_steps: int = 0
    steps_change_pct: float | None = None
    total_exercise_min: int = 0
    exercise_change_pct: float | None = None
    total_active_calories: int = 0

    # Heart (averages only)
    avg_resting_hr: float | None = None
    resting_hr_range: tuple[float, float] | None = None
    avg_hrv: float | None = None
    hrv_change_pct: float | None = None

    # Sleep (averages only)
    avg_duration_hours: float | None = None
    avg_quality_pct: float | None = None
    sleep_change_pct: float | None = None
    avg_deep_sleep_min: float | None = None
    avg_rem_sleep_min: float | None = None

    # Workouts (counts only)
    workout_count: int = 0
    total_workout_duration_min: int = 0
    workout_types: dict[str, int] = field(default_factory=dict)

    # Body (current + delta)
    weight_kg: float | None = None
    weight_change_kg: float | None = None

    @property
    def hrv_trend(self) -> Literal["improving", "stable", "declining"] | None:
        """Compute HRV trend from change percentage."""
        if self.hrv_change_pct is None:
            return None
        if self.hrv_change_pct > 5:
            return "improving"
        if self.hrv_change_pct < -5:
            return "declining"
        return "stable"

    def to_summary_text(self) -> str:
        """Format metrics as text for AI prompt."""
        lines = []

        lines.append("ACTIVITY:")
        lines.append(f"  Average daily steps: {self.avg_daily_steps:,}")
        if self.steps_change_pct is not None:
            lines.append(f"  Steps change from last week: {self.steps_change_pct:+.1f}%")
        lines.append(f"  Total exercise: {self.total_exercise_min} minutes")
        if self.exercise_change_pct is not None:
            lines.append(f"  Exercise change from last week: {self.exercise_change_pct:+.1f}%")
        lines.append(f"  Total active calories: {self.total_active_calories:,}")

        lines.append("\nHEART:")
        if self.avg_resting_hr:
            lines.append(f"  Average resting heart rate: {self.avg_resting_hr:.0f} bpm")
        if self.resting_hr_range:
            low, high = self.resting_hr_range
            lines.append(f"  Resting HR range: {low:.0f}-{high:.0f} bpm")
        if self.avg_hrv:
            lines.append(f"  Average HRV: {self.avg_hrv:.0f} ms")
        if self.hrv_change_pct is not None:
            lines.append(f"  HRV change from last week: {self.hrv_change_pct:+.1f}%")
        if self.hrv_trend:
            lines.append(f"  HRV trend: {self.hrv_trend}")

        lines.append("\nSLEEP:")
        if self.avg_duration_hours:
            lines.append(f"  Average duration: {self.avg_duration_hours:.1f} hours")
        if self.sleep_change_pct is not None:
            lines.append(f"  Sleep change from last week: {self.sleep_change_pct:+.1f}%")
        if self.avg_quality_pct:
            lines.append(f"  Average quality score: {self.avg_quality_pct:.0f}%")
        if self.avg_deep_sleep_min:
            lines.append(f"  Average deep sleep: {self.avg_deep_sleep_min:.0f} min")
        if self.avg_rem_sleep_min:
            lines.append(f"  Average REM sleep: {self.avg_rem_sleep_min:.0f} min")

        lines.append("\nWORKOUTS:")
        lines.append(f"  Total workouts: {self.workout_count}")
        lines.append(f"  Total duration: {self.total_workout_duration_min} minutes")
        if self.workout_types:
            types_str = ", ".join(f"{k}: {v}" for k, v in self.workout_types.items())
            lines.append(f"  Types: {types_str}")

        if self.weight_kg:
            lines.append("\nBODY:")
            lines.append(f"  Current weight: {self.weight_kg:.1f} kg")
            if self.weight_change_kg is not None:
                direction = "gained" if self.weight_change_kg > 0 else "lost"
                lines.append(f"  Weight change: {direction} {abs(self.weight_change_kg):.1f} kg")

        return "\n".join(lines)


@dataclass
class InsightResult:
    """Single insight with reasoning transparency."""

    category: str  # activity, heart, sleep, workouts, body, correlation
    headline: str  # Short insight title (max 60 chars)
    reasoning: str  # WHY this insight matters
    recommendation: str  # Actionable suggestion
    confidence: float  # 0.0-1.0
    source: Literal["ai", "rule"]  # Where insight came from

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "category": self.category,
            "headline": self.headline,
            "reasoning": self.reasoning,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
            "source": self.source,
        }


@dataclass
class DeliveryResult:
    """Result of a report delivery attempt."""

    success: bool
    attempt: int
    run_id: str | None = None
    error: str | None = None


@dataclass
class DailyMetrics:
    """Raw daily health values from InfluxDB."""

    # Sleep
    sleep_duration_min: float | None = None
    sleep_deep_min: float | None = None
    sleep_rem_min: float | None = None
    sleep_core_min: float | None = None
    sleep_awake_min: float | None = None
    sleep_quality_score: float | None = None

    # Heart
    resting_hr: float | None = None
    hrv_ms: float | None = None

    # Activity
    steps: int = 0
    active_calories: int = 0
    exercise_min: int = 0
    stand_hours: int = 0
    distance_m: float = 0
    floors_climbed: int = 0

    # Workouts
    workouts: list[dict] = field(default_factory=list)

    # Body
    weight_kg: float | None = None

    # Vitals
    spo2_pct: float | None = None
    respiratory_rate: float | None = None

    # 7-day averages
    avg_7d_steps: float | None = None
    avg_7d_sleep_duration_min: float | None = None
    avg_7d_resting_hr: float | None = None
    avg_7d_hrv_ms: float | None = None
    avg_7d_exercise_min: float | None = None


@dataclass
class PrivacySafeDailyMetrics:
    """Privacy-safe daily metrics for AI consumption."""

    mode: SummaryMode = SummaryMode.MORNING

    # Sleep
    sleep_duration_min: float | None = None
    sleep_deep_min: float | None = None
    sleep_rem_min: float | None = None
    sleep_core_min: float | None = None
    sleep_awake_min: float | None = None
    sleep_quality_score: float | None = None

    # Heart
    resting_hr: float | None = None
    hrv_ms: float | None = None

    # Activity
    steps: int = 0
    active_calories: int = 0
    exercise_min: int = 0
    stand_hours: int = 0

    # Workouts
    workout_summaries: list[str] = field(default_factory=list)

    # Body
    weight_kg: float | None = None

    # Comparisons to 7-day average (percentage)
    steps_vs_7d_avg: float | None = None
    exercise_vs_7d_avg: float | None = None
    hrv_vs_7d_avg: float | None = None

    def to_summary_text(self) -> str:
        """Format metrics as text for AI prompt, varies by mode."""
        lines = []

        if self.mode == SummaryMode.MORNING:
            lines.append("LAST NIGHT'S SLEEP:")
            if self.sleep_duration_min is not None:
                hours = self.sleep_duration_min / 60
                lines.append(f"  Duration: {hours:.1f} hours ({self.sleep_duration_min:.0f} min)")
            if self.sleep_deep_min is not None:
                lines.append(f"  Deep sleep: {self.sleep_deep_min:.0f} min")
            if self.sleep_rem_min is not None:
                lines.append(f"  REM sleep: {self.sleep_rem_min:.0f} min")
            if self.sleep_core_min is not None:
                lines.append(f"  Core sleep: {self.sleep_core_min:.0f} min")
            if self.sleep_awake_min is not None:
                lines.append(f"  Awake: {self.sleep_awake_min:.0f} min")
            if self.sleep_quality_score is not None:
                lines.append(f"  Quality score: {self.sleep_quality_score:.0f}%")

            lines.append("\nMORNING VITALS:")
            if self.resting_hr is not None:
                lines.append(f"  Resting heart rate: {self.resting_hr:.0f} bpm")
            if self.hrv_ms is not None:
                hrv_line = f"  HRV: {self.hrv_ms:.0f} ms"
                if self.hrv_vs_7d_avg is not None:
                    hrv_line += f" ({self.hrv_vs_7d_avg:+.0f}% vs 7-day avg)"
                lines.append(hrv_line)
            if self.weight_kg is not None:
                lines.append(f"  Weight: {self.weight_kg:.1f} kg")

            lines.append("\nYESTERDAY'S ACTIVITY:")
            lines.append(f"  Steps: {self.steps:,}")
            if self.steps_vs_7d_avg is not None:
                lines.append(f"  Steps vs 7-day avg: {self.steps_vs_7d_avg:+.0f}%")
            lines.append(f"  Active calories: {self.active_calories:,}")
            lines.append(f"  Exercise: {self.exercise_min} min")
        else:
            lines.append("TODAY'S ACTIVITY:")
            lines.append(f"  Steps: {self.steps:,}")
            if self.steps_vs_7d_avg is not None:
                lines.append(f"  Steps vs 7-day avg: {self.steps_vs_7d_avg:+.0f}%")
            lines.append(f"  Active calories: {self.active_calories:,}")
            lines.append(f"  Exercise: {self.exercise_min} min")
            if self.exercise_vs_7d_avg is not None:
                lines.append(f"  Exercise vs 7-day avg: {self.exercise_vs_7d_avg:+.0f}%")
            lines.append(f"  Stand hours: {self.stand_hours}")

            lines.append("\nHEART:")
            if self.resting_hr is not None:
                lines.append(f"  Resting heart rate: {self.resting_hr:.0f} bpm")
            if self.hrv_ms is not None:
                hrv_line = f"  HRV: {self.hrv_ms:.0f} ms"
                if self.hrv_vs_7d_avg is not None:
                    hrv_line += f" ({self.hrv_vs_7d_avg:+.0f}% vs 7-day avg)"
                lines.append(hrv_line)

        if self.workout_summaries:
            lines.append("\nWORKOUTS:")
            for summary in self.workout_summaries:
                lines.append(f"  - {summary}")

        return "\n".join(lines)
