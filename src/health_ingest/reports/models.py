"""Data models for health insights and privacy-safe metrics."""

from dataclasses import dataclass, field
from typing import Literal


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
