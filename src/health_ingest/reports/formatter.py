"""Telegram message formatting for health reports."""

from datetime import datetime

from .models import InsightResult, PrivacySafeMetrics


class TelegramFormatter:
    """Formats health reports for Telegram delivery."""

    MAX_MESSAGE_LENGTH = 4000  # Telegram limit with buffer

    def format(
        self,
        metrics: PrivacySafeMetrics,
        insights: list[InsightResult],
        week_start: datetime,
        week_end: datetime,
    ) -> str:
        """Format a complete weekly report for Telegram.

        Uses Telegram Markdown formatting for readability.

        Args:
            metrics: Privacy-safe aggregated metrics.
            insights: List of generated insights.
            week_start: Start of the report week.
            week_end: End of the report week.

        Returns:
            Formatted Telegram message string.
        """
        sections = []

        # Header
        date_range = f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d')}"
        sections.append(f"*Weekly Health Report*\n{date_range}")

        # Quick Stats section
        sections.append(self._format_quick_stats(metrics))

        # Insights section
        sections.append(self._format_insights(insights))

        # Footer with source indicator
        source = self._get_primary_source(insights)
        sections.append(f"\n_{source.upper()}-generated insights_")

        report = "\n\n".join(sections)

        # Truncate if needed
        if len(report) > self.MAX_MESSAGE_LENGTH:
            report = report[: self.MAX_MESSAGE_LENGTH - 100] + "\n\n_...truncated_"

        return report

    def _format_quick_stats(self, metrics: PrivacySafeMetrics) -> str:
        """Format the quick statistics section."""
        lines = ["*Quick Stats*"]

        # Activity
        steps_trend = self._trend_indicator(metrics.steps_change_pct)
        lines.append(f"Steps: {metrics.avg_daily_steps:,}/day {steps_trend}")

        # Sleep
        if metrics.avg_duration_hours:
            sleep_trend = self._trend_indicator(metrics.sleep_change_pct)
            lines.append(f"Sleep: {metrics.avg_duration_hours:.1f}h avg {sleep_trend}")

        # Exercise
        exercise_trend = self._trend_indicator(metrics.exercise_change_pct)
        lines.append(f"Exercise: {metrics.total_exercise_min} min {exercise_trend}")

        # Heart metrics
        if metrics.avg_resting_hr:
            lines.append(f"Resting HR: {metrics.avg_resting_hr:.0f} bpm")
        if metrics.avg_hrv:
            hrv_trend = self._trend_indicator(metrics.hrv_change_pct)
            lines.append(f"HRV: {metrics.avg_hrv:.0f} ms {hrv_trend}")

        # Workouts
        if metrics.workout_count > 0:
            lines.append(f"Workouts: {metrics.workout_count}")

        # Weight
        if metrics.weight_kg:
            weight_change = ""
            if metrics.weight_change_kg is not None and abs(metrics.weight_change_kg) >= 0.1:
                sign = "+" if metrics.weight_change_kg > 0 else ""
                weight_change = f" ({sign}{metrics.weight_change_kg:.1f}kg)"
            lines.append(f"Weight: {metrics.weight_kg:.1f}kg{weight_change}")

        return "\n".join(lines)

    def _format_insights(self, insights: list[InsightResult]) -> str:
        """Format the insights section."""
        if not insights:
            return "*Insights*\nNo significant patterns detected this week."

        lines = ["*Insights*"]

        for i, insight in enumerate(insights[:4], 1):  # Limit to 4 for readability
            lines.append(f"\n{i}. *{insight.headline}*")
            lines.append(f"   {insight.reasoning}")
            lines.append(f"   {insight.recommendation}")

        return "\n".join(lines)

    def _trend_indicator(self, change_pct: float | None) -> str:
        """Return a trend indicator string."""
        if change_pct is None:
            return ""
        if change_pct > 5:
            return f"(+{change_pct:.0f}%)"
        if change_pct < -5:
            return f"({change_pct:.0f}%)"
        return "(stable)"

    def _get_primary_source(self, insights: list[InsightResult]) -> str:
        """Determine the primary source of insights."""
        if not insights:
            return "rule"
        ai_count = sum(1 for i in insights if i.source == "ai")
        return "ai" if ai_count > len(insights) / 2 else "rule"

    def format_error(self, error: str) -> str:
        """Format an error message for Telegram.

        Args:
            error: Error description.

        Returns:
            Formatted error message.
        """
        return f"*Weekly Health Report*\n\nUnable to generate report: {error}"
