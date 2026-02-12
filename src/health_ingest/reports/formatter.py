"""Telegram message formatting for health reports."""

from datetime import datetime

from .analysis_contract import AnalysisProvenance
from .models import InsightResult, PrivacySafeDailyMetrics, PrivacySafeMetrics, SummaryMode


class TelegramFormatter:
    """Formats health reports for Telegram delivery."""

    MAX_MESSAGE_LENGTH = 4000  # Telegram limit with buffer
    TEMPLATE_VERSION = "weekly-telegram.v2"

    def format(
        self,
        metrics: PrivacySafeMetrics,
        insights: list[InsightResult],
        week_start: datetime,
        week_end: datetime,
        analysis_provenance: AnalysisProvenance | None = None,
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

        # Insights split: facts first, recommendations second
        sections.append(self._format_observations(insights))
        sections.append(self._format_recommendations(insights))

        # Footer with source indicator
        source = self._get_primary_source(insights)
        sections.append(f"\n_{source.upper()}-generated insights_")
        if analysis_provenance:
            sections.append(
                self._format_provenance_footer(
                    analysis_provenance.with_template_version(self.TEMPLATE_VERSION)
                )
            )

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

    def _format_observations(self, insights: list[InsightResult]) -> str:
        """Format factual observations section."""
        if not insights:
            return "*Observations (Facts First)*\nNo significant patterns detected this week."

        lines = ["*Observations (Facts First)*"]

        for i, insight in enumerate(insights[:4], 1):  # Limit to 4 for readability
            lines.append(f"\n{i}. *{insight.headline}*")
            lines.append(f"   {insight.reasoning}")

        return "\n".join(lines)

    def _format_recommendations(self, insights: list[InsightResult]) -> str:
        """Format recommendation section."""
        if not insights:
            return "*Recommendations*\n- Keep your current healthy routines this week."

        lines = ["*Recommendations*"]
        for i, insight in enumerate(insights[:4], 1):
            lines.append(f"{i}. {insight.recommendation}")
        return "\n".join(lines)

    def _format_provenance_footer(self, provenance: AnalysisProvenance) -> str:
        """Format compact version trace to explain output changes."""
        dataset_version = provenance.dataset_version.replace("sha256:", "")
        prompt_ref = (
            f"{provenance.prompt_id}@{provenance.prompt_version}:{provenance.prompt_hash[:12]}"
        )
        return (
            "_trace: "
            f"req={provenance.request_type} "
            f"ds={dataset_version[:12]} "
            f"prompt={prompt_ref} "
            f"model={provenance.model} "
            f"tpl={provenance.report_template_version}_"
        )

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


def _trend_indicator(change_pct: float | None) -> str:
    """Return a trend indicator string."""
    if change_pct is None:
        return ""
    if change_pct > 5:
        return f"(+{change_pct:.0f}%)"
    if change_pct < -5:
        return f"({change_pct:.0f}%)"
    return "(stable)"


def _get_primary_source(insights: list[InsightResult]) -> str:
    """Determine the primary source of insights."""
    if not insights:
        return "rule"
    ai_count = sum(1 for i in insights if i.source == "ai")
    return "ai" if ai_count > len(insights) / 2 else "rule"


class DailyTelegramFormatter:
    """Formats daily health summaries for Telegram delivery."""

    MAX_MESSAGE_LENGTH = 4000
    TEMPLATE_VERSION = "daily-telegram.v2"

    def format(
        self,
        metrics: PrivacySafeDailyMetrics,
        insights: list[InsightResult],
        reference_time: datetime,
        analysis_provenance: AnalysisProvenance | None = None,
    ) -> str:
        """Format a daily summary for Telegram.

        Args:
            metrics: Privacy-safe daily metrics.
            insights: List of generated insights.
            reference_time: Reference time for the summary.

        Returns:
            Formatted Telegram message string.
        """
        sections = []

        if metrics.mode == SummaryMode.MORNING:
            sections.append(self._format_morning(metrics, reference_time))
        else:
            sections.append(self._format_evening(metrics, reference_time))

        # Insights split: facts first, recommendations second
        sections.append(self._format_observations(insights))
        sections.append(self._format_recommendations(insights))

        # Footer
        source = _get_primary_source(insights)
        sections.append(f"_{source.upper()}-generated insights_")
        if analysis_provenance:
            sections.append(
                self._format_provenance_footer(
                    analysis_provenance.with_template_version(self.TEMPLATE_VERSION)
                )
            )

        report = "\n\n".join(sections)

        if len(report) > self.MAX_MESSAGE_LENGTH:
            report = report[: self.MAX_MESSAGE_LENGTH - 100] + "\n\n_...truncated_"

        return report

    def _format_morning(self, metrics: PrivacySafeDailyMetrics, reference_time: datetime) -> str:
        """Format morning summary sections."""
        date_str = reference_time.strftime("%A, %b %d")
        lines = [f"*Good Morning*\n{date_str}"]

        # Sleep section
        if metrics.sleep_duration_min is not None:
            lines.append("")
            lines.append("*Sleep*")
            hours = metrics.sleep_duration_min / 60
            lines.append(f"Duration: {hours:.1f}h")
            parts = []
            if metrics.sleep_deep_min is not None:
                parts.append(f"Deep {metrics.sleep_deep_min:.0f}m")
            if metrics.sleep_rem_min is not None:
                parts.append(f"REM {metrics.sleep_rem_min:.0f}m")
            if metrics.sleep_core_min is not None:
                parts.append(f"Core {metrics.sleep_core_min:.0f}m")
            if parts:
                lines.append(" | ".join(parts))
            if metrics.sleep_quality_score is not None:
                lines.append(f"Quality: {metrics.sleep_quality_score:.0f}%")

        # Morning vitals
        vitals = []
        if metrics.resting_hr is not None:
            vitals.append(f"Resting HR: {metrics.resting_hr:.0f} bpm")
        if metrics.hrv_ms is not None:
            hrv_trend = _trend_indicator(metrics.hrv_vs_7d_avg)
            vitals.append(f"HRV: {metrics.hrv_ms:.0f} ms {hrv_trend}")
        if metrics.weight_kg is not None:
            vitals.append(f"Weight: {metrics.weight_kg:.1f} kg")
        if vitals:
            lines.append("")
            lines.append("*Vitals*")
            lines.extend(vitals)

        # Yesterday's activity
        lines.append("")
        lines.append("*Yesterday's Activity*")
        steps_trend = _trend_indicator(metrics.steps_vs_7d_avg)
        lines.append(f"Steps: {metrics.steps:,} {steps_trend}")
        lines.append(f"Calories: {metrics.active_calories:,}")
        lines.append(f"Exercise: {metrics.exercise_min} min")

        # Workouts
        if metrics.workout_summaries:
            lines.append("")
            lines.append("*Workouts*")
            for summary in metrics.workout_summaries:
                lines.append(f"- {summary}")

        return "\n".join(lines)

    def _format_evening(self, metrics: PrivacySafeDailyMetrics, reference_time: datetime) -> str:
        """Format evening summary sections."""
        date_str = reference_time.strftime("%A, %b %d")
        lines = [f"*Evening Recap*\n{date_str}"]

        # Today's activity
        lines.append("")
        lines.append("*Today's Activity*")
        steps_trend = _trend_indicator(metrics.steps_vs_7d_avg)
        lines.append(f"Steps: {metrics.steps:,} {steps_trend}")
        lines.append(f"Calories: {metrics.active_calories:,}")
        exercise_trend = _trend_indicator(metrics.exercise_vs_7d_avg)
        lines.append(f"Exercise: {metrics.exercise_min} min {exercise_trend}")
        lines.append(f"Stand hours: {metrics.stand_hours}")

        # Workouts
        if metrics.workout_summaries:
            lines.append("")
            lines.append("*Workouts*")
            for summary in metrics.workout_summaries:
                lines.append(f"- {summary}")

        # Heart summary
        heart = []
        if metrics.resting_hr is not None:
            heart.append(f"Resting HR: {metrics.resting_hr:.0f} bpm")
        if metrics.hrv_ms is not None:
            hrv_trend = _trend_indicator(metrics.hrv_vs_7d_avg)
            heart.append(f"HRV: {metrics.hrv_ms:.0f} ms {hrv_trend}")
        if heart:
            lines.append("")
            lines.append("*Heart*")
            lines.extend(heart)

        return "\n".join(lines)

    def _format_observations(self, insights: list[InsightResult]) -> str:
        """Format factual observations section for daily report."""
        if not insights:
            return "*What Stood Out*\nNo specific patterns stood out today."

        lines = ["*What Stood Out*"]
        for insight in insights[:3]:
            lines.append(f"- *{insight.headline}*: {insight.reasoning}")
        return "\n".join(lines)

    def _format_recommendations(self, insights: list[InsightResult]) -> str:
        """Format recommendation section for daily report."""
        if not insights:
            return "*Recommended Actions*\n- Keep consistent hydration, movement, and sleep timing."

        lines = ["*Recommended Actions*"]
        for insight in insights[:3]:
            lines.append(f"- {insight.recommendation}")
        return "\n".join(lines)

    def _format_provenance_footer(self, provenance: AnalysisProvenance) -> str:
        """Format compact version trace to explain output changes."""
        dataset_version = provenance.dataset_version.replace("sha256:", "")
        prompt_ref = (
            f"{provenance.prompt_id}@{provenance.prompt_version}:{provenance.prompt_hash[:12]}"
        )
        return (
            "_trace: "
            f"req={provenance.request_type} "
            f"ds={dataset_version[:12]} "
            f"prompt={prompt_ref} "
            f"model={provenance.model} "
            f"tpl={provenance.report_template_version}_"
        )
