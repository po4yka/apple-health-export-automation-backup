"""SVG infographic rendering for weekly health reports."""

from datetime import datetime
from html import escape
from pathlib import Path

from .analysis_contract import AnalysisProvenance
from .models import InsightResult, PrivacySafeDailyMetrics, PrivacySafeMetrics, SummaryMode


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, maximum))


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3]}..."


class WeeklyInfographicRenderer:
    """Renders a styled weekly infographic as SVG."""

    WIDTH = 1080
    HEIGHT = 1440
    TEMPLATE_VERSION = "weekly-infographic.v1"

    def render(
        self,
        *,
        metrics: PrivacySafeMetrics,
        insights: list[InsightResult],
        week_start: datetime,
        week_end: datetime,
        analysis_provenance: AnalysisProvenance | None = None,
    ) -> str:
        """Render weekly metrics and insights into an SVG infographic."""
        date_range = f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}"
        cards_svg = self._render_stat_cards(metrics)
        bars_svg = self._render_progress_bars(metrics)
        insights_svg = self._render_insight_cards(insights)
        provenance_svg = self._render_provenance(analysis_provenance)

        return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{self.WIDTH}" height="{self.HEIGHT}" viewBox="0 0 {self.WIDTH} {self.HEIGHT}">
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0B1B2B"/>
      <stop offset="45%" stop-color="#123A53"/>
      <stop offset="100%" stop-color="#061018"/>
    </linearGradient>
    <linearGradient id="cardAccent" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#66D1FF"/>
      <stop offset="100%" stop-color="#2DE2B7"/>
    </linearGradient>
    <linearGradient id="barFill" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#6FE8FF"/>
      <stop offset="100%" stop-color="#58FFB8"/>
    </linearGradient>
    <filter id="softGlow" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur stdDeviation="18" result="blur"/>
      <feMerge>
        <feMergeNode in="blur"/>
        <feMergeNode in="SourceGraphic"/>
      </feMerge>
    </filter>
  </defs>

  <rect x="0" y="0" width="{self.WIDTH}" height="{self.HEIGHT}" fill="url(#bg)"/>
  <circle cx="980" cy="120" r="220" fill="#7DE7FF" opacity="0.08" filter="url(#softGlow)"/>
  <circle cx="120" cy="1240" r="180" fill="#2DE2B7" opacity="0.06" filter="url(#softGlow)"/>

  <text x="72" y="92" fill="#B7D6E7" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="24" letter-spacing="1.5">WEEKLY HEALTH INFOGRAPHIC</text>
  <text x="72" y="150" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="54" font-weight="700">Health Performance</text>
  <text x="72" y="188" fill="#C8E5F0" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="26">{escape(date_range)}</text>

  {cards_svg}
  {bars_svg}
  {insights_svg}
  {provenance_svg}
</svg>"""

    def write_svg(self, svg_text: str, output_path: str | Path) -> Path:
        """Persist SVG to disk and return final path."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(svg_text, encoding="utf-8")
        return path

    def _render_stat_cards(self, metrics: PrivacySafeMetrics) -> str:
        cards = [
            ("Avg Steps / Day", f"{metrics.avg_daily_steps:,}"),
            ("Avg Sleep / Night", f"{metrics.avg_duration_hours:.1f} h" if metrics.avg_duration_hours else "N/A"),
            ("Exercise / Week", f"{metrics.total_exercise_min} min"),
            ("Avg HRV", f"{metrics.avg_hrv:.0f} ms" if metrics.avg_hrv else "N/A"),
        ]
        x_positions = (72, 330, 588, 846)
        elements: list[str] = []
        for (label, value), x in zip(cards, x_positions, strict=True):
            elements.append(
                f"""<g>
  <rect x="{x}" y="248" width="210" height="146" rx="24" fill="#FFFFFF" fill-opacity="0.08" stroke="#A0D4E8" stroke-opacity="0.28"/>
  <rect x="{x}" y="248" width="210" height="5" rx="3" fill="url(#cardAccent)"/>
  <text x="{x + 20}" y="300" fill="#B7D8E8" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="18">{escape(label)}</text>
  <text x="{x + 20}" y="352" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="38" font-weight="700">{escape(value)}</text>
</g>"""
            )
        return "\n".join(elements)

    def _render_progress_bars(self, metrics: PrivacySafeMetrics) -> str:
        step_ratio = _clamp(metrics.avg_daily_steps / 10000 if metrics.avg_daily_steps else 0.0, 0.0, 1.2)
        sleep_ratio = _clamp(
            (metrics.avg_duration_hours or 0.0) / 8.0,
            0.0,
            1.2,
        )
        exercise_ratio = _clamp(metrics.total_exercise_min / 150 if metrics.total_exercise_min else 0.0, 0.0, 1.2)

        return "\n".join(
            [
                '<g transform="translate(72, 450)">',
                '  <rect x="0" y="0" width="936" height="290" rx="30" fill="#FFFFFF" fill-opacity="0.07" stroke="#A0D4E8" stroke-opacity="0.25"/>',
                '  <text x="32" y="54" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="34" font-weight="700">Goal Progress</text>',
                self._bar_row(32, 108, "Steps Goal (10,000/day)", step_ratio, f"{step_ratio * 100:.0f}%"),
                self._bar_row(32, 176, "Sleep Goal (8h/night)", sleep_ratio, f"{sleep_ratio * 100:.0f}%"),
                self._bar_row(32, 244, "Exercise Goal (150min/week)", exercise_ratio, f"{exercise_ratio * 100:.0f}%"),
                "</g>",
            ]
        )

    def _bar_row(self, x: int, y: int, label: str, ratio: float, pct_text: str) -> str:
        track_width = 720
        fill_width = int(track_width * ratio)
        return "\n".join(
            [
                f'<text x="{x}" y="{y}" fill="#BFE0EE" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="20">{escape(label)}</text>',
                f'<rect x="{x}" y="{y + 16}" width="{track_width}" height="18" rx="9" fill="#FFFFFF" fill-opacity="0.14"/>',
                f'<rect x="{x}" y="{y + 16}" width="{fill_width}" height="18" rx="9" fill="url(#barFill)"/>',
                f'<text x="{x + 748}" y="{y + 31}" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="18" text-anchor="end">{escape(pct_text)}</text>',
            ]
        )

    def _render_insight_cards(self, insights: list[InsightResult]) -> str:
        if not insights:
            return """
<g transform="translate(72, 772)">
  <rect x="0" y="0" width="936" height="460" rx="30" fill="#FFFFFF" fill-opacity="0.07" stroke="#A0D4E8" stroke-opacity="0.25"/>
  <text x="32" y="56" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="34" font-weight="700">Insights</text>
  <text x="32" y="122" fill="#CBE6F2" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="24">No notable patterns this week.</text>
</g>
"""

        lines: list[str] = [
            '<g transform="translate(72, 772)">',
            '  <rect x="0" y="0" width="936" height="600" rx="30" fill="#FFFFFF" fill-opacity="0.07" stroke="#A0D4E8" stroke-opacity="0.25"/>',
            '  <text x="32" y="56" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="34" font-weight="700">Facts and Recommendations</text>',
        ]

        y = 96
        for index, insight in enumerate(insights[:4], start=1):
            headline = _truncate(insight.headline, 54)
            reasoning = _truncate(insight.reasoning, 110)
            recommendation = _truncate(insight.recommendation, 110)
            lines.append(
                f"""
  <g transform="translate(24, {y})">
    <rect x="0" y="0" width="888" height="116" rx="18" fill="#05111A" fill-opacity="0.36" stroke="#8CCEE3" stroke-opacity="0.22"/>
    <text x="18" y="34" fill="#D2EEFA" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="20" font-weight="700">{index}. {escape(headline)}</text>
    <text x="18" y="64" fill="#B2D7E6" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="17">Fact: {escape(reasoning)}</text>
    <text x="18" y="93" fill="#8DF6D4" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="17">Action: {escape(recommendation)}</text>
  </g>"""
            )
            y += 130
        lines.append("</g>")
        return "\n".join(lines)

    def _render_provenance(self, provenance: AnalysisProvenance | None) -> str:
        if provenance is None:
            return ""
        dataset_hash = provenance.dataset_version.replace("sha256:", "")[:12]
        prompt_hash = provenance.prompt_hash[:12]
        trace = (
            f"trace: req={provenance.request_type} ds={dataset_hash} "
            f"prompt={provenance.prompt_id}@{provenance.prompt_version}:{prompt_hash} "
            f"model={provenance.model} tpl={self.TEMPLATE_VERSION}"
        )
        return (
            '<text x="72" y="1410" fill="#8AAFC0" '
            'font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" '
            f'font-size="15">{escape(_truncate(trace, 160))}</text>'
        )


class DailyInfographicRenderer:
    """Renders a styled daily infographic as SVG."""

    WIDTH = 1080
    HEIGHT = 1440
    MORNING_TEMPLATE_VERSION = "daily-morning-infographic.v1"
    EVENING_TEMPLATE_VERSION = "daily-evening-infographic.v1"

    def render(
        self,
        *,
        metrics: PrivacySafeDailyMetrics,
        insights: list[InsightResult],
        reference_time: datetime,
        analysis_provenance: AnalysisProvenance | None = None,
    ) -> str:
        """Render daily metrics and insights into an SVG infographic."""
        is_morning = metrics.mode == SummaryMode.MORNING
        date_line = reference_time.strftime("%A, %b %d, %Y")
        palette = self._palette(is_morning)
        top_stats = self._render_top_stats(metrics, is_morning)
        bars = self._render_daily_bars(metrics, is_morning)
        insights_svg = self._render_insights(insights, is_morning)
        provenance_svg = self._render_provenance(metrics.mode, analysis_provenance)
        title = "Morning Readiness" if is_morning else "Evening Recovery"
        subtitle = "DAILY HEALTH SNAPSHOT"

        return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{self.WIDTH}" height="{self.HEIGHT}" viewBox="0 0 {self.WIDTH} {self.HEIGHT}">
  <defs>
    <linearGradient id="bgDaily" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="{palette["bg_start"]}"/>
      <stop offset="55%" stop-color="{palette["bg_mid"]}"/>
      <stop offset="100%" stop-color="{palette["bg_end"]}"/>
    </linearGradient>
    <linearGradient id="dailyAccent" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="{palette["accent_a"]}"/>
      <stop offset="100%" stop-color="{palette["accent_b"]}"/>
    </linearGradient>
    <linearGradient id="dailyBarFill" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="{palette["bar_a"]}"/>
      <stop offset="100%" stop-color="{palette["bar_b"]}"/>
    </linearGradient>
    <filter id="dailyGlow" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur stdDeviation="16" result="blur"/>
      <feMerge>
        <feMergeNode in="blur"/>
        <feMergeNode in="SourceGraphic"/>
      </feMerge>
    </filter>
  </defs>

  <rect x="0" y="0" width="{self.WIDTH}" height="{self.HEIGHT}" fill="url(#bgDaily)"/>
  <circle cx="900" cy="130" r="210" fill="{palette["orb_a"]}" opacity="0.10" filter="url(#dailyGlow)"/>
  <circle cx="160" cy="1230" r="190" fill="{palette["orb_b"]}" opacity="0.08" filter="url(#dailyGlow)"/>

  <text x="72" y="90" fill="{palette["label"]}" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="24" letter-spacing="1.5">{subtitle}</text>
  <text x="72" y="148" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="56" font-weight="700">{title}</text>
  <text x="72" y="188" fill="{palette["muted"]}" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="26">{escape(date_line)}</text>

  {top_stats}
  {bars}
  {insights_svg}
  {provenance_svg}
</svg>"""

    def write_svg(self, svg_text: str, output_path: str | Path) -> Path:
        """Persist SVG to disk and return final path."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(svg_text, encoding="utf-8")
        return path

    def _palette(self, is_morning: bool) -> dict[str, str]:
        if is_morning:
            return {
                "bg_start": "#13243B",
                "bg_mid": "#2E5B74",
                "bg_end": "#112334",
                "accent_a": "#FFD166",
                "accent_b": "#7EE6FF",
                "bar_a": "#FFD166",
                "bar_b": "#5AE5FF",
                "orb_a": "#FFD166",
                "orb_b": "#90F6FF",
                "label": "#FFDDA1",
                "muted": "#E5F0F6",
                "card_stroke": "#BDE4F3",
            }
        return {
            "bg_start": "#1B1731",
            "bg_mid": "#233B5C",
            "bg_end": "#111626",
            "accent_a": "#FFA06B",
            "accent_b": "#77C3FF",
            "bar_a": "#FFA06B",
            "bar_b": "#7DD0FF",
            "orb_a": "#FF9B72",
            "orb_b": "#7CC4FF",
            "label": "#FFC7AD",
            "muted": "#DDEAF4",
            "card_stroke": "#B4D6ED",
        }

    def _render_top_stats(self, metrics: PrivacySafeDailyMetrics, is_morning: bool) -> str:
        stats = [
            ("Steps", f"{metrics.steps:,}"),
            ("Exercise", f"{metrics.exercise_min} min"),
            ("Calories", f"{metrics.active_calories:,}"),
            ("HRV", f"{metrics.hrv_ms:.0f} ms" if metrics.hrv_ms is not None else "N/A"),
        ]
        if is_morning:
            stats[3] = (
                "Sleep",
                f"{metrics.sleep_duration_min / 60:.1f} h" if metrics.sleep_duration_min else "N/A",
            )
        x_positions = (72, 330, 588, 846)
        cards: list[str] = []
        for (label, value), x in zip(stats, x_positions, strict=True):
            cards.append(
                f"""<g>
  <rect x="{x}" y="246" width="210" height="146" rx="24" fill="#FFFFFF" fill-opacity="0.10" stroke="#C1E4F4" stroke-opacity="0.3"/>
  <rect x="{x}" y="246" width="210" height="5" rx="3" fill="url(#dailyAccent)"/>
  <text x="{x + 20}" y="298" fill="#D1EBF7" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="18">{escape(label)}</text>
  <text x="{x + 20}" y="350" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="38" font-weight="700">{escape(value)}</text>
</g>"""
            )
        return "\n".join(cards)

    def _render_daily_bars(self, metrics: PrivacySafeDailyMetrics, is_morning: bool) -> str:
        steps_ratio = _clamp(metrics.steps / 10000 if metrics.steps else 0.0, 0.0, 1.2)
        exercise_ratio = _clamp(metrics.exercise_min / 45 if metrics.exercise_min else 0.0, 0.0, 1.2)
        if is_morning:
            recovery_ratio = _clamp((metrics.sleep_quality_score or 0.0) / 100.0, 0.0, 1.2)
            recovery_label = "Recovery (Sleep Quality)"
            recovery_text = f"{(metrics.sleep_quality_score or 0.0):.0f}%"
        else:
            recovery_ratio = _clamp(((metrics.hrv_vs_7d_avg or 0.0) + 100.0) / 100.0, 0.0, 1.2)
            recovery_label = "Recovery (HRV vs 7d)"
            recovery_text = f"{(metrics.hrv_vs_7d_avg or 0.0):+.0f}%"

        return "\n".join(
            [
                '<g transform="translate(72, 450)">',
                '  <rect x="0" y="0" width="936" height="290" rx="30" fill="#FFFFFF" fill-opacity="0.08" stroke="#BADDEF" stroke-opacity="0.28"/>',
                '  <text x="32" y="54" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="34" font-weight="700">Today at a Glance</text>',
                self._bar_row(32, 108, "Movement (Steps)", steps_ratio, f"{steps_ratio * 100:.0f}%"),
                self._bar_row(32, 176, "Training Load (Exercise)", exercise_ratio, f"{exercise_ratio * 100:.0f}%"),
                self._bar_row(32, 244, recovery_label, recovery_ratio, recovery_text),
                "</g>",
            ]
        )

    def _bar_row(self, x: int, y: int, label: str, ratio: float, pct_text: str) -> str:
        track_width = 720
        fill_width = int(track_width * ratio)
        return "\n".join(
            [
                f'<text x="{x}" y="{y}" fill="#CDE8F4" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="20">{escape(label)}</text>',
                f'<rect x="{x}" y="{y + 16}" width="{track_width}" height="18" rx="9" fill="#FFFFFF" fill-opacity="0.16"/>',
                f'<rect x="{x}" y="{y + 16}" width="{fill_width}" height="18" rx="9" fill="url(#dailyBarFill)"/>',
                f'<text x="{x + 748}" y="{y + 31}" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="18" text-anchor="end">{escape(pct_text)}</text>',
            ]
        )

    def _render_insights(self, insights: list[InsightResult], is_morning: bool) -> str:
        panel_title = "Morning Insights" if is_morning else "Evening Insights"
        if not insights:
            return f"""
<g transform="translate(72, 772)">
  <rect x="0" y="0" width="936" height="560" rx="30" fill="#FFFFFF" fill-opacity="0.08" stroke="#BADDEF" stroke-opacity="0.28"/>
  <text x="32" y="56" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="34" font-weight="700">{panel_title}</text>
  <text x="32" y="118" fill="#D1E7F3" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="24">No notable patterns in today's summary.</text>
</g>
"""

        lines: list[str] = [
            '<g transform="translate(72, 772)">',
            '  <rect x="0" y="0" width="936" height="620" rx="30" fill="#FFFFFF" fill-opacity="0.08" stroke="#BADDEF" stroke-opacity="0.28"/>',
            f'  <text x="32" y="56" fill="#FFFFFF" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="34" font-weight="700">{panel_title}</text>',
        ]
        y = 96
        for index, insight in enumerate(insights[:3], start=1):
            headline = _truncate(insight.headline, 56)
            reasoning = _truncate(insight.reasoning, 112)
            recommendation = _truncate(insight.recommendation, 112)
            lines.append(
                f"""
  <g transform="translate(24, {y})">
    <rect x="0" y="0" width="888" height="142" rx="18" fill="#081520" fill-opacity="0.4" stroke="#A5D4E9" stroke-opacity="0.24"/>
    <text x="18" y="36" fill="#D6EDF8" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="20" font-weight="700">{index}. {escape(headline)}</text>
    <text x="18" y="72" fill="#C0DEED" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="17">Fact: {escape(reasoning)}</text>
    <text x="18" y="108" fill="#96F5DA" font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" font-size="17">Action: {escape(recommendation)}</text>
  </g>"""
            )
            y += 158
        lines.append("</g>")
        return "\n".join(lines)

    def _render_provenance(
        self,
        mode: SummaryMode,
        provenance: AnalysisProvenance | None,
    ) -> str:
        if provenance is None:
            return ""
        dataset_hash = provenance.dataset_version.replace("sha256:", "")[:12]
        prompt_hash = provenance.prompt_hash[:12]
        template_version = (
            self.MORNING_TEMPLATE_VERSION
            if mode == SummaryMode.MORNING
            else self.EVENING_TEMPLATE_VERSION
        )
        trace = (
            f"trace: req={provenance.request_type} ds={dataset_hash} "
            f"prompt={provenance.prompt_id}@{provenance.prompt_version}:{prompt_hash} "
            f"model={provenance.model} tpl={template_version}"
        )
        return (
            '<text x="72" y="1410" fill="#90B7C8" '
            'font-family="Avenir Next, Helvetica Neue, Segoe UI, sans-serif" '
            f'font-size="15">{escape(_truncate(trace, 160))}</text>'
        )
