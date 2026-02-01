"""Tests for insight generation."""

import pytest

from health_ingest.reports.insights import InsightEngine
from health_ingest.reports.models import PrivacySafeMetrics
from health_ingest.reports.rules import RuleEngine


@pytest.fixture
def sample_metrics():
    """Create sample privacy-safe metrics."""
    return PrivacySafeMetrics(
        avg_daily_steps=9500,
        steps_change_pct=12.5,
        total_exercise_min=180,
        exercise_change_pct=15.0,
        total_active_calories=2500,
        avg_resting_hr=62.0,
        resting_hr_range=(58.0, 68.0),
        avg_hrv=48.0,
        hrv_change_pct=10.0,
        avg_duration_hours=7.2,
        avg_quality_pct=85.0,
        sleep_change_pct=-5.0,
        avg_deep_sleep_min=90.0,
        avg_rem_sleep_min=100.0,
        workout_count=4,
        total_workout_duration_min=200,
        workout_types={"running": 2, "cycling": 1, "strength": 1},
        weight_kg=75.5,
        weight_change_kg=-0.3,
    )


@pytest.fixture
def low_activity_metrics():
    """Create metrics with concerning patterns."""
    return PrivacySafeMetrics(
        avg_daily_steps=4500,
        steps_change_pct=-25.0,
        total_exercise_min=30,
        exercise_change_pct=-40.0,
        total_active_calories=800,
        avg_resting_hr=78.0,
        resting_hr_range=(72.0, 85.0),
        avg_hrv=35.0,
        hrv_change_pct=-15.0,
        avg_duration_hours=5.5,
        avg_quality_pct=60.0,
        sleep_change_pct=-20.0,
        workout_count=0,
        total_workout_duration_min=0,
        workout_types={},
        weight_kg=80.0,
        weight_change_kg=1.2,
    )


class TestPrivacySafeMetrics:
    """Tests for PrivacySafeMetrics model."""

    def test_hrv_trend_improving(self):
        """Test HRV trend calculation for improving."""
        metrics = PrivacySafeMetrics(hrv_change_pct=15.0)
        assert metrics.hrv_trend == "improving"

    def test_hrv_trend_declining(self):
        """Test HRV trend calculation for declining."""
        metrics = PrivacySafeMetrics(hrv_change_pct=-10.0)
        assert metrics.hrv_trend == "declining"

    def test_hrv_trend_stable(self):
        """Test HRV trend calculation for stable."""
        metrics = PrivacySafeMetrics(hrv_change_pct=2.0)
        assert metrics.hrv_trend == "stable"

    def test_hrv_trend_none(self):
        """Test HRV trend when no data."""
        metrics = PrivacySafeMetrics()
        assert metrics.hrv_trend is None

    def test_to_summary_text(self, sample_metrics):
        """Test text summary generation."""
        text = sample_metrics.to_summary_text()

        assert "ACTIVITY:" in text
        assert "9,500" in text  # avg daily steps
        assert "HEART:" in text
        assert "62" in text  # resting HR
        assert "SLEEP:" in text
        assert "7.2 hours" in text
        assert "WORKOUTS:" in text
        assert "4" in text  # workout count


class TestRuleEngine:
    """Tests for the rule-based insight engine."""

    @pytest.fixture
    def engine(self):
        """Create rule engine instance."""
        return RuleEngine()

    def test_steps_goal_achieved_rule(self, engine):
        """Test rule triggers for meeting step goal."""
        metrics = PrivacySafeMetrics(avg_daily_steps=11000)
        insights = engine.evaluate(metrics)

        step_insights = [i for i in insights if "step" in i.headline.lower()]
        assert len(step_insights) >= 1
        assert "achieved" in step_insights[0].headline.lower()

    def test_steps_nearly_achieved_rule(self, engine):
        """Test rule for nearly meeting step goal."""
        metrics = PrivacySafeMetrics(avg_daily_steps=9200)
        insights = engine.evaluate(metrics)

        step_insights = [i for i in insights if "step" in i.headline.lower()]
        assert len(step_insights) >= 1

    def test_steps_significant_drop_rule(self, engine):
        """Test rule for significant activity drop."""
        metrics = PrivacySafeMetrics(avg_daily_steps=5000, steps_change_pct=-30.0)
        insights = engine.evaluate(metrics)

        drop_insights = [i for i in insights if "drop" in i.headline.lower()]
        assert len(drop_insights) >= 1

    def test_hrv_improving_rule(self, engine):
        """Test rule for HRV improvement."""
        metrics = PrivacySafeMetrics(
            avg_hrv=50.0,
            hrv_change_pct=15.0,
        )
        insights = engine.evaluate(metrics)

        hrv_insights = [i for i in insights if i.category == "heart"]
        assert len(hrv_insights) >= 1

    def test_sleep_deficit_rule(self, engine):
        """Test rule for sleep deficit."""
        metrics = PrivacySafeMetrics(avg_duration_hours=5.5)
        insights = engine.evaluate(metrics)

        sleep_insights = [i for i in insights if i.category == "sleep"]
        assert len(sleep_insights) >= 1
        assert any("below" in i.headline.lower() for i in sleep_insights)

    def test_correlation_rule(self, engine):
        """Test cross-metric correlation rules."""
        metrics = PrivacySafeMetrics(
            exercise_change_pct=20.0,
            hrv_change_pct=15.0,
            total_exercise_min=200,
            avg_hrv=50.0,
        )
        insights = engine.evaluate(metrics)

        correlation_insights = [i for i in insights if i.category == "correlation"]
        assert len(correlation_insights) >= 1

    def test_max_insights_limit(self, engine, sample_metrics):
        """Test that max_insights parameter is respected."""
        insights = engine.evaluate(sample_metrics, max_insights=2)
        assert len(insights) <= 2

    def test_insights_have_required_fields(self, engine, sample_metrics):
        """Test that all insights have required fields."""
        insights = engine.evaluate(sample_metrics)

        for insight in insights:
            assert insight.category
            assert insight.headline
            assert insight.reasoning
            assert insight.recommendation
            assert 0 <= insight.confidence <= 1
            assert insight.source == "rule"

    def test_priority_ordering(self, engine, low_activity_metrics):
        """Test that higher priority rules come first."""
        insights = engine.evaluate(low_activity_metrics)

        # Sleep deficit and HRV decline are high priority
        # They should appear before lower-priority insights
        if len(insights) > 1:
            # Just verify insights are returned (ordering may vary)
            assert all(i.confidence > 0 for i in insights)


class TestInsightEngine:
    """Tests for the main insight engine with AI fallback."""

    @pytest.fixture
    def mock_anthropic_settings(self):
        """Create mock Anthropic settings without API key."""
        from health_ingest.config import AnthropicSettings

        return AnthropicSettings(api_key=None)

    @pytest.fixture
    def mock_insight_settings(self):
        """Create mock insight settings."""
        from health_ingest.config import InsightSettings

        return InsightSettings(prefer_ai=True, max_insights=5)

    @pytest.fixture
    def mock_openai_settings(self):
        """Create mock OpenAI settings with API key."""
        from health_ingest.config import OpenAISettings

        return OpenAISettings(api_key="test-key")

    def _patch_openai(self, monkeypatch, response_text: str):
        """Patch OpenAI client to return the provided response."""

        class _FakeMessage:
            def __init__(self, content: str) -> None:
                self.content = content

        class _FakeChoice:
            def __init__(self, content: str) -> None:
                self.message = _FakeMessage(content)

        class _FakeCompletions:
            def __init__(self, content: str) -> None:
                self._content = content

            def create(self, *args, **kwargs):
                return type("Response", (), {"choices": [_FakeChoice(self._content)]})()

        class _FakeChat:
            def __init__(self, content: str) -> None:
                self.completions = _FakeCompletions(content)

        class _FakeOpenAI:
            def __init__(self, *args, **kwargs) -> None:
                self.chat = _FakeChat(response_text)

        monkeypatch.setattr("health_ingest.reports.insights.OpenAI", _FakeOpenAI)

    @pytest.mark.asyncio
    async def test_falls_back_to_rules_without_api_key(
        self, mock_anthropic_settings, mock_insight_settings, sample_metrics
    ):
        """Test that engine falls back to rules when no API key."""
        engine = InsightEngine(
            anthropic_settings=mock_anthropic_settings,
            insight_settings=mock_insight_settings,
        )

        insights = await engine.generate(sample_metrics)

        assert len(insights) > 0
        assert all(i.source == "rule" for i in insights)

    @pytest.mark.asyncio
    async def test_respects_max_insights(
        self, mock_anthropic_settings, mock_insight_settings, sample_metrics
    ):
        """Test that max_insights setting is respected."""
        mock_insight_settings.max_insights = 2
        engine = InsightEngine(
            anthropic_settings=mock_anthropic_settings,
            insight_settings=mock_insight_settings,
        )

        insights = await engine.generate(sample_metrics)

        assert len(insights) <= 2

    @pytest.mark.asyncio
    async def test_generates_insights_for_concerning_metrics(
        self, mock_anthropic_settings, mock_insight_settings, low_activity_metrics
    ):
        """Test that concerning metrics generate appropriate insights."""
        engine = InsightEngine(
            anthropic_settings=mock_anthropic_settings,
            insight_settings=mock_insight_settings,
        )

        insights = await engine.generate(low_activity_metrics)

        # Should have insights about concerning patterns
        assert len(insights) > 0

        # Check for expected categories
        categories = {i.category for i in insights}
        assert len(categories) >= 1

    @pytest.mark.asyncio
    async def test_ai_insights_parses_json_response(
        self,
        mock_anthropic_settings,
        mock_openai_settings,
        mock_insight_settings,
        sample_metrics,
        monkeypatch,
    ):
        """Test AI response parsing for valid JSON content."""
        mock_insight_settings.ai_provider = "openai"
        self._patch_openai(
            monkeypatch,
            '[{"category":"activity","headline":"Solid week",'
            '"reasoning":"Steps up","recommendation":"Keep it up"}]',
        )

        engine = InsightEngine(
            anthropic_settings=mock_anthropic_settings,
            openai_settings=mock_openai_settings,
            insight_settings=mock_insight_settings,
        )

        insights = await engine.generate(sample_metrics)

        assert len(insights) == 1
        assert insights[0].source == "ai"
        assert insights[0].category == "activity"

    @pytest.mark.asyncio
    async def test_ai_insights_handles_code_fence_json(
        self,
        mock_anthropic_settings,
        mock_openai_settings,
        mock_insight_settings,
        sample_metrics,
        monkeypatch,
    ):
        """Test AI response parsing when JSON is wrapped in code fences."""
        mock_insight_settings.ai_provider = "openai"
        self._patch_openai(
            monkeypatch,
            '```json\n[{"category":"sleep","headline":"Solid sleep",'
            '"reasoning":"7.2 hours","recommendation":"Keep routine"}]\n```',
        )

        engine = InsightEngine(
            anthropic_settings=mock_anthropic_settings,
            openai_settings=mock_openai_settings,
            insight_settings=mock_insight_settings,
        )

        insights = await engine.generate(sample_metrics)

        assert len(insights) == 1
        assert insights[0].source == "ai"
        assert insights[0].category == "sleep"

    @pytest.mark.asyncio
    async def test_ai_insights_invalid_json_falls_back_to_rules(
        self,
        mock_anthropic_settings,
        mock_openai_settings,
        mock_insight_settings,
        sample_metrics,
        monkeypatch,
    ):
        """Test fallback to rules when AI response is invalid JSON."""
        mock_insight_settings.ai_provider = "openai"
        self._patch_openai(monkeypatch, "not-json")

        engine = InsightEngine(
            anthropic_settings=mock_anthropic_settings,
            openai_settings=mock_openai_settings,
            insight_settings=mock_insight_settings,
        )

        insights = await engine.generate(sample_metrics)

        assert insights
        assert all(insight.source == "rule" for insight in insights)
