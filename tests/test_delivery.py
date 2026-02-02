"""Tests for report delivery via OpenClaw."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from health_ingest.config import OpenClawSettings
from health_ingest.reports.delivery import OpenClawDelivery
from health_ingest.reports.formatter import TelegramFormatter
from health_ingest.reports.models import InsightResult, PrivacySafeMetrics


@pytest.fixture
def openclaw_settings():
    """Create test OpenClaw settings."""
    return OpenClawSettings(
        enabled=True,
        gateway_url="http://localhost:18789",
        hooks_token="test-token",
        telegram_user_id=12345,
        max_retries=3,
        retry_delay_seconds=0.1,  # Fast retries for tests
    )


@pytest.fixture
def delivery(openclaw_settings):
    """Create OpenClawDelivery instance."""
    return OpenClawDelivery(openclaw_settings)


@pytest.fixture
def sample_report():
    """Create a sample report message."""
    return """*Weekly Health Report*
Jan 20 - Jan 26

*Quick Stats*
Steps: 9,500/day (+12%)
Sleep: 7.2h avg (stable)
Exercise: 180 min (+15%)

*Insights*

1. *Great activity improvement*
   Steps increased 12% from last week.
   Keep up the momentum!

_RULE-generated insights_"""


class TestOpenClawDelivery:
    """Tests for OpenClawDelivery class."""

    @pytest.mark.asyncio
    async def test_send_report_success(self, delivery, sample_report):
        """Test successful report delivery."""
        mock_response = Mock()
        mock_response.status_code = 202
        mock_response.json.return_value = {"runId": "abc123"}

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                return_value=mock_response
            )

            result = await delivery.send_report(sample_report)

        assert result.success is True
        assert result.run_id == "abc123"
        assert result.attempt == 1

    @pytest.mark.asyncio
    async def test_send_report_auth_failure(self, delivery, sample_report):
        """Test authentication failure handling."""
        mock_response = Mock()
        mock_response.status_code = 401

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                return_value=mock_response
            )

            result = await delivery.send_report(sample_report)

        assert result.success is False
        assert "auth" in result.error.lower() or result.error == "Authentication failed"

    @pytest.mark.asyncio
    async def test_send_report_retry_on_failure(self, delivery, sample_report):
        """Test retry logic on transient failures."""
        mock_fail = Mock()
        mock_fail.status_code = 500
        mock_fail.json.return_value = {"error": "Server error"}

        mock_success = Mock()
        mock_success.status_code = 202
        mock_success.json.return_value = {"runId": "retry123"}

        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return mock_fail
            return mock_success

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = mock_post

            result = await delivery.send_report(sample_report)

        assert result.success is True
        assert call_count == 3
        assert result.attempt == 3

    @pytest.mark.asyncio
    async def test_send_report_all_retries_exhausted(self, delivery, sample_report):
        """Test behavior when all retries fail."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"error": "Persistent error"}

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                return_value=mock_response
            )

            result = await delivery.send_report(sample_report)

        assert result.success is False
        assert result.attempt == 3  # max_retries

    @pytest.mark.asyncio
    async def test_send_report_no_token(self, sample_report):
        """Test failure when no token configured."""
        settings = OpenClawSettings(hooks_token=None)
        delivery = OpenClawDelivery(settings)

        result = await delivery.send_report(sample_report)

        assert result.success is False
        assert "token" in result.error.lower()

    @pytest.mark.asyncio
    async def test_send_report_with_week_id(self, delivery, sample_report):
        """Test that week_id is included in session key."""
        mock_response = Mock()
        mock_response.status_code = 202
        mock_response.json.return_value = {"runId": "test123"}

        captured_payload = None

        async def capture_post(url, json, **kwargs):
            nonlocal captured_payload
            captured_payload = json
            return mock_response

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.post = capture_post

            await delivery.send_report(sample_report, week_id="2024-W04")

        assert captured_payload is not None
        assert captured_payload["sessionKey"] == "health-report:2024-W04"

    @pytest.mark.asyncio
    async def test_health_check_success(self, delivery):
        """Test health check when gateway is healthy."""
        mock_response = Mock()
        mock_response.status_code = 200

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )

            healthy = await delivery.health_check()

        assert healthy is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self, delivery):
        """Test health check when gateway is down."""
        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=Exception("Connection refused")
            )

            healthy = await delivery.health_check()

        assert healthy is False


class TestTelegramFormatter:
    """Tests for TelegramFormatter class."""

    @pytest.fixture
    def formatter(self):
        """Create formatter instance."""
        return TelegramFormatter()

    @pytest.fixture
    def sample_metrics(self):
        """Create sample metrics."""
        return PrivacySafeMetrics(
            avg_daily_steps=9500,
            steps_change_pct=12.5,
            total_exercise_min=180,
            exercise_change_pct=15.0,
            avg_resting_hr=62.0,
            avg_hrv=48.0,
            hrv_change_pct=8.0,
            avg_duration_hours=7.2,
            sleep_change_pct=-3.0,
            workout_count=4,
            weight_kg=75.5,
            weight_change_kg=-0.3,
        )

    @pytest.fixture
    def sample_insights(self):
        """Create sample insights."""
        return [
            InsightResult(
                category="activity",
                headline="Great improvement in activity",
                reasoning="Steps increased 12.5% from last week.",
                recommendation="Keep up the momentum with consistent daily walks.",
                confidence=0.85,
                source="rule",
            ),
            InsightResult(
                category="heart",
                headline="HRV trending upward",
                reasoning="HRV improved 8% indicating good recovery.",
                recommendation="Maintain current balance of activity and rest.",
                confidence=0.8,
                source="rule",
            ),
        ]

    def test_format_includes_header(self, formatter, sample_metrics, sample_insights):
        """Test that formatted report includes header."""
        from datetime import datetime

        week_start = datetime(2024, 1, 20)
        week_end = datetime(2024, 1, 26)

        report = formatter.format(sample_metrics, sample_insights, week_start, week_end)

        assert "*Weekly Health Report*" in report
        assert "Jan 20 - Jan 26" in report

    def test_format_includes_quick_stats(self, formatter, sample_metrics, sample_insights):
        """Test that formatted report includes quick stats."""
        from datetime import datetime

        week_start = datetime(2024, 1, 20)
        week_end = datetime(2024, 1, 26)

        report = formatter.format(sample_metrics, sample_insights, week_start, week_end)

        assert "*Quick Stats*" in report
        assert "9,500" in report  # steps
        assert "7.2h" in report  # sleep
        assert "180 min" in report  # exercise

    def test_format_includes_insights(self, formatter, sample_metrics, sample_insights):
        """Test that formatted report includes insights."""
        from datetime import datetime

        week_start = datetime(2024, 1, 20)
        week_end = datetime(2024, 1, 26)

        report = formatter.format(sample_metrics, sample_insights, week_start, week_end)

        assert "*Insights*" in report
        assert "Great improvement" in report
        assert "HRV trending" in report

    def test_format_includes_source_footer(self, formatter, sample_metrics, sample_insights):
        """Test that formatted report includes source indicator."""
        from datetime import datetime

        week_start = datetime(2024, 1, 20)
        week_end = datetime(2024, 1, 26)

        report = formatter.format(sample_metrics, sample_insights, week_start, week_end)

        assert "RULE-generated" in report

    def test_format_respects_max_length(self, formatter, sample_metrics):
        """Test that report is truncated if too long."""
        from datetime import datetime

        # Create many long insights
        long_insights = [
            InsightResult(
                category="test",
                headline="Test headline " * 5,
                reasoning="Long reasoning text. " * 50,
                recommendation="Long recommendation. " * 30,
                confidence=0.5,
                source="rule",
            )
            for _ in range(10)
        ]

        week_start = datetime(2024, 1, 20)
        week_end = datetime(2024, 1, 26)

        report = formatter.format(sample_metrics, long_insights, week_start, week_end)

        assert len(report) <= formatter.MAX_MESSAGE_LENGTH

    def test_format_no_insights(self, formatter, sample_metrics):
        """Test formatting with no insights."""
        from datetime import datetime

        week_start = datetime(2024, 1, 20)
        week_end = datetime(2024, 1, 26)

        report = formatter.format(sample_metrics, [], week_start, week_end)

        assert "No significant patterns" in report

    def test_trend_indicator_positive(self, formatter):
        """Test trend indicator for positive change."""
        indicator = formatter._trend_indicator(15.0)
        assert "+15%" in indicator

    def test_trend_indicator_negative(self, formatter):
        """Test trend indicator for negative change."""
        indicator = formatter._trend_indicator(-10.0)
        assert "-10%" in indicator

    def test_trend_indicator_stable(self, formatter):
        """Test trend indicator for stable."""
        indicator = formatter._trend_indicator(2.0)
        assert "stable" in indicator

    def test_trend_indicator_none(self, formatter):
        """Test trend indicator when no data."""
        indicator = formatter._trend_indicator(None)
        assert indicator == ""

    def test_format_error(self, formatter):
        """Test error message formatting."""
        error_msg = formatter.format_error("Connection timeout")

        assert "*Weekly Health Report*" in error_msg
        assert "Connection timeout" in error_msg
