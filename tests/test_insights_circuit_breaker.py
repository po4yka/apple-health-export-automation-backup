"""Tests for InsightEngine circuit breaker integration."""

import asyncio
from unittest.mock import AsyncMock, patch
import pytest

from health_ingest.reports.insights import InsightEngine
from health_ingest.reports.models import PrivacySafeMetrics
from health_ingest.circuit_breaker import CircuitOpenError, CircuitState
from health_ingest.config import AnthropicSettings, InsightSettings, OpenAISettings

@pytest.fixture
def mock_metrics():
    """Create sample metrics."""
    return PrivacySafeMetrics(
        avg_daily_steps=10000,
        avg_resting_hr=60,
    )

@pytest.fixture
def failing_openai_engine(mock_metrics):
    """Create an InsightEngine with a failing OpenAI provider."""
    insight_settings = InsightSettings(
        prefer_ai=True,
        ai_provider="openai",
        max_insights=1,
    )
    openai_settings = OpenAISettings(api_key="test-key")
    anthropic_settings = AnthropicSettings(api_key=None)

    return InsightEngine(
        anthropic_settings=anthropic_settings,
        insight_settings=insight_settings,
        openai_settings=openai_settings,
    )

@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_failures(failing_openai_engine, mock_metrics):
    """Test that consecutive failures trip the circuit breaker."""
    engine = failing_openai_engine
    
    # Configure the circuit breaker to fail fast for testing
    engine._circuit_breaker._failure_threshold = 2
    
    # Patch the _generate_ai_insights method to fail
    with patch.object(
        engine, 
        '_generate_ai_insights', 
        side_effect=Exception("API Error")
    ) as mock_generate:
        
        # 1st failure
        await engine.generate(mock_metrics)
        assert engine._circuit_breaker.state == CircuitState.CLOSED
        assert engine._circuit_breaker._failure_count == 1
        
        # 2nd failure - should trip
        await engine.generate(mock_metrics)
        assert engine._circuit_breaker.state == CircuitState.OPEN
        
        # 3rd attempt - should not call AI (circuit open)
        mock_generate.reset_mock()
        await engine.generate(mock_metrics)
        mock_generate.assert_not_called()
        
        # Should fall back to rules (source="rule")
        insights = await engine.generate(mock_metrics)
        assert len(insights) > 0
        assert all(i.source == "rule" for i in insights)

@pytest.mark.asyncio
async def test_circuit_breaker_half_open_recovery(failing_openai_engine, mock_metrics):
    """Test that circuit recovers after timeout."""
    engine = failing_openai_engine
    engine._circuit_breaker._failure_threshold = 1
    engine._circuit_breaker._recovery_timeout = 0.1 # Short timeout
    
    # 1. Trip the circuit
    with patch.object(engine, '_generate_ai_insights', side_effect=Exception("Fail")):
        await engine.generate(mock_metrics)
    
    assert engine._circuit_breaker.state == CircuitState.OPEN
    
    # 2. Wait for recovery timeout
    await asyncio.sleep(0.2)
    
    # 3. Next call should probe (half-open)
    # Mock success this time
    fake_insights = [{"category": "test", "headline": "h", "reasoning": "r", "recommendation": "rec"}]
    with patch.object(engine, '_generate_ai_insights', return_value=fake_insights) as mock_generate:
        result = await engine.generate(mock_metrics)
        
        # Should have called AI
        mock_generate.assert_called_once()
        
        # Should be closed now
        assert engine._circuit_breaker.state == CircuitState.CLOSED
        assert result == fake_insights
