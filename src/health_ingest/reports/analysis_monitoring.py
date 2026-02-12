"""Unified analysis monitoring across cost, latency, and quality."""

import structlog

from ..metrics import (
    ANALYSIS_COST_USD,
    ANALYSIS_LATENCY_SECONDS,
    ANALYSIS_QUALITY_SCORE,
    ANALYSIS_RUNS,
)
from .analysis_contract import AnalysisProvenance

logger = structlog.get_logger(__name__)


def record_analysis_observation(
    *,
    provenance: AnalysisProvenance,
    status: str,
    latency_seconds: float,
    estimated_cost_usd: float | None,
    quality_score: float,
) -> None:
    """Record cost, latency, and quality for one analysis run."""
    request_type = provenance.request_type
    source = provenance.source
    provider = provenance.provider
    cost = max(estimated_cost_usd or 0.0, 0.0)
    clamped_quality = min(max(quality_score, 0.0), 1.0)
    clamped_latency = max(latency_seconds, 0.0)

    ANALYSIS_RUNS.labels(
        request_type=request_type,
        source=source,
        provider=provider,
        status=status,
    ).inc()
    ANALYSIS_LATENCY_SECONDS.labels(
        request_type=request_type,
        source=source,
        provider=provider,
    ).observe(clamped_latency)
    ANALYSIS_COST_USD.labels(
        request_type=request_type,
        source=source,
        provider=provider,
    ).observe(cost)
    ANALYSIS_QUALITY_SCORE.labels(
        request_type=request_type,
        source=source,
        provider=provider,
    ).observe(clamped_quality)

    logger.info(
        "analysis_observation",
        request_type=request_type,
        source=source,
        provider=provider,
        status=status,
        model=provenance.model,
        prompt_id=provenance.prompt_id,
        prompt_version=provenance.prompt_version,
        prompt_hash=provenance.prompt_hash,
        dataset_version=provenance.dataset_version,
        report_template_version=provenance.report_template_version,
        latency_ms=round(clamped_latency * 1000, 2),
        estimated_cost_usd=round(cost, 6),
        quality_score=round(clamped_quality, 3),
    )

