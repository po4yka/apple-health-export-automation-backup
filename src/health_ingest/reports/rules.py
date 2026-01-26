"""Rule-based insight generation for when AI is unavailable."""

from collections.abc import Callable
from dataclasses import dataclass

import structlog

from .models import InsightResult, PrivacySafeMetrics

logger = structlog.get_logger(__name__)


@dataclass
class Rule:
    """A single rule for generating insights."""

    name: str
    category: str
    condition: Callable[[PrivacySafeMetrics], bool]
    generate: Callable[[PrivacySafeMetrics], InsightResult]
    priority: int = 50  # Higher = more important


class RuleEngine:
    """Evaluates predefined rules to generate insights."""

    def __init__(self) -> None:
        """Initialize the rule engine with predefined rules."""
        self._rules = self._build_rules()

    def evaluate(self, metrics: PrivacySafeMetrics, max_insights: int = 5) -> list[InsightResult]:
        """Evaluate all rules and return matching insights.

        Args:
            metrics: Privacy-safe metrics to evaluate.
            max_insights: Maximum number of insights to return.

        Returns:
            List of InsightResult objects, sorted by priority.
        """
        insights: list[tuple[int, InsightResult]] = []

        for rule in self._rules:
            try:
                if rule.condition(metrics):
                    insight = rule.generate(metrics)
                    insights.append((rule.priority, insight))
                    logger.debug("rule_matched", rule=rule.name, category=rule.category)
            except Exception as e:
                logger.warning("rule_evaluation_failed", rule=rule.name, error=str(e))

        # Sort by priority (descending) and return top N
        insights.sort(key=lambda x: x[0], reverse=True)
        return [insight for _, insight in insights[:max_insights]]

    def _build_rules(self) -> list[Rule]:
        """Build the list of predefined rules."""
        rules = []

        # Activity Rules
        rules.append(
            Rule(
                name="steps_goal_achieved",
                category="activity",
                priority=70,
                condition=lambda m: m.avg_daily_steps >= 10000,
                generate=lambda m: InsightResult(
                    category="activity",
                    headline="Daily step goal achieved!",
                    reasoning=f"Averaged {m.avg_daily_steps:,} steps/day, exceeding the 10,000 step goal.",
                    recommendation="Maintain this excellent activity level. Consider adding variety with different walking routes.",
                    confidence=0.9,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="steps_nearly_achieved",
                category="activity",
                priority=60,
                condition=lambda m: 8000 <= m.avg_daily_steps < 10000,
                generate=lambda m: InsightResult(
                    category="activity",
                    headline="Close to step goal",
                    reasoning=f"Averaged {m.avg_daily_steps:,} steps/day, just {10000 - m.avg_daily_steps:,} short of 10K.",
                    recommendation="Add a short post-dinner walk to consistently hit your daily target.",
                    confidence=0.85,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="steps_significant_drop",
                category="activity",
                priority=75,
                condition=lambda m: (m.steps_change_pct or 0) < -20,
                generate=lambda m: InsightResult(
                    category="activity",
                    headline="Activity dropped significantly",
                    reasoning=f"Steps decreased {abs(m.steps_change_pct):.1f}% from last week.",
                    recommendation="Schedule short walking breaks throughout the day to rebuild momentum.",
                    confidence=0.85,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="steps_significant_increase",
                category="activity",
                priority=65,
                condition=lambda m: (m.steps_change_pct or 0) > 20,
                generate=lambda m: InsightResult(
                    category="activity",
                    headline="Great improvement in activity",
                    reasoning=f"Steps increased {m.steps_change_pct:.1f}% from last week.",
                    recommendation="Well done! Maintain this level while listening to your body for recovery needs.",
                    confidence=0.85,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="exercise_increased",
                category="activity",
                priority=60,
                condition=lambda m: (m.exercise_change_pct or 0) > 15,
                generate=lambda m: InsightResult(
                    category="activity",
                    headline="Exercise time increased",
                    reasoning=f"Exercise minutes up {m.exercise_change_pct:.1f}% from last week ({m.total_exercise_min} total minutes).",
                    recommendation="Great progress! Ensure adequate recovery between sessions.",
                    confidence=0.8,
                    source="rule",
                ),
            )
        )

        # Heart Rules
        rules.append(
            Rule(
                name="hrv_improving",
                category="heart",
                priority=70,
                condition=lambda m: m.hrv_trend == "improving" and (m.hrv_change_pct or 0) > 10,
                generate=lambda m: InsightResult(
                    category="heart",
                    headline="HRV shows strong recovery",
                    reasoning=f"HRV improved {m.hrv_change_pct:.1f}% (avg {m.avg_hrv:.0f}ms), indicating good stress adaptation.",
                    recommendation="Your body is recovering well. Maintain current activity balance.",
                    confidence=0.85,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="hrv_declining",
                category="heart",
                priority=80,
                condition=lambda m: m.hrv_trend == "declining" and (m.hrv_change_pct or 0) < -10,
                generate=lambda m: InsightResult(
                    category="heart",
                    headline="HRV indicates recovery stress",
                    reasoning=f"HRV decreased {abs(m.hrv_change_pct):.1f}%, suggesting accumulated stress or fatigue.",
                    recommendation="Consider lighter activity, more sleep, and stress management techniques.",
                    confidence=0.8,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="elevated_resting_hr",
                category="heart",
                priority=75,
                condition=lambda m: (m.avg_resting_hr or 0) > 75,
                generate=lambda m: InsightResult(
                    category="heart",
                    headline="Resting heart rate elevated",
                    reasoning=f"Average resting HR of {m.avg_resting_hr:.0f} bpm is above typical healthy range.",
                    recommendation="Focus on sleep quality, hydration, and stress reduction this week.",
                    confidence=0.8,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="good_resting_hr",
                category="heart",
                priority=50,
                condition=lambda m: m.avg_resting_hr is not None and 50 <= m.avg_resting_hr <= 65,
                generate=lambda m: InsightResult(
                    category="heart",
                    headline="Excellent resting heart rate",
                    reasoning=f"Average resting HR of {m.avg_resting_hr:.0f} bpm indicates strong cardiovascular fitness.",
                    recommendation="Keep up the good work with your current fitness routine.",
                    confidence=0.85,
                    source="rule",
                ),
            )
        )

        # Sleep Rules
        rules.append(
            Rule(
                name="sleep_deficit",
                category="sleep",
                priority=85,
                condition=lambda m: m.avg_duration_hours is not None and m.avg_duration_hours < 7,
                generate=lambda m: InsightResult(
                    category="sleep",
                    headline="Sleep duration below recommended",
                    reasoning=f"Averaging {m.avg_duration_hours:.1f} hours, below the 7-hour minimum for adults.",
                    recommendation="Aim for consistent bedtime 30 minutes earlier this week.",
                    confidence=0.9,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="excellent_sleep",
                category="sleep",
                priority=60,
                condition=lambda m: (
                    m.avg_duration_hours is not None
                    and m.avg_duration_hours >= 7.5
                    and (m.avg_quality_pct or 0) >= 85
                ),
                generate=lambda m: InsightResult(
                    category="sleep",
                    headline="Excellent sleep quality",
                    reasoning=f"Averaging {m.avg_duration_hours:.1f} hours with {m.avg_quality_pct:.0f}% quality score.",
                    recommendation="Maintain your current sleep routine - it's working well.",
                    confidence=0.85,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="sleep_dropped",
                category="sleep",
                priority=70,
                condition=lambda m: (m.sleep_change_pct or 0) < -15,
                generate=lambda m: InsightResult(
                    category="sleep",
                    headline="Sleep duration decreased",
                    reasoning=f"Sleep dropped {abs(m.sleep_change_pct):.1f}% from last week.",
                    recommendation="Prioritize sleep this week with a consistent wind-down routine.",
                    confidence=0.8,
                    source="rule",
                ),
            )
        )

        # Workout Rules
        rules.append(
            Rule(
                name="consistent_workouts",
                category="workouts",
                priority=55,
                condition=lambda m: m.workout_count >= 4,
                generate=lambda m: InsightResult(
                    category="workouts",
                    headline="Consistent workout routine",
                    reasoning=f"Completed {m.workout_count} workouts totaling {m.total_workout_duration_min} minutes.",
                    recommendation="Great consistency! Consider adding variety or progression to continue improving.",
                    confidence=0.8,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="no_workouts",
                category="workouts",
                priority=65,
                condition=lambda m: m.workout_count == 0 and m.total_exercise_min < 60,
                generate=lambda m: InsightResult(
                    category="workouts",
                    headline="No structured workouts recorded",
                    reasoning="No workout sessions logged this week with minimal exercise time.",
                    recommendation="Start with 2-3 short sessions this week, even 15-20 minutes helps.",
                    confidence=0.75,
                    source="rule",
                ),
            )
        )

        # Correlation Rules
        rules.append(
            Rule(
                name="exercise_hrv_positive",
                category="correlation",
                priority=80,
                condition=lambda m: (
                    (m.exercise_change_pct or 0) > 15 and (m.hrv_change_pct or 0) > 10
                ),
                generate=lambda m: InsightResult(
                    category="correlation",
                    headline="Exercise boosting recovery metrics",
                    reasoning=f"Exercise up {m.exercise_change_pct:.1f}% while HRV improved {m.hrv_change_pct:.1f}%.",
                    recommendation="Your body is adapting positively. Continue this gradual progression.",
                    confidence=0.75,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="sleep_hrv_declining",
                category="correlation",
                priority=85,
                condition=lambda m: (
                    (m.sleep_change_pct or 0) < -10 and (m.hrv_change_pct or 0) < -10
                ),
                generate=lambda m: InsightResult(
                    category="correlation",
                    headline="Sleep and recovery both declining",
                    reasoning=f"Sleep down {abs(m.sleep_change_pct):.1f}% and HRV down {abs(m.hrv_change_pct):.1f}%.",
                    recommendation="Focus on sleep quality first - it's likely the root cause of lower HRV.",
                    confidence=0.8,
                    source="rule",
                ),
            )
        )

        rules.append(
            Rule(
                name="weight_stable_active",
                category="correlation",
                priority=50,
                condition=lambda m: (
                    m.weight_change_kg is not None
                    and abs(m.weight_change_kg) < 0.5
                    and m.total_exercise_min > 150
                ),
                generate=lambda m: InsightResult(
                    category="correlation",
                    headline="Weight stable with good activity",
                    reasoning=f"Weight steady at {m.weight_kg:.1f}kg with {m.total_exercise_min} minutes of exercise.",
                    recommendation="Body composition may be improving even without scale changes.",
                    confidence=0.7,
                    source="rule",
                ),
            )
        )

        return rules
