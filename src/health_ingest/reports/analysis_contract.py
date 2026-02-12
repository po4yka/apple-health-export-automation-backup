"""Analysis request profiles and prompt/template version contracts."""

import hashlib
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from pathlib import Path
from string import Formatter


class AnalysisRequestType(str, Enum):
    """Supported analysis request types."""

    WEEKLY_SUMMARY = "weekly_summary"
    DAILY_MORNING_BRIEF = "daily_morning_brief"
    DAILY_EVENING_RECAP = "daily_evening_recap"
    BOT_COMMAND_INSIGHT = "bot_command_insight"


@dataclass(frozen=True)
class PromptSpec:
    """Prompt specification with immutable version contract."""

    prompt_id: str
    version: str
    filename: str
    required_placeholders: tuple[str, ...]


@dataclass(frozen=True)
class PromptTemplate:
    """Loaded prompt template with digest."""

    prompt_id: str
    version: str
    path: Path
    text: str
    sha256: str

    @property
    def short_hash(self) -> str:
        """Return a short hash for compact footer/log usage."""
        return self.sha256[:12]


@dataclass(frozen=True)
class AnalysisRequestProfile:
    """Concrete analysis request configuration."""

    request_type: AnalysisRequestType
    objective: str
    expected_outcome: str
    prompt_id: str
    default_max_insights: int


@dataclass(frozen=True)
class AnalysisProvenance:
    """Version metadata describing why a generated output looks the way it does."""

    request_type: str
    source: str
    provider: str
    model: str
    dataset_version: str
    prompt_id: str
    prompt_version: str
    prompt_hash: str
    report_template_version: str | None = None

    def with_template_version(self, template_version: str) -> "AnalysisProvenance":
        """Return a copy with report template version populated."""
        return AnalysisProvenance(
            request_type=self.request_type,
            source=self.source,
            provider=self.provider,
            model=self.model,
            dataset_version=self.dataset_version,
            prompt_id=self.prompt_id,
            prompt_version=self.prompt_version,
            prompt_hash=self.prompt_hash,
            report_template_version=template_version,
        )


PROMPT_SPECS: dict[str, PromptSpec] = {
    "weekly_insight": PromptSpec(
        prompt_id="weekly_insight",
        version="v1",
        filename="weekly_insight_v1.md",
        required_placeholders=(
            "analysis_objective",
            "expected_outcome",
            "dataset_version",
            "metrics_text",
            "max_insights",
        ),
    ),
    "daily_morning": PromptSpec(
        prompt_id="daily_morning",
        version="v1",
        filename="daily_morning_v1.md",
        required_placeholders=(
            "analysis_objective",
            "expected_outcome",
            "dataset_version",
            "metrics_text",
            "max_insights",
        ),
    ),
    "daily_evening": PromptSpec(
        prompt_id="daily_evening",
        version="v1",
        filename="daily_evening_v1.md",
        required_placeholders=(
            "analysis_objective",
            "expected_outcome",
            "dataset_version",
            "metrics_text",
            "max_insights",
        ),
    ),
    "bot_command": PromptSpec(
        prompt_id="bot_command",
        version="v1",
        filename="bot_command_v1.md",
        required_placeholders=(
            "analysis_objective",
            "expected_outcome",
            "dataset_version",
            "data_text",
            "command",
            "max_insights",
        ),
    ),
}


ANALYSIS_PROFILES: dict[AnalysisRequestType, AnalysisRequestProfile] = {
    AnalysisRequestType.WEEKLY_SUMMARY: AnalysisRequestProfile(
        request_type=AnalysisRequestType.WEEKLY_SUMMARY,
        objective="Summarize weekly health patterns from aggregated metrics.",
        expected_outcome=(
            "Facts-first insights with explicit numeric reasoning and one actionable next step."
        ),
        prompt_id="weekly_insight",
        default_max_insights=4,
    ),
    AnalysisRequestType.DAILY_MORNING_BRIEF: AnalysisRequestProfile(
        request_type=AnalysisRequestType.DAILY_MORNING_BRIEF,
        objective="Provide a concise morning readiness brief based on sleep and vitals.",
        expected_outcome="Top recovery observations first, then practical actions for today.",
        prompt_id="daily_morning",
        default_max_insights=3,
    ),
    AnalysisRequestType.DAILY_EVENING_RECAP: AnalysisRequestProfile(
        request_type=AnalysisRequestType.DAILY_EVENING_RECAP,
        objective="Provide a concise evening recap of activity and recovery.",
        expected_outcome="What stood out today first, then specific recovery actions for tonight.",
        prompt_id="daily_evening",
        default_max_insights=3,
    ),
    AnalysisRequestType.BOT_COMMAND_INSIGHT: AnalysisRequestProfile(
        request_type=AnalysisRequestType.BOT_COMMAND_INSIGHT,
        objective="Answer the exact user command context with concise, actionable insights.",
        expected_outcome="Short command-specific insights grounded in provided values.",
        prompt_id="bot_command",
        default_max_insights=2,
    ),
}

_PROMPTS_DIR = Path(__file__).parent / "prompts"
_FORMATTER = Formatter()


def get_analysis_profile(request_type: AnalysisRequestType) -> AnalysisRequestProfile:
    """Return the analysis request profile."""
    return ANALYSIS_PROFILES[request_type]


def dataset_version_for_text(text: str) -> str:
    """Build a stable dataset version from normalized summary text."""
    normalized = "\n".join(line.rstrip() for line in text.strip().splitlines())
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"sha256:{digest[:16]}"


def _validate_placeholders(prompt_text: str, required: tuple[str, ...]) -> None:
    found = {
        field_name
        for _, field_name, _, _ in _FORMATTER.parse(prompt_text)
        if field_name is not None
    }
    missing = [name for name in required if name not in found]
    if missing:
        msg = ", ".join(missing)
        raise ValueError(f"Prompt missing required placeholders: {msg}")


@lru_cache(maxsize=16)
def load_prompt_template(prompt_id: str) -> PromptTemplate:
    """Load prompt template from versioned file and validate placeholders."""
    spec = PROMPT_SPECS[prompt_id]
    path = _PROMPTS_DIR / spec.filename
    text = path.read_text(encoding="utf-8").strip()
    _validate_placeholders(text, spec.required_placeholders)
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return PromptTemplate(
        prompt_id=spec.prompt_id,
        version=spec.version,
        path=path,
        text=text,
        sha256=digest,
    )

