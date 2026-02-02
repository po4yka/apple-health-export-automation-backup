"""Report delivery via OpenClaw gateway."""

from datetime import UTC, datetime

import httpx
import structlog
from tenacity import (
    AsyncRetrying,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..config import OpenClawSettings
from ..metrics import REPORT_DELIVERIES
from .models import DeliveryResult

logger = structlog.get_logger(__name__)


class DeliveryAuthError(Exception):
    """Raised when OpenClaw authentication fails."""

    pass


class OpenClawDelivery:
    """Delivers reports to Telegram via OpenClaw gateway."""

    def __init__(self, settings: OpenClawSettings) -> None:
        """Initialize the delivery client.

        Args:
            settings: OpenClaw gateway settings.
        """
        self._settings = settings

    async def send_report(self, report: str, week_id: str | None = None) -> DeliveryResult:
        """Send a formatted report to Telegram via OpenClaw.

        Uses the /hooks/agent endpoint with deliver=true for direct sending.

        Args:
            report: Formatted report message.
            week_id: Optional week identifier for session key.

        Returns:
            DeliveryResult with success status.
        """
        if not self._settings.hooks_token:
            logger.error("openclaw_no_token")
            return DeliveryResult(
                success=False,
                attempt=0,
                error="No hooks token configured",
            )

        if week_id is None:
            week_id = datetime.now(UTC).strftime("%Y-W%W")

        payload = {
            "message": report,
            "channel": "telegram",
            "to": str(self._settings.telegram_user_id),
            "deliver": True,  # Direct delivery, no AI processing
            "name": "Weekly Health Report",
            "sessionKey": f"health-report:{week_id}",
        }

        attempt = 0
        try:
            result = await self._send_with_retries(payload)
            REPORT_DELIVERIES.labels(status="success").inc()
            return result
        except DeliveryAuthError:
            logger.error("openclaw_auth_failed")
            REPORT_DELIVERIES.labels(status="auth_error").inc()
            return DeliveryResult(
                success=False,
                attempt=attempt,
                error="Authentication failed",
            )
        except Exception as e:
            logger.error(
                "delivery_failed_final",
                attempts=self._settings.max_retries,
                report_length=len(report),
                error=str(e),
            )
            REPORT_DELIVERIES.labels(status="failed").inc()
            return DeliveryResult(
                success=False,
                attempt=self._settings.max_retries,
                error="All delivery attempts failed",
            )

    async def _send_with_retries(self, payload: dict) -> DeliveryResult:
        """Send with tenacity-managed retries.

        Retry logic is configured dynamically from settings to allow
        tenacity's decorator-style to work with instance settings.
        """
        attempt = 0
        async for attempt_state in AsyncRetrying(
            stop=stop_after_attempt(self._settings.max_retries),
            wait=wait_exponential(
                multiplier=self._settings.retry_delay_seconds,
                min=self._settings.retry_delay_seconds,
                max=60,
            ),
            retry=retry_if_not_exception_type(DeliveryAuthError),
            reraise=True,
        ):
            with attempt_state:
                attempt = attempt_state.retry_state.attempt_number
                result = await self._attempt_send(payload, attempt)
                if not result.success:
                    # Check for auth errors - don't retry
                    if result.error and "auth" in result.error.lower():
                        raise DeliveryAuthError(result.error)
                    raise RuntimeError(result.error or "Delivery failed")
                return result
        # Should not reach here, but satisfy type checker
        return DeliveryResult(success=False, attempt=attempt, error="Retries exhausted")

    async def _attempt_send(self, payload: dict, attempt: int) -> DeliveryResult:
        """Attempt a single delivery.

        Args:
            payload: Request payload.
            attempt: Current attempt number.

        Returns:
            DeliveryResult for this attempt.

        Raises:
            DeliveryAuthError: If authentication fails.
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self._settings.gateway_url}/hooks/agent",
                json=payload,
                headers={
                    "Authorization": f"Bearer {self._settings.hooks_token}",
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )

            if response.status_code == 202:
                # Accepted
                result = response.json()
                run_id = result.get("runId")
                logger.info(
                    "delivery_accepted",
                    run_id=run_id,
                    attempt=attempt,
                )
                return DeliveryResult(
                    success=True,
                    attempt=attempt,
                    run_id=run_id,
                )

            if response.status_code in (401, 403):
                raise DeliveryAuthError(f"Auth error: {response.status_code}")

            if response.status_code == 200:
                # Direct success (some endpoints return 200)
                result = response.json()
                logger.info("delivery_success", attempt=attempt)
                return DeliveryResult(
                    success=True,
                    attempt=attempt,
                    run_id=result.get("runId"),
                )

            # Other errors
            error_msg = f"HTTP {response.status_code}"
            try:
                error_data = response.json()
                if "error" in error_data:
                    error_msg = f"{error_msg}: {error_data['error']}"
            except Exception:
                pass

            logger.warning(
                "delivery_http_error",
                status=response.status_code,
                attempt=attempt,
            )
            return DeliveryResult(
                success=False,
                attempt=attempt,
                error=error_msg,
            )

    async def health_check(self) -> bool:
        """Check if OpenClaw gateway is reachable.

        Returns:
            True if gateway responds, False otherwise.
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self._settings.gateway_url}/health",
                    timeout=5.0,
                )
                return response.status_code == 200
        except Exception:
            return False
