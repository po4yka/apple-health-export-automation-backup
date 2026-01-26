"""Report delivery via Clawdbot gateway."""

import asyncio
from datetime import UTC, datetime

import httpx
import structlog

from ..config import ClawdbotSettings
from .models import DeliveryResult

logger = structlog.get_logger(__name__)


class DeliveryAuthError(Exception):
    """Raised when Clawdbot authentication fails."""

    pass


class ClawdbotDelivery:
    """Delivers reports to Telegram via Clawdbot gateway."""

    def __init__(self, settings: ClawdbotSettings) -> None:
        """Initialize the delivery client.

        Args:
            settings: Clawdbot gateway settings.
        """
        self._settings = settings

    async def send_report(self, report: str, week_id: str | None = None) -> DeliveryResult:
        """Send a formatted report to Telegram via Clawdbot.

        Uses the /hooks/agent endpoint with deliver=true for direct sending.

        Args:
            report: Formatted report message.
            week_id: Optional week identifier for session key.

        Returns:
            DeliveryResult with success status.
        """
        if not self._settings.hooks_token:
            logger.error("clawdbot_no_token")
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

        for attempt in range(1, self._settings.max_retries + 1):
            try:
                result = await self._attempt_send(payload, attempt)
                if result.success:
                    return result

                # Check for auth errors - don't retry
                if result.error and "auth" in result.error.lower():
                    logger.error("clawdbot_auth_failed")
                    return result

            except DeliveryAuthError:
                logger.error("clawdbot_auth_failed")
                return DeliveryResult(
                    success=False,
                    attempt=attempt,
                    error="Authentication failed",
                )
            except Exception as e:
                logger.warning(
                    "delivery_attempt_failed",
                    attempt=attempt,
                    error=str(e),
                    error_type=type(e).__name__,
                )

            # Wait before retry with exponential backoff
            if attempt < self._settings.max_retries:
                delay = self._settings.retry_delay_seconds * (2 ** (attempt - 1))
                logger.debug("delivery_retry_wait", delay=delay, attempt=attempt)
                await asyncio.sleep(delay)

        # All retries exhausted
        logger.error(
            "delivery_failed_final",
            attempts=self._settings.max_retries,
            report_length=len(report),
        )
        return DeliveryResult(
            success=False,
            attempt=self._settings.max_retries,
            error="All delivery attempts failed",
        )

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
        """Check if Clawdbot gateway is reachable.

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
