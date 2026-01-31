"""Shared type aliases and typed dictionaries."""

from __future__ import annotations

from typing import NotRequired, TypeAlias, TypedDict

JSONValue: TypeAlias = (
    str | int | float | bool | None | list["JSONValue"] | dict[str, "JSONValue"]
)
JSONObject: TypeAlias = dict[str, JSONValue]
TraceContextCarrier: TypeAlias = dict[str, str]


class InfluxStatus(TypedDict):
    """InfluxDB readiness status payload."""

    connected: bool
    running: bool
    circuit_state: str
    buffer_size: int


class QueueStatus(TypedDict):
    """Queue readiness status payload."""

    ready: bool
    size: int
    max_size: int


ComponentStatus: TypeAlias = InfluxStatus | QueueStatus | str | JSONObject
StatusComponents: TypeAlias = dict[str, ComponentStatus]


class ServiceStatusSnapshot(TypedDict):
    """Service readiness snapshot payload."""

    status: str
    components: StatusComponents


class HealthCheckStatus(TypedDict, total=False):
    """Service health check payload."""

    service: str
    messages_processed: int
    duplicates_filtered: int
    queue_size: int
    workers: int
    max_concurrent: int
    http: dict[str, bool]
    influxdb: JSONObject
    archive: JSONObject
    dedup: JSONObject
    dlq: JSONObject


class ErrorDetail(TypedDict):
    """Validation error detail shape from Pydantic."""

    loc: list[JSONValue]
    msg: str
    type: str
    input: NotRequired[JSONValue]
    ctx: NotRequired[JSONObject]
