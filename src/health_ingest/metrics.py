"""Prometheus metrics definitions for the health ingestion service."""

from prometheus_client import Counter, Gauge, Histogram, Info

# -- Service info --
SERVICE_INFO = Info("health_ingest", "Health ingestion service info")

# -- Message processing --
MESSAGES_PROCESSED = Counter(
    "health_ingest_messages_processed_total",
    "Total messages processed",
)
MESSAGES_FAILED = Counter(
    "health_ingest_messages_failed_total",
    "Total messages that failed processing",
    ["error_type"],
)
DUPLICATES_FILTERED = Counter(
    "health_ingest_duplicates_filtered_total",
    "Total duplicate points filtered",
)

# -- Queue --
QUEUE_DEPTH = Gauge(
    "health_ingest_queue_depth",
    "Current message queue depth",
)

# -- InfluxDB writes --
INFLUX_WRITES = Counter(
    "health_ingest_influx_writes_total",
    "Total InfluxDB write operations",
    ["status"],
)
INFLUX_POINTS_WRITTEN = Counter(
    "health_ingest_influx_points_written_total",
    "Total points written to InfluxDB",
)
INFLUX_POINTS_DROPPED = Counter(
    "health_ingest_influx_points_dropped_total",
    "Total points dropped due to errors or buffer overflow",
    ["reason"],
)
INFLUX_WRITE_DURATION = Histogram(
    "health_ingest_influx_write_duration_seconds",
    "InfluxDB write latency",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
INFLUX_BUFFER_SIZE = Gauge(
    "health_ingest_influx_buffer_size",
    "Current InfluxDB write buffer size",
)

# -- Dedup cache --
DEDUP_CACHE_SIZE = Gauge(
    "health_ingest_dedup_cache_size",
    "Current dedup cache entries",
)
DEDUP_HITS = Counter(
    "health_ingest_dedup_hits_total",
    "Total dedup cache hits",
)
DEDUP_MISSES = Counter(
    "health_ingest_dedup_misses_total",
    "Total dedup cache misses",
)
DEDUP_EVICTIONS = Counter(
    "health_ingest_dedup_evictions_total",
    "Total dedup cache evictions",
)

# -- DLQ --
DLQ_ENTRIES = Counter(
    "health_ingest_dlq_entries_total",
    "Total DLQ entries added",
    ["category"],
)

# -- Circuit breakers --
CIRCUIT_BREAKER_STATE = Gauge(
    "health_ingest_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half_open)",
    ["name"],
)
CIRCUIT_BREAKER_TRIPS = Gauge(
    "health_ingest_circuit_breaker_trips_total",
    "Total circuit breaker trips",
    ["name"],
)

# -- HTTP --
HTTP_REQUESTS_TOTAL = Counter(
    "health_ingest_http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)

# -- Report delivery --
REPORT_DELIVERIES = Counter(
    "health_ingest_report_deliveries_total",
    "Total report delivery attempts",
    ["status"],
)
