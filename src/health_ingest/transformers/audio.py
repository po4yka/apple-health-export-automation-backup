"""Audio exposure transformer."""

from typing import Any

from influxdb_client import Point

from .base import BaseTransformer, HealthMetric

# Metrics that map to audio measurement
AUDIO_METRICS = {
    "headphone_audio_exposure": "headphone_db",
    "headphoneAudioExposure": "headphone_db",
    "environmental_audio_exposure": "environmental_db",
    "environmentalAudioExposure": "environmental_db",
    "headphone_audio_levels": "headphone_db",
    "headphoneAudioLevels": "headphone_db",
}


class AudioTransformer(BaseTransformer):
    """Transformer for audio exposure metrics."""

    measurement = "audio"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is an audio exposure metric."""
        lower = metric_name.lower()
        return lower in {k.lower() for k in AUDIO_METRICS} or any(
            keyword in lower
            for keyword in [
                "audio_exposure",
                "audio_levels",
                "headphone_audio",
                "environmental_audio",
            ]
        )

    def transform(self, data: dict[str, Any]) -> list[Point]:
        """Transform audio exposure data to InfluxDB points."""
        points = []

        metrics = data.get("data", [data]) if "data" in data else [data]

        for item in metrics:
            try:
                metric = HealthMetric.model_validate(item)
                if metric.qty is None:
                    continue

                metric_name = metric.name.lower().replace(" ", "_")
                field_name = self._lookup_field(metric_name, AUDIO_METRICS)

                point = (
                    Point(self.measurement)
                    .tag("source", self._get_source(item))
                    .field(field_name, float(metric.qty))
                    .time(metric.date)
                )

                points.append(point)

            except Exception as e:
                self._log_transform_error(e, item)
                continue

        return points
