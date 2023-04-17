from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

from spaceone.core.config.default_conf import OTEL_COLLECTOR_ENDPOINT

__all__ = ['set_metric']


def set_metric():
    _init_metric()


def _init_metric():
    exporter = OTLPMetricExporter(endpoint=OTEL_COLLECTOR_ENDPOINT)
    reader = PeriodicExportingMetricReader(exporter)
    provider = MeterProvider(metric_readers=[reader])
    set_meter_provider(provider)
