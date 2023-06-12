from opentelemetry import metrics
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

from spaceone.core import config

__all__ = ['set_metric']


def set_metric():
    # Not Implemented
    # service = config.get_service()
    # endpoint = config.get_global('OTEL.endpoint')
    # _init_metric(service, endpoint)
    pass


def _init_metric(service, endpoint):
    resource = Resource(attributes={
        SERVICE_NAME: service
    })

    exporter = OTLPMetricExporter(endpoint=endpoint)
    reader = PeriodicExportingMetricReader(exporter)
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)
