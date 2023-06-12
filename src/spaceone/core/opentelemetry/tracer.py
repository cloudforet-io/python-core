import logging
from opentelemetry import trace
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from spaceone.core import config

__all__ = ['set_tracer']

_LOGGER = logging.getLogger(__name__)


def set_tracer():
    service = config.get_service()
    endpoint = config.get_global('OTEL', {}).get('endpoint')
    if endpoint is None:
        _LOGGER.info(f'[set_tracer] OTEL endpoint is not configured. (service={service})')
    _init_tracer(service, endpoint)


def _init_tracer(service, endpoint):
    resource = Resource(attributes={
        SERVICE_NAME: service
    })
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
