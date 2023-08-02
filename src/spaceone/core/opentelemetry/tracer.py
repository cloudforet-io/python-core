import logging
from opentelemetry import trace
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from spaceone.core import config

__all__ = ['set_tracer']

_LOGGER = logging.getLogger(__name__)


def set_tracer():
    service = config.get_service()
    endpoint = config.get_global('OTEL', {}).get('endpoint')

    resource = Resource(attributes={
        SERVICE_NAME: service
    })
    provider = TracerProvider(resource=resource)

    if endpoint:
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    else:
        processor = BatchSpanProcessor(SpanExporter())

    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
