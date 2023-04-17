from opentelemetry import trace
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from spaceone.core.config.default_conf import OTEL_COLLECTOR_ENDPOINT

__all__ = ['set_tracer']


def set_tracer(service):
    _init_tracer(service)


def _init_tracer(service):
    resource = Resource(attributes={
        SERVICE_NAME: service
    })
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_COLLECTOR_ENDPOINT))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
