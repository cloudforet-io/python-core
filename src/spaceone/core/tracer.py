from opentelemetry import trace
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


# class Tracer(object):
#     def __init__(self, service_name):
#         self.service_name = service_name
#         self.tracer = self.init_tracer()
#
#     def init_tracer(self):
#         resource = Resource(attributes={
#             SERVICE_NAME: self.service_name
#         })
#         provider = TracerProvider(resource=resource)
#         processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="https://otel.doodle-dev.spaceone.dev"))
#         provider.add_span_processor(processor)
#         trace.set_tracer_provider(provider)
#         tracer = trace.get_tracer(__name__)
#         return tracer


def init_tracer(service_name):
    resource = Resource(attributes={
        SERVICE_NAME: service_name
    })
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="https://otel.doodle-dev.spaceone.dev"))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer(__name__)
    return tracer

tracer = init_tracer(__name__)
# resource = Resource(attributes={
#     SERVICE_NAME: "core"
# })
# provider = TracerProvider(resource=resource)
# processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="https://otel.doodle-dev.spaceone.dev"))
# provider.add_span_processor(processor)
# trace.set_tracer_provider(provider)
# tracer = trace.get_tracer(__name__)
