import grpc
import inspect
import logging
import types
from collections.abc import Iterable

from google.protobuf.descriptor import ServiceDescriptor
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.empty_pb2 import Empty
from google.protobuf.struct_pb2 import Struct

from spaceone.core import config
from spaceone.core.error import *
from spaceone.core.locator import Locator

_LOGGER = logging.getLogger(__name__)


class BaseAPI(object):
    locator = Locator()

    pb2 = None
    pb2_grpc = None

    def __init__(self):
        self._desc_pool = self.pb2.DESCRIPTOR.pool
        self._grpc_messages = {}
        self._load_grpc_messages()
        self._check_variables()
        self._set_grpc_method()

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def pb2_grpc_module(self):
        return self.pb2_grpc

    @property
    def service_name(self):
        return self.pb2.DESCRIPTOR.services_by_name[self.__class__.__name__].full_name

    def _load_grpc_messages(self):
        service_desc: ServiceDescriptor = self._desc_pool.FindServiceByName(
            self.service_name
        )
        for method_desc in service_desc.methods:
            self._grpc_messages[method_desc.name] = {
                "request": method_desc.input_type.name,
                "response": method_desc.output_type.name,
            }

    def _check_variables(self):
        if not hasattr(self, "pb2"):
            raise Exception(
                f"gRPC Servicer has not set <pb2> variable. (servicer={self.__class__.__name__})"
            )

        if not hasattr(self, "pb2_grpc"):
            raise Exception(
                f"gRPC Servicer has not set <pb2_grpc> variable. (servicer={self.__class__.__name__})"
            )

    def _get_grpc_servicer(self):
        grpc_servicer = None
        for base_class in self.__class__.__bases__:
            if base_class.__module__ == self.pb2_grpc.__name__:
                grpc_servicer = base_class

        if grpc_servicer is None:
            raise Exception(
                f"gRPC servicer is not set. (servicer={self.__class__.__name__})"
            )

        return grpc_servicer

    def _set_grpc_method(self):
        grpc_servicer = self._get_grpc_servicer()

        for f_name, f_object in inspect.getmembers(
            self.__class__, predicate=inspect.isfunction
        ):
            if hasattr(grpc_servicer, f_name):
                setattr(self, f_name, self._grpc_method(f_object))

    @staticmethod
    def _error_method(error, context):
        if not isinstance(error, ERROR_BASE):
            error = ERROR_UNKNOWN(message=error)

        if not error.meta.get("skip_error_log"):
            _LOGGER.error(f"(Error) => {error.message} {error}", exc_info=True)

        details = f"{error.error_code}: {error.message}"
        context.abort(grpc.StatusCode[error.status_code], details)

    def _generate_response(self, response_iterator, context):
        try:
            for response in response_iterator:
                yield response

        except Exception as e:
            self._error_method(e, context)

    def _grpc_method(self, func):
        def wrapper(request_or_iterator, context):
            try:
                response_or_iterator = func(self, request_or_iterator, context)

                if isinstance(response_or_iterator, types.GeneratorType):
                    return self._generate_response(response_or_iterator, context)
                else:
                    return response_or_iterator

            except Exception as e:
                self._error_method(e, context)

        return wrapper

    @staticmethod
    def _convert_message(request):
        return MessageToDict(request, preserving_proto_field_name=True)

    @staticmethod
    def _get_metadata(context):
        metadata = {}
        for key, value in context.invocation_metadata():
            metadata[key.strip()] = value.strip()

        metadata.update({"peer": context.peer()})
        return metadata

    def _generate_message(self, request_iterator):
        for request in request_iterator:
            yield self._convert_message(request)

    def parse_request(self, request_or_iterator, context):
        if isinstance(request_or_iterator, Iterable):
            return self._generate_message(request_or_iterator), self._get_metadata(
                context
            )
        else:
            return self._convert_message(request_or_iterator), self._get_metadata(
                context
            )

    def empty(self):
        return Empty()

    def dict_to_message(self, response: dict):
        # Get grpc method name from call stack
        method_name = inspect.stack()[1][3]

        response_message_name = self._grpc_messages[method_name]["response"]

        if hasattr(self.pb2, response_message_name):
            response_message = getattr(self.pb2, response_message_name)()
        elif response_message_name == "Struct":
            response_message = Struct()
        else:
            raise Exception(
                f"Not found response message in pb2. (message={response_message_name})"
            )

        return ParseDict(response, response_message)

    @staticmethod
    def get_minimal(params: dict):
        return params.get("query", {}).get("minimal", False)
