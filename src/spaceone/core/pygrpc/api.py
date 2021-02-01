import grpc
import traceback
import collections
import inspect
import types
import logging
from google.protobuf.json_format import MessageToDict
from spaceone.core import config
from spaceone.core.error import *
from spaceone.core.locator import Locator

_LOGGER = logging.getLogger(__name__)


class BaseAPI(object):
    locator = Locator()

    def __init__(self):
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

    def _check_variables(self):
        if not hasattr(self, 'pb2'):
            raise Exception(f'gRPC Servicer has not set <pb2> variable. (servicer={self.__class__.__name__})')

        if not hasattr(self, 'pb2_grpc'):
            raise Exception(f'gRPC Servicer has not set <pb2_grpc> variable. (servicer={self.__class__.__name__})')

    def _get_grpc_servicer(self):
        grpc_servicer = None
        for base_class in self.__class__.__bases__:
            if base_class.__module__ == self.pb2_grpc.__name__:
                grpc_servicer = base_class

        if grpc_servicer is None:
            raise Exception(f'gRPC servicer is not set. (servicer={self.__class__.__name__})')

        return grpc_servicer

    def _set_grpc_method(self):
        grpc_servicer = self._get_grpc_servicer()

        for f_name, f_object in inspect.getmembers(self.__class__, predicate=inspect.isfunction):
            if hasattr(grpc_servicer, f_name):
                setattr(self, f_name, self._grpc_method(f_object, config.get_service()))

    @staticmethod
    def get_minimal(params):
        return params.get('query', {}).get('minimal', False)

    @staticmethod
    def _error_method(error, context):
        is_logging = False
        if not isinstance(error, ERROR_BASE):
            error = ERROR_UNKNOWN(message=error)
            is_logging = True
        elif error.meta.get('type') == 'service':
            is_logging = True

        if is_logging:
            _LOGGER.error(f'(Error) => {error.message} {error}',
                          extra={'error_code': error.error_code,
                                 'error_message': error.message,
                                 'traceback': traceback.format_exc()})

        details = f'{error.error_code}: {error.message}'
        context.abort(grpc.StatusCode[error.status_code], details)

    def _generate_response(self, response_iterator, context):
        try:
            for response in response_iterator:
                yield response

        except Exception as e:
            self._error_method(e, context)

    def _grpc_method(self, func, service_name):
        def wrapper(request_or_iterator, context):
            try:
                context.api_info = {
                    'service': service_name,
                    'resource': self.__class__.__name__,
                    'verb': func.__name__
                }

                context.handler = {
                    'authentication': True,
                    'authorization': True,
                    'mutation': True,
                    'event': True
                }

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

        metadata.update(context.api_info)
        metadata.update(context.handler)

        metadata.update({'peer': context.peer()})

        return metadata

    def _generate_message(self, request_iterator):
        for request in request_iterator:
            yield self._convert_message(request)

    def parse_request(self, request_or_iterator, context):
        if isinstance(request_or_iterator, collections.Iterable):
            return self._generate_message(request_or_iterator), self._get_metadata(context)
        else:
            return self._convert_message(request_or_iterator), self._get_metadata(context)
