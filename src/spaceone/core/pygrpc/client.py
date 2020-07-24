import re
import logging
import types
import grpc
from google.protobuf import descriptor_pb2
from grpc_reflection.v1alpha import reflection_pb2
from grpc_reflection.v1alpha import reflection_pb2_grpc
from spaceone.core import utils
from spaceone.core.error import *
from spaceone.core.pygrpc.message_type import get_well_known_types

_MAX_RETRIES = 2
_GRPC_CHANNEL = {}
_LOGGER = logging.getLogger(__name__)


def _list_to_kwargs(func):
    def wrapped_func(list_args):
        messages = []
        for args in list_args:
            messages.append(func(**args))

        return messages

    return wrapped_func


def _dict_to_kwargs(func):
    def wrapped_func(args):
        return func(**args)

    return wrapped_func


class _ClientInterceptor(
    grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor, grpc.StreamStreamClientInterceptor):

    def __init__(self, options, channel_key):
        self._channel_key = channel_key
        self.metadata = options.get('metadata', {})
        self._MESSAGE_TYPE_MAP = {}
        self._FIELD_TYPE_MAP = {}

    def add_message_type(self, package, module_name, service_name, method_name, input_type):
        message_name = input_type.split('.')[-1:][0]
        method_key = f'/{package}.{service_name}/{method_name}'
        pb2_module = __import__(f'{package}.{module_name}_pb2', fromlist=[f'{module_name}_pb2'])

        if method_key not in self._MESSAGE_TYPE_MAP:
            self._MESSAGE_TYPE_MAP[method_key] = {}

        well_known_types = get_well_known_types()

        if input_type in well_known_types:
            self._MESSAGE_TYPE_MAP[method_key] = {
                'message': well_known_types[input_type],
                'message_type': 'well_known_message'
            }

        elif input_type.startswith(f'.{package}'):
            self._MESSAGE_TYPE_MAP[method_key] = {
                'message': getattr(pb2_module, message_name),
                'message_name': message_name
            }

            if message_name not in self._FIELD_TYPE_MAP:
                self._FIELD_TYPE_MAP[message_name] = {
                    'well_known_type': {},
                    'message_type': {}
                }

    def add_wellknown_type(self, message_type_name, field_name, change_method):
        if message_type_name not in self._FIELD_TYPE_MAP:
            self._FIELD_TYPE_MAP[message_type_name] = {
                'well_known_type': {},
                'message_type': {}
            }

        self._FIELD_TYPE_MAP[message_type_name]['well_known_type'][field_name] = change_method

    def add_field_type(self, message_type_name, field_name, change_method):
        if message_type_name not in self._FIELD_TYPE_MAP:
            self._FIELD_TYPE_MAP[message_type_name] = {
                'well_known_type': {},
                'message_type': {}
            }

        self._FIELD_TYPE_MAP[message_type_name]['message_type'][field_name] = change_method

    def _change_request(self, message_info, request):
        message = message_info['message']
        message_name = message_info.get('message_name')

        if message_info.get('message_type') == 'well_known_message':
            return message(request)

        elif message_name:
            for key in self._FIELD_TYPE_MAP[message_name]['well_known_type']:
                change_method = self._FIELD_TYPE_MAP[message_name]['well_known_type'][key]
                request = utils.change_dict_value(request, key, change_method, change_type='func')

            for key, value in request.items():
                if key in self._FIELD_TYPE_MAP[message_name]['message_type']:
                    change_method = self._FIELD_TYPE_MAP[message_name]['message_type'][key]
                    request[key] = change_method(value)

            change_message = message(**request)

            return change_message

        else:
            return request

    def _make_message(self, request, method_key):
        if isinstance(request, dict):
            return self._change_request(self._MESSAGE_TYPE_MAP[method_key], request)

        else:
            return request

    def _generate_message(self, request_iterator, method_key):
        for request in request_iterator:
            yield self._make_message(request, method_key)

    def _check_message(self, client_call_details, request_or_iterator, is_stream):
        if client_call_details.method in self._MESSAGE_TYPE_MAP:
            if is_stream:
                if not isinstance(request_or_iterator, types.GeneratorType):
                    raise Exception("Stream method must be specified as a generator type.")

                return self._generate_message(request_or_iterator, client_call_details.method)

            else:
                return self._make_message(request_or_iterator, client_call_details.method)

        return request_or_iterator

    def _check_error(self, response):
        if isinstance(response, Exception):
            details = response.details()
            if details.startswith('ERROR_'):
                details_split = details.split(':', 1)
                if len(details_split) == 2:
                    raise ERROR_INTERNAL_API(error_code=details_split[0], message=details_split[1])
                else:
                    raise ERROR_INTERNAL_API(error_code=details_split[0], message=details)

            else:
                if response.code() == grpc.StatusCode.UNAVAILABLE:
                    raise ERROR_GRPC_CONNECTION(channel=self._channel_key, message=response.details())
                else:
                    raise ERROR_INTERNAL_API(message=response.details())

        return response

    def _generate_response(self, response_iterator):
        try:
            for response in response_iterator:
                yield self._check_error(response)
        except Exception as e:
            self._check_error(e)

    def _retry_call(self, continuation, client_call_details, request_or_iterator, is_stream):
        retries = 0

        while True:
            try:
                response_or_iterator = continuation(client_call_details, request_or_iterator)

                if is_stream:
                    response_or_iterator = self._generate_response(response_or_iterator)
                else:
                    self._check_error(response_or_iterator)

                return response_or_iterator

            except Exception as e:
                if e.error_code != 'ERROR_GRPC_CONNECTION' or retries >= _MAX_RETRIES:
                    raise e

                _LOGGER.debug(f'Retry gRPC Call: reason = {e.message}')

            retries += 1

    def _intercept_call(self, continuation, client_call_details,
                        request_or_iterator, is_request_stream, is_response_stream):
        new_request_or_iterator = self._check_message(
            client_call_details, request_or_iterator, is_request_stream)

        return self._retry_call(continuation, client_call_details,
                                new_request_or_iterator, is_response_stream)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return self._intercept_call(continuation, client_call_details, request, False, False)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return self._intercept_call(continuation, client_call_details, request, False, True)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return self._intercept_call(continuation, client_call_details, request_iterator, True, False)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return self._intercept_call(continuation, client_call_details, request_iterator, True, True)


class _GRPCClient:

    def __init__(self, channel, options, channel_key):
        self.api_resources = {}
        self._well_known_types = get_well_known_types()
        self._related_message_types = {}
        self._client_interceptor = _ClientInterceptor(options, channel_key)
        self._channel = channel
        self._intercept_channel = grpc.intercept_channel(channel, self._client_interceptor)
        self._get_server_reflection_info()

    @staticmethod
    def _parse_proto_name(proto_name, package):
        return re.findall(r'%s/(.*?).proto' % package, proto_name)[0]

    def _create_grpc_stub(self, package, module_name, service_name):
        grpc_pb2_module = __import__(f'{package}.{module_name}_pb2_grpc', fromlist=['{module_name}_pb2_grpc'])

        setattr(self, service_name,
                getattr(grpc_pb2_module, f'{service_name}Stub')(self._intercept_channel))

    def _change_message_field_type(self, module_name, message_type_name, field, is_repeated=False):
        pb2_module = self._related_message_types[module_name]['module']
        message_types = self._related_message_types[module_name]['message_types']

        if field.type_name in message_types:
            field_message_name = field.type_name.split('.')[-1:][0]

            if is_repeated:
                change_method = _list_to_kwargs(getattr(pb2_module, field_message_name))
            else:
                change_method = _dict_to_kwargs(getattr(pb2_module, field_message_name))

            self._client_interceptor.add_field_type(
                message_type_name,
                field.name,
                change_method)

    def _change_wellknown_field_type(self, message_type_name, field, field_name):
        change_method = self._well_known_types[field.type_name]
        self._client_interceptor.add_wellknown_type(
            message_type_name,
            field_name,
            change_method)

    def _find_well_known_type_in_field(self, module_name, message_type_name,
                                       field_type_name, parent_field_name, parent_field_label):
        if field_type_name in self._related_message_types[module_name]['message_types']:
            for field in self._related_message_types[module_name]['message_types'][field_type_name].field:
                if field.type == 11:
                    if parent_field_label == 3:  # field.label == LABEL_REPEATED
                        field_key = f'{parent_field_name}.[].{field.name}'
                    else:
                        field_key = f'{parent_field_name}.{field.name}'

                    if field.type_name in self._well_known_types:
                        self._change_wellknown_field_type(message_type_name, field, field_key)

                    else:
                        if field_type_name != field.type_name:
                            self._find_well_known_type_in_field(module_name,
                                                                message_type_name,
                                                                field.type_name,
                                                                field_key,
                                                                field.label)

    def _preload_field_type(self, message_types, module_name):
        for message_type in message_types:
            for field in message_type.field:
                if field.type == 11:  # field.type_name == TYPE_MESSAGE
                    if field.type_name in self._well_known_types:
                        if field.label == 3:  # field.label == LABEL_REPEATED
                            self._change_wellknown_field_type(message_type.name, field, f'{field.name}.[]')
                        else:
                            self._change_wellknown_field_type(message_type.name, field, field.name)

                    else:
                        self._find_well_known_type_in_field(module_name,
                                                            message_type.name,
                                                            field.type_name,
                                                            field.name,
                                                            field.label)

                        if field.label == 3:  # field.label == LABEL_REPEATED
                            self._change_message_field_type(module_name, message_type.name, field, True)
                        else:
                            self._change_message_field_type(module_name, message_type.name, field)

    def _preload_related_message_type(self, file_descriptor_proto, module_name):
        package = file_descriptor_proto.package
        if module_name not in self._related_message_types:
            self._related_message_types[module_name] = {
                'module': __import__(f'{package}.{module_name}_pb2', fromlist=[f'{module_name}_pb2']),
                'message_types': {}
            }

        for message_type in file_descriptor_proto.message_type:
            message_type_name = f'.{package}.{message_type.name}'
            self._related_message_types[module_name]['message_types'][message_type_name] = message_type

    def _preload_message_type(self, package, module_name, service_name, file_descriptor_proto):
        self.api_resources[service_name] = []
        methods = file_descriptor_proto.service[0].method
        for method in methods:
            self._client_interceptor.add_message_type(
                package,
                module_name,
                service_name,
                method.name,
                method.input_type)

            self.api_resources[service_name].append(method.name)

    def _get_file_descriptor(self, reflection_stub, service):
        # Get File Descriptor
        message = reflection_pb2.ServerReflectionRequest(file_containing_symbol=service.name)
        responses = reflection_stub.ServerReflectionInfo(iter((message,)))
        for response in responses:
            file_descriptor_proto = descriptor_pb2.FileDescriptorProto.FromString(
                response.file_descriptor_response.file_descriptor_proto[0])

            # Parse File Descriptor
            package = file_descriptor_proto.package
            service_name = file_descriptor_proto.service[0].name
            module_name = self._parse_proto_name(file_descriptor_proto.name, package)

            self._create_grpc_stub(package, module_name, service_name)
            self._preload_message_type(
                package,
                module_name,
                service_name,
                file_descriptor_proto)

            self._preload_related_message_type(file_descriptor_proto, module_name)
            self._preload_field_type(file_descriptor_proto.message_type, module_name)

    def _get_server_reflection_info(self):
        reflection_stub = reflection_pb2_grpc.ServerReflectionStub(self._channel)

        # List Services
        message = reflection_pb2.ServerReflectionRequest(list_services='')
        responses = reflection_stub.ServerReflectionInfo(iter((message,)))

        for response in responses:
            for service in response.list_services_response.service:
                self._get_file_descriptor(reflection_stub, service)


def client(**client_opts):
    if not client_opts.get('endpoint'):
        raise Exception("Client's endpoint is undefined.")

    endpoint = client_opts['endpoint']
    channel_key = f'{endpoint}'
    options = []

    if channel_key not in _GRPC_CHANNEL:
        if 'max_message_length' in client_opts:
            max_message_length = client_opts['max_message_length']
            options.append(('grpc.max_send_message_length', max_message_length))
            options.append(('grpc.max_receive_message_length', max_message_length))

        channel = grpc.insecure_channel(endpoint, options=options)
        try:
            _GRPC_CHANNEL[channel_key] = _GRPCClient(channel, client_opts, channel_key)
        except Exception as e:
            if hasattr(e, 'details'):
                raise ERROR_GRPC_CONNECTION(channel=channel_key, message=e.details())
            else:
                raise ERROR_GRPC_CONNECTION(channel=channel_key, message=str(e))

    return _GRPC_CHANNEL[channel_key]


def get_grpc_method(uri_info):
    try:
        conn = client(endpoint=uri_info['endpoint'])
        return getattr(getattr(conn, uri_info['service']), uri_info['method'])

    except Exception as e:
        raise ERROR_GRPC_CONFIGURATION(endpoint=uri_info.get('endpoint'),
                                       service=uri_info.get('service'),
                                       method=uri_info.get('method'))
