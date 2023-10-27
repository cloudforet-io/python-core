import re
import logging
import types
import grpc
from google.protobuf.message_factory import MessageFactory
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.descriptor import ServiceDescriptor
from grpc_reflection.v1alpha.proto_reflection_descriptor_database import ProtoReflectionDescriptorDatabase
from spaceone.core.error import *

_MAX_RETRIES = 2
_GRPC_CHANNEL = {}
_LOGGER = logging.getLogger(__name__)


class _ClientInterceptor(
    grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor, grpc.StreamStreamClientInterceptor):

    def __init__(self, options: dict, channel_key: str, request_map: dict):
        self._request_map = request_map
        self._channel_key = channel_key
        self.metadata = options.get('metadata', {})

    def _check_message(self, client_call_details, request_or_iterator, is_stream):
        if client_call_details.method in self._request_map:
            if is_stream:
                if not isinstance(request_or_iterator, types.GeneratorType):
                    raise Exception("Stream method must be specified as a generator type.")

                return self._generate_message(request_or_iterator, client_call_details.method)

            else:
                return self._make_message(request_or_iterator, client_call_details.method)

        return request_or_iterator

    def _make_message(self, request, method_key):
        if isinstance(request, dict):
            return self._request_map[method_key](**request)

        else:
            return request

    def _generate_message(self, request_iterator, method_key):
        for request in request_iterator:
            yield self._make_message(request, method_key)

    def _check_error(self, response):
        if isinstance(response, Exception):
            details = response.details()
            status_code = response.code().name
            if details.startswith('ERROR_'):
                details_split = details.split(':', 1)
                if len(details_split) == 2:
                    error_code, error_message = details_split
                else:
                    error_code = details_split[0]
                    error_message = details

                if status_code == 'PERMISSION_DENIED':
                    raise ERROR_PERMISSION_DENIED()
                elif status_code == 'UNAUTHENTICATED':
                    raise ERROR_AUTHENTICATE_FAILURE(message=error_message)
                else:
                    e = ERROR_INTERNAL_API(message=error_message)
                    e.error_code = error_code
                    e.status_code = status_code
                    raise e

            else:
                error_message = response.details()
                if status_code == 'PERMISSION_DENIED':
                    raise ERROR_PERMISSION_DENIED()
                elif status_code == 'PERMISSION_DENIED':
                    raise ERROR_AUTHENTICATE_FAILURE(message=error_message)
                elif status_code == 'UNAVAILABLE':
                    e = ERROR_GRPC_CONNECTION(channel=self._channel_key, message=error_message)
                    e.meta['channel'] = self._channel_key
                    raise e
                else:
                    e = ERROR_INTERNAL_API(message=error_message)
                    e.status_code = status_code
                    raise e

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
                if e.error_code == 'ERROR_GRPC_CONNECTION':
                    if retries >= _MAX_RETRIES:
                        channel = e.meta.get('channel')
                        if channel in _GRPC_CHANNEL:
                            _LOGGER.error(f'Disconnect gRPC Endpoint. (channel = {channel})')
                            del _GRPC_CHANNEL[channel]
                        raise e
                    else:
                        _LOGGER.debug(f'Retry gRPC Call: reason = {e.message}, retry = {retries + 1}')
                else:
                    raise e

            retries += 1

    def _intercept_call(self, continuation, client_call_details,
                        request_or_iterator, is_request_stream, is_response_stream):
        new_request_or_iterator = self. _check_message(
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
        self._request_map = {}
        self._api_resources = {}

        self._reflection_db = ProtoReflectionDescriptorDatabase(channel)
        self._desc_pool = DescriptorPool(self._reflection_db)
        self._init_grpc_reflection()

        _client_interceptor = _ClientInterceptor(options, channel_key, self._request_map)
        _intercept_channel = grpc.intercept_channel(channel, _client_interceptor)
        self._bind_grpc_stub(_intercept_channel)

    @property
    def api_resources(self):
        return self._api_resources

    def _init_grpc_reflection(self):
        for service in self._reflection_db.get_services():
            service_desc: ServiceDescriptor = self._desc_pool.FindServiceByName(service)
            service_name = service_desc.name
            for method in service_desc.methods:
                method_key = f'/{service}/{method.name}'
                request_desc = self._desc_pool.FindMessageTypeByName(method.input_type.full_name)
                self._request_map[method_key] = MessageFactory(self._desc_pool).GetPrototype(request_desc)

                if service_desc.name not in self._api_resources:
                    self._api_resources[service_name] = []

                self._api_resources[service_desc.name].append(method.name)

    def _bind_grpc_stub(self, intercept_channel: grpc.Channel):
        for service in self._reflection_db.get_services():
            service_desc: ServiceDescriptor = self._desc_pool.FindServiceByName(service)
            package = service_desc.file.package
            module_name = self._parse_proto_name(service_desc.file.name, package)
            self._create_grpc_stub(package, module_name, service_desc.name, intercept_channel)

    def _create_grpc_stub(self, package: str, module_name: str, service_name: str, intercept_channel: grpc.Channel):
        module_path = f'{package}.{module_name}_pb2_grpc'
        grpc_pb2_module = __import__(module_path, fromlist=[f'{module_name}_pb2_grpc'])

        setattr(self, service_name,
                getattr(grpc_pb2_module, f'{service_name}Stub')(intercept_channel))

    @staticmethod
    def _parse_proto_name(proto_name, package):
        return re.findall(r'%s/(.*?).proto' % package, proto_name)[0]


def _create_secure_channel(endpoint, options):
    try:
        # cert = ssl.get_server_certificate((host, port))
        # creds = grpc.ssl_channel_credentials(str.encode(cert))
        creds = grpc.ssl_channel_credentials()
    except Exception as e:
        raise ERROR_GRPC_TLS_HANDSHAKE(reason=e)

    return grpc.secure_channel(endpoint, creds, options=options)


def _create_insecure_channel(endpoint, options):
    return grpc.insecure_channel(endpoint, options=options)


def client(endpoint=None, ssl_enabled=False, max_message_length=None, **client_opts):
    if endpoint is None:
        raise Exception("Client's endpoint is undefined.")

    if endpoint not in _GRPC_CHANNEL:
        options = []

        if max_message_length:
            options.append(('grpc.max_send_message_length', max_message_length))
            options.append(('grpc.max_receive_message_length', max_message_length))

        if ssl_enabled:
            channel = _create_secure_channel(endpoint, options)
        else:
            channel = _create_insecure_channel(endpoint, options)

        try:
            grpc.channel_ready_future(channel).result(timeout=3)
        except Exception as e:
            raise ERROR_GRPC_CONNECTION(channel=endpoint, message='Channel is not ready.')

        try:
            _GRPC_CHANNEL[endpoint] = _GRPCClient(channel, client_opts, endpoint)
        except Exception as e:
            if hasattr(e, 'details'):
                raise ERROR_GRPC_CONNECTION(channel=endpoint, message=e.details())
            else:
                raise ERROR_GRPC_CONNECTION(channel=endpoint, message=str(e))

    return _GRPC_CHANNEL[endpoint]


def get_grpc_method(uri_info):
    try:
        conn = client(endpoint=uri_info['endpoint'], ssl_enabled=uri_info['ssl_enabled'])
        return getattr(getattr(conn, uri_info['service']), uri_info['method'])

    except ERROR_BASE as e:
        raise e
    except Exception as e:
        raise ERROR_GRPC_CONFIGURATION(endpoint=uri_info.get('endpoint'),
                                       service=uri_info.get('service'),
                                       method=uri_info.get('method'))
