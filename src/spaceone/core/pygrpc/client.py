import logging
import types
import grpc
from google.protobuf.json_format import ParseDict
from google.protobuf.message_factory import MessageFactory
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.descriptor import ServiceDescriptor, MethodDescriptor
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
            return ParseDict(request, self._request_map[method_key]())

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


class _GRPCStub(object):

    def __init__(self, desc_pool: DescriptorPool, service_desc: ServiceDescriptor, channel: grpc.Channel):
        self._desc_pool = desc_pool
        for method_desc in service_desc.methods:
            self._bind_grpc_method(service_desc, method_desc, channel)

    def _bind_grpc_method(self, service_desc: ServiceDescriptor, method_desc: MethodDescriptor, channel: grpc.Channel):
        method_name = method_desc.name
        method_key = f'/{service_desc.full_name}/{method_name}'
        request_desc = self._desc_pool.FindMessageTypeByName(method_desc.input_type.full_name)
        request_message_desc = MessageFactory(self._desc_pool).GetPrototype(request_desc)
        response_desc = self._desc_pool.FindMessageTypeByName(method_desc.output_type.full_name)
        response_message_desc = MessageFactory(self._desc_pool).GetPrototype(response_desc)

        if method_desc.client_streaming and method_desc.server_streaming:
            setattr(self, method_name, channel.stream_stream(
                method_key,
                request_serializer=request_message_desc.SerializeToString,
                response_deserializer=response_message_desc.FromString
            ))
        elif method_desc.client_streaming and not method_desc.server_streaming:
            setattr(self, method_name, channel.stream_unary(
                method_key,
                request_serializer=request_message_desc.SerializeToString,
                response_deserializer=response_message_desc.FromString
            ))
        elif not method_desc.client_streaming and method_desc.server_streaming:
            setattr(self, method_name, channel.unary_stream(
                method_key,
                request_serializer=request_message_desc.SerializeToString,
                response_deserializer=response_message_desc.FromString
            ))
        else:
            setattr(self, method_name, channel.unary_unary(
                method_key,
                request_serializer=request_message_desc.SerializeToString,
                response_deserializer=response_message_desc.FromString
            ))


class GRPCClient(object):

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
            for method_desc in service_desc.methods:
                method_key = f'/{service}/{method_desc.name}'
                request_desc = self._desc_pool.FindMessageTypeByName(method_desc.input_type.full_name)
                self._request_map[method_key] = MessageFactory(self._desc_pool).GetPrototype(request_desc)

                if service_desc.name not in self._api_resources:
                    self._api_resources[service_name] = []

                self._api_resources[service_desc.name].append(method_desc.name)

    def _bind_grpc_stub(self, intercept_channel: grpc.Channel):
        for service in self._reflection_db.get_services():
            service_desc: ServiceDescriptor = self._desc_pool.FindServiceByName(service)

            setattr(self, service_desc.name, _GRPCStub(self._desc_pool, service_desc, intercept_channel))


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
            _GRPC_CHANNEL[endpoint] = GRPCClient(channel, client_opts, endpoint)
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
