import types
import logging
from google.protobuf.json_format import MessageToDict
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from spaceone.core import config as global_config
from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_grpc_endpoint
from spaceone.core.error import *

__all__ = ["SpaceConnector"]

_LOGGER = logging.getLogger(__name__)


class SpaceConnector(BaseConnector):

    def __init__(self, return_type: str = 'dict', config: dict = None,
                 service: str = None, endpoint: str = None, **kwargs):
        super().__init__(config)

        self._mock_mode = global_config.get_global('MOCK_MODE', False)
        self._service = service
        self._endpoint = endpoint
        self._return_type = return_type
        self._token = kwargs.get('token')
        self._endpoints = self.config.get('endpoints', {})
        self._verify()

        if self._mock_mode is False:
            self._init_client()

    @property
    def client(self):
        return self._client

    def dispatch(self, method: str, params: dict = None, **kwargs):
        return self._call_api(method, params, **kwargs)

    def _call_api(self, method: str, params: dict = None, **kwargs):
        token = kwargs.get('token')

        self._check_mock_mode(method)

        resource, verb = self._parse_method(method)
        self._check_method(resource, verb)

        params = params or {}
        kwargs['metadata'] = self._get_connection_metadata(token)

        response_or_iterator = getattr(getattr(self._client, resource), verb)(params, **kwargs)

        if self._return_type == 'dict':
            if isinstance(response_or_iterator, types.GeneratorType):
                return self._generate_response(response_or_iterator)
            else:
                return self._change_message(response_or_iterator)
        else:
            return response_or_iterator

    def _check_mock_mode(self, method):
        if self._mock_mode:
            raise ERROR_CONNECTOR(connector='SpaceConnector',
                                  reason=f'Dispatch cannot be executed in mock mode. '
                                         f'(endpoint = {self._get_endpoint()}, method = {method})')

    def _verify(self):
        if self._endpoint:
            pass
        elif self._service:
            if not isinstance(self._endpoints, dict):
                raise ERROR_CONNECTOR_CONFIGURATION(connector='SpaceConnector')

            if self._service not in self._endpoints:
                raise ERROR_CONNECTOR_LOAD(connector='SpaceConnector', reason=f'{self._service} endpoint is undefined.')
        else:
            raise ERROR_CONNECTOR_LOAD(connector='SpaceConnector', reason='service or endpoint is required.')

    def _init_client(self):
        endpoint = self._get_endpoint()
        e = parse_grpc_endpoint(endpoint)
        self._client = pygrpc.client(endpoint=e['endpoint'], ssl_enabled=e['ssl_enabled'],
                                     max_message_length=1024 * 1024 * 256)

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)

    def _generate_response(self, response_iterator):
        for response in response_iterator:
            yield self._change_message(response)

    def _get_connection_metadata(self, token=None):
        metadata = []

        if token:
            metadata.append(('token', token))
        elif self._token:
            metadata.append(('token', self._token))
        elif token := self.transaction.meta.get('token'):
            metadata.append(('token', token))

        carrier = {}
        TraceContextTextMapPropagator().inject(carrier)

        if traceparent := carrier.get('traceparent'):
            metadata.append(('traceparent', traceparent))

        return metadata

    def _parse_method(self, method):
        try:
            resource, verb = method.split('.')
        except Exception:
            raise ERROR_CONNECTOR(connector='SpaceConnector',
                                  reason=f'Method is invalid. (endpoint = {self._get_endpoint()}, method = {method})')

        return resource, verb

    def _check_method(self, resource, verb):
        supported_verb = self._client.api_resources.get(resource)

        if supported_verb is None or verb not in supported_verb:
            raise ERROR_CONNECTOR(connector='SpaceConnector',
                                  reason=f'Method not supported. (endpoint = {self._get_endpoint()}, '
                                         f'method = {resource}.{verb})')

    def _get_endpoint(self):
        return self._endpoint or self._endpoints[self._service]
