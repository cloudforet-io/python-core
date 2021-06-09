import types
from google.protobuf.json_format import MessageToDict

from spaceone.core import config
from spaceone.core.transaction import Transaction
from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_grpc_endpoint
from spaceone.core.error import *

__all__ = ["SpaceConnector"]


class SpaceConnector(BaseConnector):

    def __init__(self, transaction: Transaction = None, connector_conf: dict = None, **kwargs):
        super().__init__(transaction, connector_conf)

        self._mock_mode = config.get_global('MOCK_MODE', False)
        self._service = kwargs.get('service')
        self._return_type = kwargs.get('return_type', 'dict')
        self._token = kwargs.get('token')
        self._endpoints = self.config.get('endpoints', {})
        self._verify()

        if self._mock_mode is False:
            self._init_client()

    @property
    def client(self):
        return self._client

    def dispatch(self, method: str, params: dict = None, **kwargs):
        if self._mock_mode:
            raise ERROR_CONNECTOR(connector='SpaceConnector',
                                  reason=f'Dispatch cannot be executed in mock mode. '
                                         f'(service = {self._service}, method = {method})')

        resource, verb = self._parse_method(method)
        self._check_method(resource, verb)

        if params is None:
            params = {}

        kwargs['metadata'] = self._get_connection_metadata()

        response_or_iterator = getattr(getattr(self._client, resource), verb)(params, **kwargs)

        if self._return_type == 'dict':
            if isinstance(response_or_iterator, types.GeneratorType):
                return self._generate_response(response_or_iterator)
            else:
                return self._change_message(response_or_iterator)
        else:
            return response_or_iterator

    def _verify(self):
        if self._service is None:
            raise ERROR_CONNECTOR_LOAD(connector='SpaceConnector', reason='service argument is required.')

        if not isinstance(self._endpoints, dict):
            raise ERROR_CONNECTOR_CONFIGURATION(connector='SpaceConnector')

        if self._service not in self._endpoints:
            raise ERROR_CONNECTOR_LOAD(connector='SpaceConnector', reason=f'{self._service} endpoint is undefined.')

    def _init_client(self):
        e = parse_grpc_endpoint(self._endpoints[self._service])
        self._client = pygrpc.client(endpoint=e['endpoint'], ssl_enabled=e['ssl_enabled'])

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)

    def _generate_response(self, response_iterator):
        for response in response_iterator:
            yield self._change_message(response)

    def _get_connection_metadata(self):
        tnx_meta = self.transaction.meta
        if self._token:
            tnx_meta['token'] = self._token

        keys = ['token', 'transaction_id']
        metadata = []
        for key in keys:
            if key in tnx_meta:
                metadata.append((key, tnx_meta[key]))
        return metadata

    def _parse_method(self, method):
        try:
            resource, verb = method.split('.')
        except Exception:
            raise ERROR_CONNECTOR(connector='SpaceConnector',
                                  reason=f'Method is invalid. (service = {self._service}, method = {method})')

        return resource, verb

    def _check_method(self, resource, verb):
        supported_verb = self._client.api_resources.get(resource)

        if supported_verb is None or verb not in supported_verb:
            raise ERROR_CONNECTOR(connector='SpaceConnector',
                                  reason=f'Method not supported. (service = {self._service}, method = {method})')
