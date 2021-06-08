from google.protobuf.json_format import MessageToDict
import types

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_grpc_endpoint
from spaceone.core.error import *

__all__ = ["SpaceConnector"]


class SpaceConnectorInterceptor(object):

    def __init__(self, client, resource, verbs, return_type, grpc_metadata):
        self.client = client
        self.return_type = return_type
        self.grpc_metadata = grpc_metadata

        for verb in verbs:
            self._bind_verb(resource, verb)

    def _bind_verb(self, resource, verb):
        grpc_method = getattr(getattr(self.client, resource), verb)
        setattr(self, verb, self._interceptor(grpc_method))

    def _interceptor(self, func):
        def wrapper(*args, **kwargs):
            kwargs['metadata'] = self.grpc_metadata

            if len(args) == 0:
                args = ({}, )

            response_or_iterator = func(*args, **kwargs)

            if self.return_type == 'dict':
                if isinstance(response_or_iterator, types.GeneratorType):
                    return self._generate_response(response_or_iterator)
                else:
                    return self._change_message(response_or_iterator)
            else:
                return response_or_iterator

        return wrapper

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)

    def _generate_response(self, response_iterator):
        for response in response_iterator:
            yield self._change_message(response)

class SpaceConnector(BaseConnector):
    def __init__(self, transaction, config, **kwargs):
        super().__init__(transaction, config)
        self._service = kwargs.get('service')
        self._return_type = kwargs.get('return_type', 'dict')
        self._token = kwargs.get('token')
        self._endpoints = self.config.get('endpoints', {})
        self._verify()
        self._init_client()
        self._load_grpc_method()

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

    def _load_grpc_method(self):
        for resource, verbs in self._client.api_resources.items():
            interceptor = SpaceConnectorInterceptor(self._client, resource, verbs,
                                                    self._return_type, self._get_connection_metadata())
            setattr(self, resource, interceptor)

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
