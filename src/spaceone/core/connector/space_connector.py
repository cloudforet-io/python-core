import types
import logging
from typing import Any, List, Tuple
from google.protobuf.json_format import MessageToDict
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_grpc_endpoint
from spaceone.core.pygrpc.client import GRPCClient
from spaceone.core.error import *

__all__ = ["SpaceConnector"]

_LOGGER = logging.getLogger(__name__)


class SpaceConnector(BaseConnector):
    name = "SpaceConnector"

    def __init__(
        self,
        *args,
        service: str = None,
        endpoint: str = None,
        token: str = None,
        return_type: str = "dict",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._service = service
        self._endpoint = endpoint
        self._token = token
        self._return_type = return_type

        self._client = None
        self._endpoints: dict = self.config.get("endpoints", {})

        self._verify()
        self._init_client()

    @property
    def client(self) -> GRPCClient:
        return self._client

    def dispatch(self, method: str, params: dict = None, **kwargs) -> Any:
        return self._call_api(method, params, **kwargs)

    def _call_api(
        self,
        method: str,
        params: dict = None,
        token: str = None,
        x_domain_id: str = None,
        x_workspace_id: str = None,
    ) -> Any:
        resource, verb = self._parse_method(method)
        self._check_method(resource, verb)

        params = params or {}
        metadata = self._get_connection_metadata(token, x_domain_id, x_workspace_id)

        response_or_iterator = getattr(getattr(self._client, resource), verb)(
            params, metadata=metadata
        )

        if self._return_type == "dict":
            if isinstance(response_or_iterator, types.GeneratorType):
                return self._generate_response(response_or_iterator)
            else:
                return self._change_message(response_or_iterator)
        else:
            return response_or_iterator

    def _verify(self) -> None:
        if self._service:
            if not isinstance(self._endpoints, dict):
                raise ERROR_CONNECTOR_CONFIGURATION(connector="SpaceConnector")

            if self._service not in self._endpoints:
                raise ERROR_CONNECTOR_LOAD(
                    connector="SpaceConnector",
                    reason=f"{self._service} endpoint is undefined.",
                )

        elif not self._endpoint:
            raise ERROR_CONNECTOR_LOAD(
                connector="SpaceConnector", reason="service or endpoint is required."
            )

    def _init_client(self) -> None:
        endpoint = self._get_endpoint()
        e = parse_grpc_endpoint(endpoint)
        self._client: GRPCClient = pygrpc.client(
            endpoint=e["endpoint"],
            ssl_enabled=e["ssl_enabled"],
            max_message_length=1024 * 1024 * 256,
        )

    @staticmethod
    def _change_message(message) -> dict:
        return MessageToDict(message, preserving_proto_field_name=True)

    def _generate_response(self, response_iterator):
        for response in response_iterator:
            yield self._change_message(response)

    def _get_connection_metadata(
        self, token: str = None, x_domain_id: str = None, x_workspace_id: str = None
    ) -> List[Tuple]:
        metadata = []

        if token:
            metadata.append(("token", token))
        elif self._token:
            metadata.append(("token", self._token))
        elif token := self.transaction.meta.get("token"):
            metadata.append(("token", token))

        if x_domain_id:
            metadata.append(("x_domain_id", x_domain_id))

        if x_workspace_id:
            metadata.append(("x_workspace_id", x_workspace_id))

        carrier = {}
        TraceContextTextMapPropagator().inject(carrier)

        if traceparent := carrier.get("traceparent"):
            metadata.append(("traceparent", traceparent))

        return metadata

    def _parse_method(self, method: str) -> Tuple[str, str]:
        try:
            resource, verb = method.split(".")
        except Exception:
            raise ERROR_CONNECTOR(
                connector="SpaceConnector",
                reason=f"Method is invalid. (endpoint = {self._get_endpoint()}, method = {method})",
            )

        return resource, verb

    def _check_method(self, resource: str, verb: str) -> None:
        supported_verb = self._client.api_resources.get(resource)

        if supported_verb is None or verb not in supported_verb:
            raise ERROR_CONNECTOR(
                connector="SpaceConnector",
                reason=f"Method not supported. "
                f"(endpoint = {self._get_endpoint()}, method = {resource}.{verb})",
            )

    def _get_endpoint(self) -> str:
        return self._endpoint or self._endpoints[self._service]
