import logging

from spaceone.core import config
from spaceone.core.locator import Locator
from spaceone.api.core.v1 import server_info_pb2, server_info_pb2_grpc

_LOGGER = logging.getLogger(__name__)


class ServerInfo(server_info_pb2_grpc.ServerInfoServicer):

    def __init__(self):
        locator = Locator()
        self.actuator = locator.get_actuator('ServerInfo')

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def pb2_grpc_module(self):
        return server_info_pb2_grpc

    @property
    def service_name(self):
        return server_info_pb2.DESCRIPTOR.services_by_name[self.__class__.__name__].full_name

    def get_version(self, request, context):
        return server_info_pb2.VersionInfo(version=self.actuator.get_version())
