import logging
import os
import pkg_resources

from spaceone.core import config
from spaceone.api.core.v1 import server_info_pb2, server_info_pb2_grpc

_LOGGER = logging.getLogger(__name__)


class ServerInfo(server_info_pb2_grpc.ServerInfoServicer):

    def __init__(self):
        self.server_info = ServerInfoManager()

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
        return server_info_pb2.VersionInfo(version=self.server_info.get_version())


class ServerInfoManager(object):

    def get_version(self):
        service = config.get_service()
        version = self._get_version_from_pkg(service)
        if version is None:
            version = self._get_version_from_file(service)

        return version or 'UNKNOWN'

    @staticmethod
    def _get_version_from_pkg(service):
        try:
            # TODO: Change the process of finding the package name
            return pkg_resources.get_distribution(f'spaceone-{service}').version
        except Exception as e:
            return None

    @staticmethod
    def _get_version_from_file(service):
        try:
            module = __import__(f'spaceone.{service}', fromlist=[service])
            root_path = os.path.dirname(os.path.dirname(os.path.dirname(module.__file__)))
            with open(f'{root_path}/VERSION', 'r') as f:
                version = f.read().strip()
                f.close()
                return version
        except Exception as e:
            return None
