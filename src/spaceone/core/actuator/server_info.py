import logging
import os
import pkg_resources

from spaceone.core import config

_LOGGER = logging.getLogger(__name__)


class ServerInfo(object):

    def get_version(self):
        service = config.get_service()
        version = self._get_version_from_pkg(service)
        if version is None:
            version = self._get_version_from_file(service)

        return version or 'UNKNOWN'

    @staticmethod
    def _get_version_from_pkg(service):
        try:
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
