import consul

import logging

from spaceone.core import config
from spaceone.core.error import ERROR_CONFIGURATION

__all__ = ['get_token']

_LOGGER = logging.getLogger(__name__)

MAX_COUNT = 10

class Consul:
    def __init__(self, config):
        """
        Args:
          - config: connection parameter

        Example:
            config = {
                    'host': 'consul.example.com',
                    'port': 8500
                }
        """
        self.config = self._validate_config(config)

    def _validate_config(self, config):
        """
        Parameter for Consul
        - host, port=8500, token=None, scheme=http, consistency=default, dc=None, verify=True, cert=None
        """
        options = ['host', 'port', 'token', 'scheme', 'consistency', 'dc', 'verify', 'cert']
        result = {}
        for item in options:
            value = config.get(item, None)
            if value:
                result[item] = value
        return result

    def patch_token(self, key):
        """
        Args:
            key: Query key (ex. /debug/supervisor/TOKEN)

        """
        try:
            conn = consul.Consul(**self.config)
            index, data = conn.kv.get(key)
            return data['Value'].decode('ascii')

        except Exception as e:
            _LOGGER.debug(f'[patch_token] failed: {e}')
            return False

def _validate_token(token_info):
    if isinstance(token_info, dict):
        protocol = token_info['protocol']
        if protocol == 'consul':
            consul_instance = Consul(token_info['config'])
            value = False
            while value is False:
                uri = token_info['uri']
                value = consul_instance.patch_token(uri)
                _LOGGER.warn(f'[_validate_token] token: {value[:30]} uri: {uri}')
                if value:
                    break
                time.sleep(INTERVAL)

            token = value
            return token
    raise ERROR_CONFIGURATION(key=token_info)


def get_token(name='TOKEN'):
    try:
        token = config.get_global(name)
    except Exception as e:
        _LOGGER.error(f'[get_token] config error: {name}')
        raise ERROR_CONFIGURATION(key=name)

    if token == "":
        try:
            token_info = config.get_global(f'{name}_INFO')
        except Exception as e:
            _LOGGER.error(f'[get_token] config error: {name}_INFO')
            raise ERROR_CONFIGURATION(key=name)
        token = _validate_token(token_info)
    return token


