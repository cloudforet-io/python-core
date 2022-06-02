import copy
import logging
import sys
import consul

from spaceone.core import utils
from spaceone.core.config import default_conf

_REMOTE_URL = []
_GLOBAL = {}
_LOGGER = logging.getLogger(__name__)


def init_conf(package, **kwargs):
    set_default_conf()

    _GLOBAL['PACKAGE'] = package
    _GLOBAL['SERVICE'] = package.rsplit('.', 1)[-1:][0]

    if 'server_type' in kwargs:
        _GLOBAL['SERVER_TYPE'] = kwargs['server_type']

    if 'host' in kwargs:
        _GLOBAL['HOST'] = kwargs['HOST']

    if 'port' in kwargs:
        _GLOBAL['PORT'] = kwargs['port']


def set_default_conf():
    for key, value in vars(default_conf).items():
        if not key.startswith('__'):
            _GLOBAL[key] = value


def get_package():
    return _GLOBAL['PACKAGE']


def get_service():
    return _GLOBAL['SERVICE']


def get_extension_apis():
    return _GLOBAL.get('EXTENSION_APIS', {})


def get_handler(name):
    return _GLOBAL.get('HANDLERS', {}).get(name, {})


def get_connector(name):
    return _GLOBAL.get('CONNECTORS', {}).get(name, {})


def set_service_config():
    """
    Get config from service ({package}.conf.global_conf)
    """

    package = _GLOBAL['PACKAGE']
    if package is None:
        raise ValueError(f'Package is undefined.')

    global_module = __import__(f'{package}.conf.global_conf', fromlist=['global_conf'])
    for key, value in vars(global_module).items():
        if not key.startswith('__'):
            _GLOBAL[key] = value


def get_global(key=None, default=None):
    if key:
        return copy.deepcopy(_GLOBAL.get(key, default))
    else:
        return copy.deepcopy(_GLOBAL)


def set_global(**config):
    global_conf = get_global()

    for key, value in config.items():
        if key in global_conf:
            if not isinstance(value, type(global_conf[key])) and global_conf[key] is not None:
                value_type_name = type(global_conf[key]).__name__
                raise ValueError(f'Value type is invalid. (GLOBAL.{key} = {value_type_name})')

            if isinstance(value, dict):
                global_conf[key] = utils.deep_merge(value, global_conf[key])
            else:
                global_conf[key] = value

    _GLOBAL.update(global_conf)


def set_global_force(**config):
    for key, value in config.items():
        _GLOBAL[key] = value


def set_file_conf(config_yml: str):
    file_conf: dict = utils.load_yaml_from_file(config_yml)
    global_conf: dict = file_conf.get('GLOBAL', {})
    set_global(**global_conf)

    import_conf: list = file_conf.get('IMPORT', [])
    if isinstance(import_conf, list):
        for uri in import_conf:
            import_remote_conf(uri)

    # DEPRECATED: REMOTE_URL setting changed to IMPORT
    import_conf: list = file_conf.get('REMOTE_URL', [])
    if isinstance(import_conf, list):
        for uri in import_conf:
            import_remote_conf(uri)


def import_remote_conf(uri):
    endpoint = utils.parse_endpoint(uri)
    scheme = endpoint.get('scheme')

    remote_conf = None

    if scheme == 'file':
        remote_conf = utils.load_yaml_from_file(endpoint['path'])

    elif scheme in ['http', 'https']:
        remote_conf = utils.load_yaml_from_url(uri)

    elif scheme == 'consul':
        remote_conf = load_consul_config(endpoint)

    if isinstance(remote_conf, dict):
        set_global(**remote_conf)


def load_consul_config(endpoint):
    hostname = endpoint.get('hostname')
    port = endpoint.get('port')
    key = endpoint.get('path', '')[1:]

    try:
        conf = {}
        if hostname:
            conf['host'] = hostname

        if port:
            conf['port'] = port

        c = consul.Consul(**conf)
        index, data = c.kv.get(key)
        if data:
            print(data)
            json_str = data['Value'].decode('utf-8')
            return utils.load_json(json_str)

        return {}
    except Exception as e:
        raise Exception(f'Consul Call Error: {e}')
