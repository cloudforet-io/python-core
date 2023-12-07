import copy
import logging
import consul

from spaceone.core import utils
from spaceone.core.config import default_conf

_REMOTE_URL = []
_GLOBAL = {}
_LOGGER = logging.getLogger(__name__)


def init_conf(package: str, port: int = None, host: str = None, grpc_app_path: str = None,
              rest_app_path: str = None, plugin_app_path: str = None):
    set_default_conf()

    _GLOBAL['PACKAGE'] = package
    _GLOBAL['SERVICE'] = package.rsplit('.', 1)[-1:][0]

    if host:
        _GLOBAL['HOST'] = host

    if port:
        _GLOBAL['PORT'] = port

    if grpc_app_path:
        _GLOBAL['GRPC_APP_PATH'] = grpc_app_path

    if rest_app_path:
        _GLOBAL['REST_APP_PATH'] = rest_app_path

    if plugin_app_path:
        _GLOBAL['PLUGIN_APP_PATH'] = plugin_app_path


def set_default_conf():
    for key, value in vars(default_conf).items():
        if not key.startswith('__'):
            _GLOBAL[key] = value


def get_package():
    return _GLOBAL['PACKAGE']


def get_service():
    return _GLOBAL['SERVICE']


def get_connector(name):
    return _GLOBAL.get('CONNECTORS', {}).get(name, {})


def set_service_config(global_conf_path: str = None):
    """
    Get config from service
    """

    package = _GLOBAL['PACKAGE']
    if package is None:
        raise ValueError(f'Package is undefined.')

    global_conf_path = global_conf_path or _GLOBAL['GLOBAL_CONF_PATH']
    global_conf_path = global_conf_path.format(package=package)

    module_path, fromlist = global_conf_path.split(':')
    global_module = __import__(module_path, fromlist=[fromlist])

    for key, value in vars(global_module).items():
        if not key.startswith('__'):
            if key in _GLOBAL:
                if isinstance(value, dict):
                    _GLOBAL[key] = utils.deep_merge(value, _GLOBAL[key])
                else:
                    _GLOBAL[key] = value
            else:
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
            json_str = data['Value'].decode('utf-8')
            return utils.load_json(json_str)

        return {}
    except Exception as e:
        raise Exception(f'Consul Call Error: {e}')
