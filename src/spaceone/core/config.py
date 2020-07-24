import logging

from spaceone.core import utils

_REMOTE_URL = []
_GLOBAL = {
    'PORT': 50051,
    'MODULE_PATH': None,
    'SERVICE': None,
    'SERVER_TYPE': None,
    'MAX_WORKERS': 100,
    'MAX_MESSAGE_LENGTH': 1024*1024*1024,
    'EXTENSION_APIS': {
        'spaceone.core.api.grpc_health': ['GRPCHealth'],
        'spaceone.core.api.server_info': ['ServerInfo']
    }
}
_LOGGER = logging.getLogger(__name__)


def init_conf(module_path, **kwargs):
    _GLOBAL['MODULE_PATH'] = module_path
    _GLOBAL['SERVICE'] = module_path.rsplit('.', 1)[-1:][0]

    if 'server_type' in kwargs:
        _GLOBAL['SERVER_TYPE'] = kwargs['server_type']

    if 'port' in kwargs:
        _GLOBAL['PORT'] = kwargs['port']


def get_module_path():
    return _GLOBAL['MODULE_PATH']


def get_service():
    return _GLOBAL['SERVICE']


def get_extension_apis():
    return _GLOBAL.get('EXTENSION_APIS', {})


def set_default_conf():
    """
    Get Config from module ({module_path}.conf.global_conf)
    """
    module_path = _GLOBAL['MODULE_PATH']
    if module_path is None:
        raise ValueError(f'Module path is undefined.')
    global_module = __import__(f'{module_path}.conf.global_conf', fromlist=['global_conf'])
    for key, value in vars(global_module).items():
        if not key.startswith('__'):
            _GLOBAL[key] = value


def get_global(key=None, default=None):
    if key:
        return _GLOBAL.get(key, default)
    else:
        return _GLOBAL


def set_global(**config):
    global_conf = get_global()

    for key, value in config.items():
        if global_conf.get(key, None) is not None:
            if not isinstance(value, type(global_conf[key])):
                value_type_name = type(global_conf[key]).__name__
                raise ValueError(f'Value type is invalid. (GLOBAL.{key} = {value_type_name})')

            if isinstance(value, dict):
                global_conf[key] = utils.deep_merge(value, global_conf[key])
            else:
                global_conf[key] = value


def set_remote_conf_from_file(config_yml: str):
    file_conf = utils.load_yaml_from_file(config_yml)
    url_conf: list = file_conf.get('REMOTE_URL', [])

    for url in url_conf:
        endpoint_info = utils.parse_endpoint(url)
        if endpoint_info['scheme'] == 'file':
            yaml_file = f'{endpoint_info["path"]}'
            conf_to_merge = utils.load_yaml_from_file(yaml_file)
            set_global(**conf_to_merge)
        elif endpoint_info['scheme'] in ['http', 'https']:
            conf_to_merge = utils.load_yaml_from_url(url)
            set_global(**conf_to_merge)


def set_file_conf(config_yml: str):
    file_conf = utils.load_yaml_from_file(config_yml)

    global_conf = file_conf.get('GLOBAL', {})

    set_global(**global_conf)
