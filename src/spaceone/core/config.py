# -*- coding: utf-8 -*-

import logging

from spaceone.core import utils

_REMOTE_URL = []
_GLOBAL = {
    'PORT': 50051,
    'SERVICE': None,
    'SERVER_TYPE': None,
    'ENDPOINTS': {},
    'MAX_WORKERS': 100,
    'MAX_MESSAGE_LENGTH': 1024*1024*1024,
    'CORE_DEFAULT_APIS': {
        'spaceone.core.api.grpc_health': ['GRPCHealth'],
        'spaceone.core.api.server_info': ['ServerInfo']
    }
}
_LOGGER = logging.getLogger(__name__)


def init_conf(service, server_type='grpc', port=50051):
    _GLOBAL['SERVICE'] = service
    _GLOBAL['SERVER_TYPE'] = server_type
    _GLOBAL['PORT'] = port


def get_service():
    return _GLOBAL['SERVICE']


def get_core_default_apis():
    return _GLOBAL.get('CORE_DEFAULT_APIS', {})


def set_default_conf():
    """
    Get Config from module (spaceone.{service}.conf.global_conf)
    """
    service = _GLOBAL['SERVICE']
    if service is None:
        raise ValueError(f'service is undefined.')
    global_module = __import__(f'spaceone.{service}.conf.global_conf', fromlist=['global_conf'])
    for key, value in vars(global_module).items():
        if not key.startswith('__'):
            _GLOBAL[key] = value


def get_global(key=None):
    if key:
        return _GLOBAL.get(key)
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


def set_remote_conf_from_file(config_yml):
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


def set_file_conf(config_yml):
    file_conf = utils.load_yaml_from_file(config_yml)

    global_conf = file_conf.get('GLOBAL', {})

    set_global(**global_conf)


def load_config(config_yaml_file, service=None):
    file_conf = utils.load_yaml_from_file(config_yaml_file)
    conf_to_merge = file_conf.get('GLOBAL', {})

    if service is None:
        service = conf_to_merge.get('SERVICE', None)

    init_conf(
        service=service,
        server_type=conf_to_merge.get('SERVER_TYPE', None),
        port=conf_to_merge.get('PORT', None)
    )

    set_default_conf()

    set_global(**conf_to_merge)

    return get_global()


def get_handlers(handler_type):
    global_conf = get_global()
    return global_conf.get('HANDLERS', {}).get(handler_type, [])


def get_connector(connector):
    global_conf = get_global()
    return global_conf.get('CONNECTORS', {}).get(connector, {})
