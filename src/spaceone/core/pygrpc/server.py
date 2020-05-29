# -*- coding: utf-8 -*-

import logging
import time
from concurrent import futures

import grpc
from spaceone.core import config
from spaceone.core.logger import set_logger
from grpc_reflection.v1alpha import reflection

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_LOGGER = logging.getLogger(__name__)


class _ServerInterceptor(grpc.ServerInterceptor):
    _SKIP_METHODS = (
        '/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo'
    )

    def _check_skip_method(self, method):
        is_skip = False

        if method in self._SKIP_METHODS:
            is_skip = True

        return is_skip

    def intercept_service(self, continuation, handler_call_details):
        # is_skip = self._check_skip_method(handler_call_details.method)
        #
        # if not is_skip:
        #     pass

        response = continuation(handler_call_details)

        # if not is_skip:
        #     pass

        return response


def _get_fromlist_path(module_full_path: str):
    return module_full_path[:module_full_path.rfind('.')]


def _get_pb2_module(service, version, module_name):
    return __import__(f'spaceone.api.{service}.{version}.{module_name}', fromlist=[module_name])


def _get_proto_conf(service):
    proto_module = __import__(f'spaceone.{service}.conf.proto_conf', fromlist=['proto_conf'])
    return getattr(proto_module, 'PROTO', {})


def _add_services(server):
    service_names = []
    server, service_names = _add_app_services(server, service_names)
    server, service_names = _add_core_services(server, service_names)
    return server, service_names


def _add_app_services(server, service_names: list):
    service = config.get_service()
    apis_spec = _get_proto_conf(config.get_service())
    for version, apis in apis_spec.items():
        for module_name, apis_class_name in apis.items():
            for api_class_name in apis_class_name:
                api_module = _get_api_module(service, version, module_name, api_class_name)
                servicer = getattr(api_module, api_class_name)()

                getattr(servicer.pb2_grpc_module, f'add_{servicer.name}Servicer_to_server')(servicer, server)
                service_names.append(servicer.service_name)

    return server, service_names


def _get_api_module(service: str, version: str, module_name: str, api_class_name: str):
    return __import__(f'spaceone.{service}.api.{version}.{module_name}', fromlist=[api_class_name])


def _add_core_services(server, service_names: list):
    apis_spec = config.get_core_default_apis()

    for module_path, apis_class_name in apis_spec.items():
        for api_class_name in apis_class_name:
            api_module = _import_module(module_path, api_class_name)
            if api_module:
                servicer = getattr(api_module, api_class_name)()

                getattr(servicer.pb2_grpc_module, f'add_{servicer.name}Servicer_to_server')(servicer, server)
                service_names.append(servicer.service_name)

    return server, service_names


def _import_module(module_path, api_class_name):
    module = None
    try:
        module = __import__(module_path, fromlist=[api_class_name])
    except Exception as e:
        _LOGGER.warning(f'[_import_module] Cannot import core module. (e={e})')

    return module


def _get_grpc_options(conf):
    options = []
    if 'MAX_MESSAGE_LENGTH' in conf:
        options += [
            ('grpc.max_send_message_length', conf['MAX_MESSAGE_LENGTH']),
            ('grpc.max_receive_message_length', conf['MAX_MESSAGE_LENGTH']),
        ]

    return options


def serve():
    set_logger()
    conf = config.get_global()

    server_interceptor = _ServerInterceptor()
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=conf['MAX_WORKERS']),
        interceptors=(server_interceptor,),
        # options=_get_grpc_options(conf)
    )

    server, service_names = _add_services(server)

    service_names_str = '\n\t - '.join(service_names)
    _LOGGER.debug(f'Loaded Services: \n\t - {service_names_str}')
    reflection.enable_server_reflection(service_names, server)

    server.add_insecure_port(f'[::]:{conf["PORT"]}')
    _LOGGER.info(f'Start gRPC Server ({config.get_service()}): '
                 f'port={conf["PORT"]}, max_workers={conf["MAX_WORKERS"]}')
    server.start()

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)
