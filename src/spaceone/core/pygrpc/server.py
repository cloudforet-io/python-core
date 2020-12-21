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


def _get_proto_conf(package):
    proto_module = __import__(f'{package}.conf.proto_conf', fromlist=['proto_conf'])
    return getattr(proto_module, 'PROTO', {})


def _init_services(server):
    service_names = []

    # Set Core Services
    proto_conf = _get_proto_conf(config.get_package())
    server, service_names = _add_services(server, service_names, proto_conf)

    # Set Extension Services
    proto_conf = config.get_extension_apis()
    server, service_names = _add_services(server, service_names, proto_conf)

    return server, service_names


def _add_services(server, service_names: list, proto_conf: dict):
    for module_path, servicer_names in proto_conf.items():
        for servicer_name in servicer_names:
            api_module = _import_module(module_path, servicer_name)
            if api_module:
                if hasattr(api_module, servicer_name):
                    servicer = getattr(api_module, servicer_name)()

                    getattr(servicer.pb2_grpc_module, f'add_{servicer.name}Servicer_to_server')(servicer, server)
                    service_names.append(servicer.service_name)
                else:
                    _LOGGER.warning(f'[_add_services] Failed to add service. '
                                    f'(module_path={module_path}, servicer_name={servicer_name})')

    return server, service_names


def _import_module(module_path, servicer_name):
    module = None
    try:
        module = __import__(module_path, fromlist=[servicer_name])
    except Exception as e:
        _LOGGER.warning(f'[_import_module] Cannot import grpc servicer module. (reason = {e})', exc_info=True)

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
    conf = config.get_global()

    # Enable logging configuration
    if conf.get('SET_LOGGING', True):
        set_logger()

    server_interceptor = _ServerInterceptor()
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=conf['MAX_WORKERS']),
        interceptors=(server_interceptor,),
        # options=_get_grpc_options(conf)
    )

    server, service_names = _init_services(server)

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
