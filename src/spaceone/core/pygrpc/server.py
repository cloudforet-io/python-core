import logging
import time
from concurrent import futures
from typing import List, Union

import grpc
from spaceone.core import config
from spaceone.core.opentelemetry import set_tracer, set_metric
from spaceone.core.pygrpc.api import BaseAPI
from grpc_reflection.v1alpha import reflection

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


class GRPCServer(object):

    def __init__(self):
        conf = config.get_global()
        self._service = conf['SERVICE']
        self._port = conf['PORT']
        self._max_workers = conf['MAX_WORKERS']
        self._max_message_length = conf.get('MAX_MESSAGE_LENGTH')
        self._ext_proto_conf = config.get_global('GRPC_EXTENSION_SERVICERS', {})
        self._service_names = []

        server_interceptor = _ServerInterceptor()
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=conf['MAX_WORKERS']),
            interceptors=(server_interceptor,),
            # options=_get_grpc_options(conf)
        )
        self._add_extension_services()

    @property
    def server(self) -> grpc.server:
        return self._server

    @property
    def service_names(self) -> List[str]:
        return self._service_names

    def add_service(self, Servicer: Union[BaseAPI, object]):
        servicer = Servicer()
        getattr(servicer.pb2_grpc_module, f'add_{servicer.name}Servicer_to_server')(servicer, self.server)
        self.service_names.append(servicer.service_name)

    def run(self):
        service_names_str = '\n\t - '.join(self.service_names)
        _LOGGER.debug(f'Loaded Services: \n\t - {service_names_str}')
        reflection.enable_server_reflection(self.service_names, self.server)

        self.server.add_insecure_port(f'[::]:{self._port}')
        _LOGGER.info(f'Start gRPC Server ({self._service}): '
                     f'port={self._port}, max_workers={self._max_workers}')
        self.server.start()
        self.server.wait_for_termination()

    def _add_extension_services(self):
        for module_path, servicer_names in self._ext_proto_conf.items():
            for servicer_name in servicer_names:
                if api_module := self._import_module(module_path, servicer_name):
                    if hasattr(api_module, servicer_name):
                        servicer_cls = getattr(api_module, servicer_name)

                        self.add_service(servicer_cls)
                    else:
                        _LOGGER.warning(f'[_add_services] Failed to add service. '
                                        f'(module_path={module_path}, servicer_name={servicer_name})')

    @staticmethod
    def _import_module(module_path, servicer_name):
        module = None
        try:
            module = __import__(module_path, fromlist=[servicer_name])
        except Exception as e:
            _LOGGER.warning(f'[_import_module] Cannot import grpc servicer module. (reason = {e})', exc_info=True)

        return module

    def _get_grpc_options(self):
        options = []
        if self._max_message_length:
            options += [
                ('grpc.max_send_message_length', self._max_message_length),
                ('grpc.max_receive_message_length', self._max_message_length),
            ]

        return options

def _get_app(app_path: str) -> GRPCServer:
    package_path = config.get_package()
    app_module_path, app_name = app_path.split(':')
    module_path = f'{package_path}.{app_module_path}'

    try:
        app_module = __import__(module_path, fromlist=[app_name])
        return getattr(app_module, 'app')
    except Exception as e:
        _LOGGER.warning(f'[_get_app] Cannot import app. (reason = {e})', exc_info=True)


def serve(app_path: str):
    app = _get_app(app_path)
    app.run()
