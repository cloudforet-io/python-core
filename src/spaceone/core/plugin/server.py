from spaceone.core import config
from spaceone.core.logger import set_logger
from spaceone.core.opentelemetry import set_tracer, set_metric
from spaceone.core.pygrpc.server import GRPCServer, add_extension_services


class PluginServer(object):

    _grpc_app: GRPCServer = None
    _global_conf_path: str = None
    _plugin_methods = {}

    def __init__(self):
        if self._global_conf_path:
            config.set_service_config(self._global_conf_path)

        # Enable logging configuration
        set_logger()

        # Set OTel Tracer and Metric
        set_tracer()

    @property
    def grpc_app(self) -> GRPCServer:
        return self._grpc_app

    @classmethod
    def route(cls, plugin_method: str) -> callable:
        def wrapper(func: callable):
            class_name, method_name = plugin_method.split('.')

            if class_name not in cls._plugin_methods:
                raise ValueError(f'Plugin method is invalid. (plugin_method = {plugin_method}, '
                                 f'allowed = {list(cls._plugin_methods.keys())})')

            if method_name not in cls._plugin_methods[class_name]['methods']:
                raise ValueError(f'Plugin method is invalid. (plugin_method = {plugin_method},'
                                 f'allowed = {list(cls._plugin_methods[class_name]["methods"])})')

            cls._plugin_methods[class_name]['service'].set_plugin_method(method_name, func)

        return wrapper

    def run(self):
        add_extension_services(self.grpc_app)
        self.grpc_app.run()


def _get_plugin_app() -> PluginServer:
    package: str = config.get_package()
    app_path: str = config.get_global('PLUGIN_APP_PATH')
    app_path = app_path.format(package=package)
    module_path, app_name = app_path.split(':')

    try:
        app_module = __import__(module_path, fromlist=[app_name])

        if not hasattr(app_module, 'app'):
            raise ImportError(f'App is not defined. (app_path = {package}.{app_path})')

        return getattr(app_module, 'app')
    except Exception as e:
        raise ImportError(f'Cannot import app: {e}')


def serve(app_path: str = None):
    app = _get_plugin_app()
    app.run()