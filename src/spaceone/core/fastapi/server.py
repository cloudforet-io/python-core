import logging
import uvicorn

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from spaceone.core import config
from spaceone.core.logger import set_logger
from spaceone.core.extension.server_info import ServerInfoManager

_LOGGER = logging.getLogger(__name__)


def _get_router_conf(package):
    router_conf_module = __import__(f'{package}.conf.router_conf', fromlist=['router_conf'])
    return getattr(router_conf_module, 'ROUTER', [])


def _get_router(path):
    try:
        module_path, router_name = path.split(':')
        module_name = module_path.rsplit('.')[-1:]
        router_module = __import__(module_path, fromlist=[module_name])
        return getattr(router_module, router_name)
    except Exception as e:
        _LOGGER.warning(f'[_get_router] Invalid router path. (router_path = {path})', exc_info=True)


def include_routers(app):
    # Include All Routers from router_conf.py
    routers = _get_router_conf(config.get_package())

    all_routers_path = []

    # App Routers
    for router_options in routers:
        router_path = append_router(app, router_options)
        if router_path:
            all_routers_path.append(router_path)

    # Extension Routers
    ext_routers = config.get_global('REST_EXTENSION_ROUTERS', [])
    for router_options in ext_routers:
        router_path = append_router(app, router_options)
        if router_path:
            all_routers_path.append(router_path)

    all_routers_path_str = '\n\t - '.join(all_routers_path)
    _LOGGER.debug(f'Loaded Routers: \n\t - {all_routers_path_str}')

    return app


def append_router(app, router_options):
    if 'path' in router_options:
        router = _get_router(router_options['path'])
        router_path = router_options['path']
        del router_options['path']

        app.include_router(
            router,
            **router_options
        )

        return router_path
    else:
        _LOGGER.warning(f'[_include_router] Undefined router path. (router = {router_options})')
        return None


def add_middlewares(app):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )
    return app


def init_fast_api():
    global_conf = config.get_global()
    server_info = ServerInfoManager()

    return FastAPI(
        title=global_conf.get('TITLE', 'Document'),
        version=server_info.get_version(),
        contact=global_conf.get('CONTACT', {}),
        description=global_conf.get('DESCRIPTION', ''),
    )


def fast_api_app():
    app = init_fast_api()
    app = add_middlewares(app)
    app = include_routers(app)
    return app


def serve():
    conf = config.get_global()

    # Enable logging configuration
    if conf.get('SET_LOGGING', True):
        set_logger()

    uvicorn_options = conf.get('UVICORN_OPTIONS', {})

    _LOGGER.info(f'Start REST Server ({config.get_service()}): '
                 f'host={conf["HOST"]} port={conf["PORT"]} options={uvicorn_options}')

    uvicorn.run('spaceone.core.fastapi.server:fast_api_app', host=conf['HOST'], port=conf['PORT'], **uvicorn_options)
