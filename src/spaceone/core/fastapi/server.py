import logging

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from spaceone.core import config

_LOGGER = logging.getLogger(__name__)


def _get_router_conf():
    package = config.get_package()
    router_conf_module = __import__(f'{package}.conf.router_conf', fromlist=['router_conf'])
    return getattr(router_conf_module, 'ROUTER', [])


def _get_sub_app_conf():
    package = config.get_package()
    router_conf_module = __import__(f'{package}.conf.router_conf', fromlist=['router_conf'])
    return getattr(router_conf_module, 'SUB_APP', {})


def _get_router(path):
    try:
        module_path, router_name = path.split(':')
        module_name = module_path.rsplit('.')[-1:]
        router_module = __import__(module_path, fromlist=[module_name])
        return getattr(router_module, router_name)
    except Exception as e:
        _LOGGER.warning(f'[_get_router] Invalid router path. (router_path = {path})', exc_info=True)


def _mount_sub_apps(app, sub_apps):
    sub_app_conf = _get_sub_app_conf()
    for sub_app_name, sub_app_options in sub_app_conf.items():
        sub_app = sub_apps.get(sub_app_name)
        if sub_app:
            sub_app_path = sub_app_options.get('path')
            app.mount(path=sub_app_path, app=sub_app)


def _create_sub_app(sub_app_options):
    title = sub_app_options.get('title', 'FastAPI')
    description = sub_app_options.get('description', '')
    contact = sub_app_options.get('contact', {})

    return FastAPI(title=title, description=description, contact=contact)


def _include_routers(app):
    # Include All Routers from router_conf.py
    routers_conf = _get_router_conf()
    sub_apps_conf = _get_sub_app_conf()

    sub_apps = {}
    all_routers_path = []

    # App Routers
    for router_conf in routers_conf:
        sub_app_name = router_conf.get('sub_app')
        router_path = router_conf.get('router_path')
        router_options = router_conf.get('router_options', {})

        if router_path is None:
            _LOGGER.warning(f'[include_routers] Undefined router_path. (router = {router_conf})')
            continue

        if sub_app_name in sub_apps_conf:
            if sub_app_name not in sub_apps:
                sub_app_options = sub_apps_conf[sub_app_name]
                sub_apps[sub_app_name] = _create_sub_app(sub_app_options)

            _append_router(sub_apps[sub_app_name], router_path, router_options)
        else:
            _append_router(app, router_path, router_options)

        all_routers_path.append(router_path)

    # Extension Routers
    ext_routers = config.get_global('REST_EXTENSION_ROUTERS', [])
    for router in ext_routers:
        router_path = router.get('router_path')
        router_options = router.get('router_options', {})
        _append_router(app, router_path, router_options)

        all_routers_path.append(router_path)

    # Mount Sub Applications
    _mount_sub_apps(app, sub_apps)

    all_routers_path_str = '\n\t - '.join(all_routers_path)
    _LOGGER.debug(f'Loaded Routers: \n\t - {all_routers_path_str}')

    return app


def _append_router(app, router_path, router_options):
    router = _get_router(router_path)

    app.include_router(
        router,
        **router_options
    )


def _add_middlewares(app):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )
    return app


def _init_fast_api():
    global_conf = config.get_global()

    return FastAPI(
        title=global_conf.get('REST_TITLE', 'Document'),
        version='x.y.z',
        # version=server_info.get_version(),
        contact=global_conf.get('REST_CONTACT', {}),
        description=global_conf.get('REST_DESCRIPTION', ''),
    )


def fast_api_app():
    app = _init_fast_api()
    app = _add_middlewares(app)
    app = _include_routers(app)
    return app


def serve(app_path: str = None):
    conf = config.get_global()
    app_path = conf['REST_APP_PATH']

    uvicorn_options = conf.get('UVICORN_OPTIONS', {})

    _LOGGER.info(f'Start REST Server ({config.get_service()}): '
                 f'host={conf["HOST"]} port={conf["PORT"]} options={uvicorn_options}')

    uvicorn.run('spaceone.core.fastapi.server:fast_api_app', host=conf['HOST'], port=conf['PORT'], **uvicorn_options)
