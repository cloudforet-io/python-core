import logging
import uvicorn
import json
import os

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse

from spaceone.core import config
from spaceone.core.logger import set_logger
from spaceone.core.opentelemetry import set_tracer, set_metric
from spaceone.core.extension.server_info import ServerInfoManager

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
    server_info = ServerInfoManager()

    return FastAPI(
        title=global_conf.get('TITLE', 'Document'),
        version=server_info.get_version(),
        contact=global_conf.get('CONTACT', {}),
        description=global_conf.get('DESCRIPTION', ''),
    )


def _get_all_services_from_openapi_json_files(openapi_json_path):
    services = []
    openapi_json_files = os.listdir(openapi_json_path)
    for openapi_json_file in openapi_json_files:
        services.append('_'.join(openapi_json_file.split('_')[:-1]).lower())
    return services


def _sort_services(services):
    return sorted(services, key=lambda x: ('identity' not in x, 'inventory' not in x, 'cost-analysis' not in x,
                                           'monitoring' not in x, 'notification' not in x, 'repository' not in x, x))


def _create_openapi_json(app, service_name):
    swagger_path = config.get_global('EXTENSION_SWAGGER_PATH')
    swagger_path = os.path.join(swagger_path, f"{service_name.replace('-', '_')}_openapi.json")
    try:
        with open(swagger_path, 'r') as f:
            custom_openapi_schema = json.loads(f.read())
            custom_openapi_schema['openapi'] = app.openapi().get('openapi')
            description = custom_openapi_schema['info']['summary']
            app.openapi()['info']['description'] += f"| **{service_name.replace('-', ' ').title()}** | {description} | [/{service_name}/docs](/{service_name}/docs) |\n"

        with open(swagger_path, 'w') as f:
            json.dump(custom_openapi_schema, f, indent=2)
    except Exception as e:
        _LOGGER.error(f'[_create_openapi_json] {swagger_path} : {e}', exc_info=True)


def _override_openapi(app):
    extension_swagger_path = config.get_global('EXTENSION_SWAGGER_PATH')
    if not os.path.exists(extension_swagger_path):
        _LOGGER.info(f'[_override_openapi] Extension Swagger Path is not exists. (path = {extension_swagger_path})')
        return app

    services = _get_all_services_from_openapi_json_files(extension_swagger_path)
    services = _sort_services(services)
    _openapi_info = app.openapi().get('info')
    _openapi_version = app.openapi().get('openapi')

    app.openapi()['info']['description'] += "\n<br><br>\n"
    app.openapi()['info']['description'] += "\n## List of Services\n"
    app.openapi()['info']['description'] += "\n[Home](/docs)\n"
    app.openapi()['info']['description'] += "| **Service** | **Description** | **URL** |\n"
    app.openapi()['info']['description'] += "|:---|:--- |:---|\n"

    for service in services:
        service = service.replace('_', '-')
        _create_openapi_json(app, service_name=service)
        build_docs(app, prefix=f"/{service}", service_name=service)

    app.openapi()['info'][
        'description'] += "| **Console API** | Service that offers features exclusive to the Console API. | [/docs](/docs#console-api%20%3E%20api) |\n"

    return app


def build_docs(
    app: FastAPI,
    prefix: str,
    service_name: str
) -> None:
    async def get_openapi():
        swagger_path = config.get_global('EXTENSION_SWAGGER_PATH')
        swagger_path = os.path.join(swagger_path, f"{service_name.replace('-','_')}_openapi.json")
        with open(swagger_path, 'r') as f:
            custom_openapi_schema = json.loads(f.read())
        return custom_openapi_schema

    get_openapi.__name__ = get_openapi.__name__ + prefix
    app.add_api_route(prefix + "/openapi.json", get_openapi, include_in_schema=False)

    async def swagger_ui_html() -> HTMLResponse:
        return get_swagger_ui_html(
            openapi_url=prefix + "/openapi.json",
            title=f'{service_name.title().replace("-"," ")} API' + ' - Swagger UI',
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            init_oauth=app.swagger_ui_init_oauth,
        )

    swagger_ui_html.__name__ = swagger_ui_html.__name__ + prefix
    app.add_api_route(prefix + "/docs", swagger_ui_html, include_in_schema=False)


def _get_all_services_list(app):
    services = []
    for route in app.routes:
        path = route.path.split('/')
        if len(path) == 4:
            services.append(path[1].replace('-', '_'))

    services = list(set(services))
    sorted_services = sorted(services, key=lambda x: ('identity' not in x, 'inventory' not in x, 'cost_analysis' not in x, x))
    return sorted_services


def fast_api_app():
    app = _init_fast_api()
    app = _add_middlewares(app)
    app = _include_routers(app)
    app = _override_openapi(app)
    return app


def serve():
    conf = config.get_global()

    # Enable logging configuration\
    set_logger()

    # Set OTel Tracer and Metric|
    set_tracer()

    uvicorn_options = conf.get('UVICORN_OPTIONS', {})

    _LOGGER.info(f'Start REST Server ({config.get_service()}): '
                 f'host={conf["HOST"]} port={conf["PORT"]} options={uvicorn_options}')

    uvicorn.run('spaceone.core.fastapi.server:fast_api_app', host=conf['HOST'], port=conf['PORT'], **uvicorn_options)
