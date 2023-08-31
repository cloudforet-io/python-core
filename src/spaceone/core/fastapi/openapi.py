import logging
import json
import os

from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse

from spaceone.core import config

_LOGGER = logging.getLogger(__name__)


def _add_external_api_route(
        app: FastAPI,
        prefix: str,
        service_name: str,
        external_swagger_path: str
) -> None:
    async def get_openapi():
        openapi_json_file = os.path.join(external_swagger_path, f"{service_name}_openapi.json")
        with open(openapi_json_file, 'r') as f:
            custom_openapi_schema = json.loads(f.read())
        return custom_openapi_schema

    get_openapi.__name__ = get_openapi.__name__ + prefix
    app.add_api_route(prefix + "/openapi.json", get_openapi, include_in_schema=False)

    async def swagger_ui_html() -> HTMLResponse:
        return get_swagger_ui_html(
            openapi_url=prefix + "/openapi.json",
            title=f'{service_name.title().replace("-", " ")} API' + ' - Swagger UI',
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            init_oauth=app.swagger_ui_init_oauth,
        )

    swagger_ui_html.__name__ = swagger_ui_html.__name__ + prefix
    app.add_api_route(prefix + "/docs", swagger_ui_html, include_in_schema=False)


def _services_from_openapi_json_files(openapi_json_path):
    """
    openapi json file must follow {service}_openapi.json file name format.
    if '-' exist in {service} converted to '_'.
    """
    services = []
    openapi_json_files = os.listdir(openapi_json_path)
    for openapi_json_file in openapi_json_files:
        services.append('_'.join(openapi_json_file.split('_')[:-1]).lower())
    return services


def _sort_services(services):
    external_swagger_priority_services = config.get_global('EXTERNAL_SWAGGER_PRIORITY_SERVICES', [])
    external_swagger_priority_services = [external_service.replace('-', '_').lower() for external_service in
                                          external_swagger_priority_services]

    sorted_services = list(dict.fromkeys(external_swagger_priority_services + sorted(services)))
    return sorted_services


def _create_openapi_json(app, service_name, external_swagger_path, service_description):
    openapi_json_file = os.path.join(external_swagger_path, f"{service_name}_openapi.json")
    service_name_with_dash = service_name.replace('_', '-')
    try:
        with open(openapi_json_file, 'r') as f:
            custom_openapi_schema = json.loads(f.read())
            custom_openapi_schema['openapi'] = app.openapi().get('openapi')
            description = custom_openapi_schema['info']['summary']
            service_description[service_name] = f"| **{service_name.replace('_', ' ').title()}** | {description} | [/{service_name_with_dash}/docs](/{service_name_with_dash}/docs) |\n"

        with open(openapi_json_file, 'w') as f:
            json.dump(custom_openapi_schema, f, indent=2)
    except Exception as e:
        _LOGGER.error(f'[_create_openapi_json] {openapi_json_file} : {e}', exc_info=True)


def _check_external_swagger_path(external_swagger_path):
    if not external_swagger_path:
        _LOGGER.info('[_check_external_swagger_path] EXTERNAL_SWAGGER_PATH is not set')
        return False

    if not os.path.exists(external_swagger_path):
        _LOGGER.error(f'[_check_external_swagger_path] "{external_swagger_path}" : Not Found')
        return False

    return True


def _create_external_apis_description(app, service_description):
    app.openapi()['info']['description'] += "\n<br><br>\n"
    app.openapi()['info']['description'] += "\n## List of External APIs\n"
    app.openapi()['info']['description'] += "\n[Home](/docs)\n"
    app.openapi()['info']['description'] += "| **Name** | **Description** | **URL** |\n"
    app.openapi()['info']['description'] += "|:---|:--- |:---|\n"
    app.openapi()['info']['description'] += ''.join(service_description.values())


def add_external_swagger(app):
    try:
        external_swagger_path = config.get_global('EXTERNAL_SWAGGER_PATH')

        if not _check_external_swagger_path(external_swagger_path):
            return app

        services = _services_from_openapi_json_files(external_swagger_path)
        sorted_services = _sort_services(services)

        service_description = {}
        for service in sorted_services:
            _create_openapi_json(app=app, service_name=service, external_swagger_path=external_swagger_path,
                                 service_description=service_description)
            _add_external_api_route(app=app, prefix=f"/{service.replace('_', '-')}", service_name=service,
                                    external_swagger_path=external_swagger_path)

        if len(services) > 0:
            _create_external_apis_description(app, service_description)

    except Exception as e:
        _LOGGER.error(f'[add_external_swagger] {e}', exc_info=True)
    finally:
        return app
