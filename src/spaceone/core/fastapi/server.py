import logging
import uvicorn
from starlette.applications import Starlette
from starlette.routing import Route

from spaceone.core import config
from spaceone.core.logger import set_logger

_LOGGER = logging.getLogger(__name__)


def api_app():
    conf = config.get_global()
    rest_conf = _get_rest_conf(conf['PACKAGE'])

    routes = []
    for url, api_info in rest_conf.items():
        module_split = api_info[0].split('.')
        api_func = module_split[-1]
        module = __import__('.'.join(module_split[:-1]), fromlist=[module_split[-2]])

        routes.append(Route(url, getattr(module, api_func), methods=api_info[1]))

    return Starlette(routes=routes)

def serve():
    conf = config.get_global()

    # Enable logging configuration
    if conf.get('SET_LOGGING', True):
        set_logger()

    _LOGGER.info(f'Start REST Server ({config.get_service()}): '
                 f'host={conf["HOST"]} port={conf["PORT"]}')

    uvicorn.run('spaceone.core.fastapi.server:api_app', host=conf['HOST'], port=conf['PORT'], factory=True)

def _get_rest_conf(package):
    proto_module = __import__(f'{package}.conf.rest_conf', fromlist=['rest_conf'])
    return getattr(proto_module, 'REST', {})
