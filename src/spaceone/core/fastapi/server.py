import logging
import uvicorn

from spaceone.core import config
from spaceone.core.logger import set_logger

_LOGGER = logging.getLogger(__name__)


def api_app():
    conf = config.get_global()
    package = conf['PACKAGE']

    rest_route_module = __import__(f'{package}.interface.rest.rest_router', fromlist=['rest_router'])
    return getattr(rest_route_module, 'app', {})


def serve():
    conf = config.get_global()

    # Enable logging configuration
    if conf.get('SET_LOGGING', True):
        set_logger()

    _LOGGER.info(f'Start REST Server ({config.get_service()}): '
                 f'host={conf["HOST"]} port={conf["PORT"]}')

    uvicorn.run('spaceone.core.fastapi.server:api_app', host=conf['HOST'], port=conf['PORT'], factory=True)
