import logging
import json

from spaceone.core import config
from spaceone.core.error import *
from spaceone.core.locator import Locator

_LOGGER = logging.getLogger(__name__)


class BaseAPI(object):
    locator = Locator()

    def __init__(self):
        pass

    @staticmethod
    def parse_request(request, body_data):
        try:
            params = json.loads(body_data)
        except Exception as e:
            params = {}

        path_params = request.path_params
        params.update(path_params)

        return params

