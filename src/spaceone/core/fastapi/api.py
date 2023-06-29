import logging
import traceback

from functools import wraps
from fastapi import Request, HTTPException
from spaceone.core.error import *
from spaceone.core.locator import Locator

_LOGGER = logging.getLogger(__name__)

_STATUS_CODE_MAP = {
    'OK': 200,
    'CANCELLED': 499,
    'UNKNOWN': 500,
    'INVALID_ARGUMENT': 400,
    'DEADLINE_EXCEEDED': 504,
    'NOT_FOUND': 404,
    'ALREADY_EXISTS': 409,
    'PERMISSION_DENIED': 403,
    'UNAUTHENTICATED': 401,
    'FAILED_PRECONDITION': 400,
    'ABORTED': 409,
    'OUT_OF_RANGE': 400,
    'UNIMPLEMENTED': 501,
    'INTERNAL': 500,
    'UNAVAILABLE': 503,
    'DATA_LOSS': 500
}


def exception_handler(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except ERROR_BASE as e:
            _print_error_log(e)
            _raise_exception(e)
        except Exception as e:
            error = ERROR_UNKNOWN(message=str(e))
            _print_error_log(error)
            _raise_exception(error)

    return wrapper


def _print_error_log(error):
    if not error.meta.get('skip_error_log'):
        _LOGGER.error(f'(Error) => {error.message} {error}', exc_info=True)


def _raise_exception(error):
    raise HTTPException(status_code=_check_http_status_code(error.status_code),
                        detail={'code': error.error_code, 'message': error.message})


def _check_http_status_code(grpc_status_code: str) -> int:
    return _STATUS_CODE_MAP.get(grpc_status_code, 500)


class BaseAPI(object):
    locator = Locator()
    service = None

    @property
    def name(self):
        return self.__class__.__name__

    async def parse_request(self, request: Request, token=None, resource=None, verb=None):
        try:
            metadata = {
                'url': request.url.path,
                'service': self.service,
                'resource': resource,
                'verb': verb,
                'token': token
            }

            return await request.json(), metadata
        except Exception as e:
            _LOGGER.debug(f'JSON Parsing Error: {e}', exc_info=True)
            raise ERROR_UNKNOWN(message='JSON Parsing Error: Request body requires JSON format.')
