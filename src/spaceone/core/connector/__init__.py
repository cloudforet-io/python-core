import logging
from spaceone.core.base import CoreObject
from spaceone.core.transaction import Transaction

_LOGGER = logging.getLogger(__name__)


class BaseConnector(CoreObject):

    def __init__(self, transaction: Transaction = None, connector_conf: dict = None, **kwargs):
        super().__init__(transaction=transaction)

        self.config = connector_conf or {}
        self._load_interceptors()

        for key, value in kwargs.items():
            setattr(self, key, value)

    def _load_interceptors(self):
        interceptors = self.config.get('interceptors', [])
        for interceptor in interceptors:
            backend = interceptor.get('backend')
            method = interceptor.get('method')
            options = interceptor.get('options', {})

            if backend and method:
                self._set_interceptor(backend, method, options)

    def _set_interceptor(self, backend, method, options):
        try:
            interceptor_module, func_name = self._get_interceptor_module(backend)
            if hasattr(self, method):
                interceptor_func = getattr(self, method)
                setattr(self, method,
                        getattr(interceptor_module, func_name)(interceptor_func, options))
        except Exception as e:
            _LOGGER.error(f'{self.__class__.__name__} Interceptor Load Error: {e}')

    @staticmethod
    def _get_interceptor_module(backend):
        module_name, func_name = backend.rsplit('.', 1)
        interceptor_module = __import__(module_name, fromlist=[func_name])
        return interceptor_module, func_name
