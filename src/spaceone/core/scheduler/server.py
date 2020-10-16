# -*- coding: utf-8 -*-

import logging
from spaceone.core import config
from spaceone.core.error import ERROR_BASE
from spaceone.core.logger import set_logger

_LOGGER = logging.getLogger(__name__)

DEFAULT_POOL = 8

class Server(object):
    def __init__(self, service, config):
        self.service = service
        self.config = config
        self.queues = {}
        self.schedulers = {}
        self.workers = {}

    def start(self):
        ###################
        # Queues
        ###################
        if 'QUEUES' not in self.config:
            _LOGGER.error('QUEUES is not configured')
            # TODO: exit
        # Load Queue config
        self.queues = self.config['QUEUES']
        _LOGGER.info(f'[Server] queue: {self.queues}')

        ####################
        # Start Schedulers
        ####################
        if 'SCHEDULERS' not in self.config:
            _LOGGER.warn('SCHEDULERS is not configured')
            _LOGGER.warn('Scheduling will be triggered remotely')
            self.config['SCHEDULERS'] = {}

        for (name, conf) in self.config['SCHEDULERS'].items():
            _LOGGER.info(f'[Server] name: {name}')
            _LOGGER.info(f'[Server] config: {conf}')
            params = conf.copy()
            if 'backend' not in conf:
                _LOGGER.debug('Backend is not specified')
                # TODO: ERROR
            backend = conf['backend']
            del params['backend']
            if 'queue' not in conf:
                _LOGGER.debug('Queue is not specified')
                # TODO: ERROR
            self.schedulers[name] = self._create_process(backend, params)

        #######################
        # Start Workers
        #######################
        if 'WORKERS' not in self.config:
            _LOGGER.warn('WORKER is not configured')
            _LOGGER.warn('May be schduler only')
            self.config['WORKERS'] = {}
            # TODO: exit
        # Load Worker config
        for (name, conf) in self.config['WORKERS'].items():
            params = conf.copy()
            if 'backend' not in conf:
                _LOGGER.debug('Backend is not specified')
                # TODO: ERROR
            backend = conf['backend']
            del params['backend']
            if 'queue' not in conf:
                _LOGGER.debug('Queue is not specified')
                # TODO: ERROR
            # Support Worker pool (default: 8)
            # self.queue is instance of Queue
            pool = params.get('pool', DEFAULT_POOL)
            _LOGGER.debug(f'[start] create thread pool: {pool}')
            for index in range(pool):
                worker_name = f'{name}_{index}'
                _LOGGER.debug(f'[start] create {worker_name}')
                self.workers[worker_name] = self._create_process(backend, params)

        # Start All threads
        # start worker
        for (k, v) in self.workers.items():
            v.start()

        # start scheduler
        for (k, v) in self.schedulers.items():
            v.start()

    def _create_process(self, backend, params):
        # create scheduler
        _LOGGER.debug(params)
        try:
            module_name, class_name = backend.rsplit('.', 1)
            scheduler_module = __import__(module_name, fromlist=[class_name])
            return getattr(scheduler_module, class_name)(**params)
        except ERROR_BASE as error:
            _LOGGER.error({error})
            raise error
        except Exception as e:
            _LOGGER.error({e})
            raise e


def serve():
    # Load scheduler config
    # Create Scheduler threads
    # start Scheduler
    conf = config.get_global()

    # Enable logging configuration
    if conf.get('SET_LOGGING', True):
        set_logger()

    server = Server(config.get_service(), conf)
    server.start()
