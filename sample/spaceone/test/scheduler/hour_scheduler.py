# -*- coding: utf-8 -*-
import logging

from spaceone.core.scheduler import HourlyScheduler, IntervalScheduler

__all__ = ['HourScheduler']

_LOGGER = logging.getLogger(__name__)


class HourScheduler(IntervalScheduler):


    def __init__(self, queue):
        super().__init__(queue, interval=5)

    def create_task(self):
        print(' inter create_task')
        return [
            {'name': 'inventory_cleanup_schedule',
             'version': 'v1',
             'executionEngine': 'BaseWorker',
             'stages': [
                 {
                     'locator': 'SERVICE',
                     'name': 'CleanupService',
                     'metadata': {},
                     'method': 'delete_resources',
                     'params': {'params': {
                         'options': {},
                     }
                     }
                 }
             ]}]
