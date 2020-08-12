# -*- coding: utf-8 -*-
import logging

from spaceone.core.scheduler import CronScheduler

__all__ = ['TestScheduler']

_LOGGER = logging.getLogger(__name__)


class TestScheduler(CronScheduler):
    def __init__(self, queue):
        super().__init__(queue, '* * * * *')

    def create_task(self):
        print('create_task')
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
