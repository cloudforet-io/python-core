# -*- coding: utf-8 -*-
import logging

from celery import shared_task

__all__ = ['domain_scheduler']

from spaceone.core.celery.tasks import BaseSchedulerTask

from spaceone.core import config

_LOGGER = logging.getLogger(__name__)


@shared_task(bind=True, base=BaseSchedulerTask,name='spaceone.work.scheduler.domain_scheduler')
def domain_scheduler(self):
    conf = config.get_global()
    print(conf)
    return [
        {
            'name': 'domain_schedule',
            'version': 'v1',
            'stages': [{
                'locator': 'SERVICE',
                'name': 'DomainService',
                'metadata': {},
                'method': 'enable',
                'params': {
                    'params': {
                        'domain_id': 'asdf'
                    }
                }
            }]
        }
    ]
