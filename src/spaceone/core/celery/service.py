from typing import Iterable

from spaceone.core.service import BaseService

from spaceone.core.celery.types import SpaceoneTask


class CeleryScheduleService(BaseService):


    def get(self, params)->SpaceoneTask:
        raise NotImplementedError('you must override celery service get method')


    def list(self, params)->Iterable[SpaceoneTask]:
        raise NotImplementedError('you must override celery service list method')


    def update(self, domain_id:str,schedule_id:str,)->SpaceoneTask:
        raise NotImplementedError('you must override celery service update method')

