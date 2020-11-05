import abc
from typing import Iterable

from spaceone.core.service import BaseService

from spaceone.core.celery.types import SpaceoneTaskData


class CeleryScheduleService(BaseService):
    __metaclass__ = abc.ABCMeta

    def get(self, params=None)->SpaceoneTaskData:
        raise NotImplementedError('you must override celery service get method')


    @abc.abstractmethod
    def list(self, params=None)->Iterable[SpaceoneTaskData]:
        raise NotImplementedError('you must override celery service list method')

    @abc.abstractmethod
    def update(self, params=None)->SpaceoneTaskData:
        raise NotImplementedError('you must override celery service update method')

