from typing import Iterable

from spaceone.core.celery.service import CeleryScheduleService
from spaceone.core.celery.types import Interval
from spaceone.core.celery.types import SpaceoneTaskData

mock_data = [
    {
        'domain_id': 'domain_abc',
        'schedule_id': 'schedule_1',
        'enabled': True,
        'task': 'spaceone.core.celery.tasks.test_task',
        'schedule_info': Interval(period='seconds', every=10),
        'args': ('arg1', 'arg2'),
        'kwargs': {"test": 1},
    },
    {
        'domain_id': 'domain_abc',
        'schedule_id': 'schedule_2',
        'enabled': True,
        'task': 'spaceone.core.celery.tasks.test_task',
        'schedule_info': Interval(period='seconds', every=8),
        'args': ('arg1', 'arg2', 'arg3', 'schedule_2'),
        'kwargs': {"test": 1},
    }
]


class ScheduleService(CeleryScheduleService):

    def get(self , params=None)-> SpaceoneTaskData:
        return SpaceoneTaskData(**mock_data[0])

    def list(self, params=None) -> Iterable[SpaceoneTaskData]:
        print('request celery list ')
        return (SpaceoneTaskData(**d) for d in mock_data)

    def update(self, domain_id: str, schedule_id: str, **kwargs) -> SpaceoneTaskData:
        print('request update')
        print(domain_id, schedule_id, kwargs)
