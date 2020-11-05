from typing import Iterable

from spaceone.core.celery.service import CeleryScheduleService
from spaceone.core.celery.types import Interval, SpaceoneTaskData
from spaceone.core.service import authentication_handler, authorization_handler, check_required, event_handler, \
    transaction

from spaceone.work.manager import ScheduleManager

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


@authentication_handler
@authorization_handler
@event_handler
class ScheduleService(CeleryScheduleService):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.manager: ScheduleManager = self.locator.get_manager('ScheduleManager')

    @transaction
    def add(self,params=None):
        return self.manager.add_schedule(params)

    @transaction
    @check_required(["domain_id", "schedule_id"])
    def get(self, params=None) -> SpaceoneTaskData:
        sch = self.manager.get_schedule(params['schedule_id'],params['domain_id'])
        return sch.get_task_data()

    def list(self, params=None) -> Iterable[SpaceoneTaskData]:
        schedules,_ = self.manager.list_enabled_schedules()
        return [s.get_task_data() for s in schedules]

    @check_required(["domain_id", "schedule_id"])
    def update(self, params) -> SpaceoneTaskData:
        return self.manager.update_schedule(params)
