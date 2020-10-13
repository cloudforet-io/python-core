import datetime
import traceback

from celery import current_app, schedules
from celery.beat import ScheduleEntry, Scheduler
from celery.utils.log import get_logger
from spaceone.core import config
from spaceone.core.base import CoreObject
from spaceone.core.celery.service import CeleryScheduleService
from spaceone.core.celery.types import SpaceoneTask

logger = get_logger(__name__)


class SpaceOneScheduleEntry(ScheduleEntry):

    def __init__(self, task: SpaceoneTask):
        super(CoreObject, self).__init__()
        self._task = task

        self.app = current_app._get_current_object()
        self.name = self._task.schedule_id
        self.task = self._task.task

        self.schedule = self._task.schedule_info.schedule

        self.args = self._task.args
        self.kwargs = self._task.kwargs
        self.options = self._task.options
        self.total_run_count = self._task.total_run_count

        if not self._task.last_run_at:
            self._task.last_run_at = self._default_now()
        self.last_run_at = self._task.last_run_at

    def _default_now(self):
        return self.app.now()

    def is_due(self):
        if not self._task.enabled:
            return schedules.schedstate(False, 5.0)  # 5 second delay for re-enable.
        return self.schedule.is_due(self.last_run_at)

    def __repr__(self):
        return (u'<{0} ({1} {2}(*{3}, **{4}) {{5}})>'.format(
            self.__class__.__name__,
            self.name, self.task, self.args,
            self.kwargs, self.schedule,
        ))

    def reserve(self, entry):
        new_entry = Scheduler.reserve(self, entry)
        return new_entry

    def save(self, service_obj: CeleryScheduleService):
        if self.total_run_count > self._task.total_run_count:
            self._task.total_run_count = self.total_run_count
        if self.last_run_at and self._task.last_run_at and self.last_run_at > self._task.last_run_at:
            self._task.last_run_at = self.last_run_at
        try:
            service_obj.update(
                self._task.domain_id,
                self._task.schedule_id,
                total_run_count=self._task.total_run_count,
                last_run_at=self._task.last_run_at
            )
        except Exception:
            logger.error(traceback.format_exc())


class SpaceOneSchedulerError(Exception):
    pass


class SpaceOneScheduler(Scheduler, CoreObject):
    #: how often should we sync in schedule information
    #: from the backend mongo database
    UPDATE_INTERVAL = datetime.timedelta(seconds=5)

    Entry = SpaceOneScheduleEntry

    @property
    def metadata(self):
        if not self._metadata:
            token = config.get_global('TOKEN')
            self._metadata = {'token': token, }
        return self._metadata

    def __init__(self, *args, **kwargs):
        super(CoreObject, self).__init__()
        if hasattr(current_app.conf, "spaceone_scheduler_service"):
            self.service_name = current_app.conf.get("spaceone_scheduler_service")
        else:
            raise SpaceOneSchedulerError("can not find CELERY.spaceone_scheduler_service config")

        self.service = self.locator.get_service(self.service_name, metadata=self.metadata)
        self._schedule = {}
        self._last_updated = None
        self._metadata = None
        Scheduler.__init__(self, *args, **kwargs)
        self.max_interval = (kwargs.get('max_interval')
                             or self.app.conf.CELERYBEAT_MAX_LOOP_INTERVAL or 5)

    def setup_schedule(self):
        pass

    def requires_update(self):
        """check whether we should pull an updated schedule
        from the backend database"""
        if not self._last_updated:
            return True
        return self._last_updated + self.UPDATE_INTERVAL < datetime.datetime.now()

    def get_from_service(self):
        self.sync()
        d = {}
        for task in self.service.list():
            d[task.schedule_id] = self.Entry(task)
        return d

    @property
    def schedule(self):
        if self.requires_update():
            self._schedule = self.get_from_database()
            self._last_updated = datetime.datetime.now()
        return self._schedule

    def sync(self):
        logger.debug('Writing entries...')
        for entry in self._schedule.values():
            entry.save(self.service)
