import logging

from mongoengine import BooleanField, DateTimeField, EmbeddedDocument, EmbeddedDocumentField, IntField, StringField

from spaceone.core.celery.types import Cron, Interval, SpaceoneTaskData
from spaceone.core.model.mongo_model import MongoModel

_LOGGER = logging.getLogger(__name__)


class IntervalData(EmbeddedDocument):
    period = StringField(default='seconds', choices=('days', 'hours', 'minutes', 'seconds', 'microseconds'))
    every = IntField()

    def to_schedule(self):
        return Interval(**self.to_mongo())


class CronData(EmbeddedDocument):
    minute = StringField()
    hour = StringField()
    day_of_week = StringField()
    day_of_month = StringField()
    month_of_year = StringField()

    def to_schedule(self):
        return Cron(**self.to_mongo())


class Schedule(MongoModel):
    domain_id = StringField(max_length=40, default=None, )
    schedule_id = StringField(max_length=40, generate_id='schedule', unique=True)
    enabled = BooleanField()
    task = StringField()
    name = StringField(default=None, )
    interval = EmbeddedDocumentField(IntervalData, default=None, null=True)
    cron = EmbeddedDocumentField(CronData, default=None, null=True)

    created_at = DateTimeField(auto_now_add=True)
    total_run_count = IntField(default=0)
    last_schedule_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'name',
            'task',
            'enabled',
            'total_run_count',
            'last_schedule_at',
            'interval',
            'cron',
        ],
        'exact_fields': [
            'domain_id',
            'schedule_id',
        ],
        'minimal_fields': [
            'domain_id',
            'schedule_id',
            'name',
            'task',
            'enabled',
            'last_schedule_at',
        ],
        'ordering': [
            'name',
        ],
        'indexes': [
            'schedule_id',
            'domain_id',
            'name',
            'enabled',
        ],
    }

    def get_schedule_info(self):
        return self.interval.to_schedule() if self.interval else self.cron.to_schedule()

    def get_task_data(self) -> SpaceoneTaskData:
        data = self.to_mongo()
        if 'interval' in data:
            data.pop('interval')
        if 'cron' in data:
            data.pop('cron')
        data['schedule_info'] = self.get_schedule_info()
        return SpaceoneTaskData(
            domain_id=self.domain_id,
            schedule_id=self.schedule_id,
            enabled=self.enabled,
            task=self.task,
            schedule_info=self.get_schedule_info(),
            total_run_count=self.total_run_count,
            last_run_at=self.last_schedule_at,
            args=[],
            kwargs={},
        )
