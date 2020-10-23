from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal, Union

from celery import schedules


@dataclass
class Cron():
    minute: str
    hour: str
    day_of_week: str
    day_of_month: str
    month_of_year: str

    @property
    def schedule(self):
        return schedules.crontab(minute=self.minute,
                                        hour=self.hour,
                                        day_of_week=self.day_of_week,
                                        day_of_month=self.day_of_month,
                                        month_of_year=self.month_of_year)


@dataclass
class Interval():
    """Schedule executing on a regular interval.
    Example: execute every 4 days
    every=4, period="days"
    """
    period: Literal['days', 'hours', 'minutes', 'seconds', 'microseconds']
    every: int

    @property
    def schedule(self):
        return schedules.schedule(timedelta(**{self.period: self.every}))


@dataclass
class SpaceoneTaskData():
    domain_id: str
    schedule_id: str
    enabled: bool
    task: str
    schedule_info: Union[Interval, Cron]
    args: Any = None
    kwargs: dict = None
    queue:str = None
    exchange:Any = None
    routing_key:str = None
    expires:datetime = None
    soft_time_limit:Any = None
    total_run_count: int = 0
    last_run_at:datetime = None

    @property
    def options(self):
        return {
            'queue': self.queue,
            'exchange': self.exchange,
            'routing_key': self.routing_key,
            'expires': self.expires,
            'soft_time_limit': self.soft_time_limit,
            'enabled': self.enabled
        }