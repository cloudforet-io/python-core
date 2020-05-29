# -*- coding: utf-8 -*-

import json
import logging
import schedule
import time

from jsonschema import validate
from multiprocessing import Process

from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.core import queue
from spaceone.core.scheduler.task_schema import SPACEONE_TASK_SCHEMA

_LOGGER = logging.getLogger(__name__)


class BaseScheduler(Process):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self.config = None

    def push_task(self):
        # Create Task
        tasks = self.create_task()
        _LOGGER.debug(f'[push_task] task: {len(tasks)}')
        for task in tasks:
            try:
                validate(task, schema=SPACEONE_TASK_SCHEMA)
                json_task = json.dumps(task)
                _LOGGER.debug(f'[push_task] Task schema: {task}')
                queue.put(self.queue, json_task)
            except Exception as e:
                _LOGGER.debug(f'[push_task] Task schema: {task}, {e}')

    def run(self):
        NotImplementedError('scheduler.run is not implemented')

    def create_task(self):
        NotImplementedError('scheduler.create_task is not implemented')


class IntervalScheduler(BaseScheduler):
    def __init__(self, queue, interval):
        super().__init__(queue)
        self.config = self.parse_config(interval)

    def parse_config(self, expr):
        """ expr
          format: integer (second)
        """
        try:
            if isinstance(expr, int):
                return int(expr)
        except Exception as e:
            _LOGGER.error(f'[parse_config] Wrong configraiton, {e}')

    def run(self):

        schedule.every(self.config).seconds.do(self.push_task)
        while True:
            schedule.run_pending()
            time.sleep(1)


class HourlyScheduler(BaseScheduler):
    """
    HourlyScheduler starts every HH:00
    If you want to start at different minutes
    send minute like ':15' meaning every 15 minute
    """
    def __init__(self, queue, interval=1, minute=':00'):
        super().__init__(queue)
        self.config = self.parse_config(interval)
        self.minute = minute

    def parse_config(self, expr):
        """ expr
          format: integer (hour)
        """
        try:
            if isinstance(expr, int):
                return int(expr)
        except Exception as e:
            _LOGGER.error(f'[parse_config] Wrong configuration, {e}')
            raise ERROR_CONFIGURATION(key='interval')

    def run(self):
        # Call push_task in every hour
        schedule.every(self.config).hour.at(self.minute).do(self.push_task)
        while True:
            schedule.run_pending()
            time.sleep(1)


class CronScheduler(BaseScheduler):
    """
    cronjob: min hour day month week
    """
    def __init__(self, queue, rule):
        super().__init__(queue, rule)
        self.config = self.parse_config(rule)

    def parse_config(self, expr):
        """ expr
          format: min hour day month week
          * * * * *
        """
        items = expr.split(' ')
        if len(items) != 5:
            return False
        # TODO: verify format
        return items

    def run(self):
        if self.config is False:
            # May be error format
            return

        # Minute
        if self.config[0] == '*':
            schedule.every().minutes.do(self.push_task)
        else:
            schedule.every(int(self.config[0])).minutes.do(self.push_task)

        # Hour
        if self.config[1] == '*':
            schedule.every().hour.do(self.push_task)
        else:
            schedule.every(int(self.config[1])).hour.do(self.push_task)

        # Day
        if self.config[2] == '*':
            schedule.every().day.do(self.push_task)
        else:
            schedule.every(int(self.config[2])).day.do(self.push_task)

        # Month
        if self.config[3] != '*':
            _LOGGER.warn("Month is not applicable")

        while True:
            schedule.run_pending()
            time.sleep(1)
