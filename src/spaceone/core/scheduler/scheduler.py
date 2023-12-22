import json
import logging
import time
import copy
from multiprocessing import Process
from uuid import uuid4

import schedule
from jsonschema import validate
from scheduler import Scheduler as CronSchedulerServer
from spaceone.core import queue, config
from spaceone.core.logger import set_logger
from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.core.scheduler.task_schema import SPACEONE_TASK_SCHEMA

_LOGGER = logging.getLogger(__name__)


class BaseScheduler(Process):
    def __init__(self, queue, **kwargs):
        self.queue = queue
        self.config = None

        self.global_config = config.get_global()
        super().__init__()

    def push_task(self):
        # Create Task
        try:
            tasks = self.create_task()
            _LOGGER.debug(f"[push_task] task: {len(tasks)}")
        except Exception as e:
            _LOGGER.error(f"[push_task] error create_task: {e}")
            tasks = []

        for task in tasks:
            try:
                validate(task, schema=SPACEONE_TASK_SCHEMA)
                json_task = json.dumps(task)
                _LOGGER.debug(f"[push_task] Task schema: {self._remove_metadata(task)}")
                queue.put(self.queue, json_task)
            except Exception as e:
                _LOGGER.debug(f"[push_task] Task schema: {task}, {e}")

    def run(self):
        NotImplementedError("scheduler.run is not implemented")

    def create_task(self):
        NotImplementedError("scheduler.create_task is not implemented")

    def _remove_metadata(self, task):
        copied_task = copy.deepcopy(task)
        change_stages = []
        for stage in copied_task.get("stages", []):
            if "metadata" in stage:
                stage["metadata"] = "*****"
            change_stages.append(stage)

        copied_task["stages"] = change_stages
        return copied_task


class IntervalScheduler(BaseScheduler):
    def __init__(self, queue, interval):
        super().__init__(queue)
        self.config = self.parse_config(interval)

    def parse_config(self, expr):
        """expr
        format: integer (second)
        """
        try:
            if isinstance(expr, int):
                return int(expr)
        except Exception as e:
            _LOGGER.error(f"[parse_config] Wrong configraiton, {e}")

    def run(self):
        config.set_global_force(**self.global_config)

        # Enable logging configuration
        set_logger()

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

    def __init__(self, queue, interval=1, minute=":00"):
        super().__init__(queue)
        self.config = self.parse_config(interval)
        self.minute = minute

    def parse_config(self, expr):
        """expr
        format: integer (hour)
        """
        try:
            if isinstance(expr, int):
                return int(expr)
        except Exception as e:
            _LOGGER.error(f"[parse_config] Wrong configuration, {e}")
            raise ERROR_CONFIGURATION(key="interval")

    def run(self):
        config.set_global_force(**self.global_config)

        # Enable logging configuration
        set_logger()

        # Call push_task in every hour
        schedule.every(self.config).hours.at(self.minute).do(self.push_task)
        while True:
            schedule.run_pending()
            time.sleep(1)


class CronScheduler(BaseScheduler):
    """
    cronjob: min hour day month week
    """

    def __init__(self, queue, rule):
        super().__init__(queue)
        self.config = self.parse_config(rule)

    def parse_config(self, expr):
        """exprd
        format: min hour day month week
        * * * * *
        """
        # TODO: verify format
        return expr

    def run(self):
        config.set_global_force(**self.global_config)

        # Enable logging configuration\
        set_logger()

        if self.config is False:
            # May be error format
            return
        scheduler = CronSchedulerServer(10)
        scheduler.add(f"{uuid4()}", self.config, self.push_task)
        scheduler.start()
