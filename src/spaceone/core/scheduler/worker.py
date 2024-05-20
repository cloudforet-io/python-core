import copy
import json
import random
import string
import logging

from multiprocessing import Process

from spaceone.core import queue, config
from spaceone.core.locator import Locator
from spaceone.core.logger import set_logger
from spaceone.core.error import ERROR_TASK_LOCATOR, ERROR_TASK_METHOD

_LOGGER = logging.getLogger(__name__)


def randomString(stringLength=8):
    """Generate a random string of fixed length"""
    letters = string.ascii_lowercase
    return "".join(random.sample(letters, stringLength))


class SingleTask:
    def __init__(self, single_task: dict):
        self.locator = single_task["locator"]
        self.name = single_task["name"]
        self.metadata = single_task["metadata"]
        self.method = single_task["method"]
        self.params = single_task["params"]
        self._locator = Locator()

    def execute(self):
        """Run method"""
        try:
            if self.locator == "SERVICE":
                caller = self._locator.get_service(self.name, self.metadata)
            elif self.locator == "MANAGER":
                caller = self._locator.get_manager(self.name)

        except Exception as e:
            _LOGGER.debug(f"[SingleTask.execute] locator error: {e}")
            raise ERROR_TASK_LOCATOR(locator=self.locator, name=self.name)

        try:
            _LOGGER.debug(
                f"[SingleTask.execute] (REQUEST) => {self.name}.{self.method}"
            )
            method = getattr(caller, self.method)
            resp = method(**self.params)
            _LOGGER.debug(f"[SingleTask.execute] (RESPONSE) => SUCCESS")
            return resp
        except Exception as e:
            _LOGGER.error(
                f"[SingleTask.execute] Fail to execute method ({self.method}): {e}",
                exc_info=True,
            )
            raise ERROR_TASK_METHOD(name=self.name, method=self.method)


class SpaceoneTask:
    def __init__(self, task: dict):
        self.task = task
        self.name = task.get("name", "")
        self.version = task.get("version", "v1")
        self.execution_engine = task.get("executionEngine", "BaseWorker")
        self.stages = task.get("stages")
        self.stop_on_failure = task.get("stop_on_failure", True)

    def execute(self):
        """Run stage"""
        self.tasks = []
        for stage in self.stages:
            try:
                single_task = SingleTask(stage)
                single_task.execute()
            except Exception as e:
                stage_log = copy.deepcopy(stage)
                stage_log["metadata"] = "*****"
                _LOGGER.error(
                    f"[SpaceoneTask] fail to parse {stage_log}, {e}", exc_info=True
                )
                if self.stop_on_failure:
                    _LOGGER.debug(
                        f"[SpaceoneTask] stop task, since stop on failure is enabled"
                    )
                    break


class BaseWorker(Process):
    def __init__(self, queue, **kwargs):
        self._name_ = "worker-%s" % randomString()
        self.queue = queue
        _LOGGER.debug(f"[BaseWorker] BaseWorker name  : {self._name_}")
        _LOGGER.debug(f"[BaseWorker] BaseWorker queue : {self.queue}")

        self.global_config = config.get_global()
        super().__init__()

    def run(self):
        """Infinite Loop"""
        config.set_global_force(**self.global_config)

        # Enable logging configuration
        set_logger()

        while True:
            # Read from Queue
            binary_task = queue.get(self.queue)
            try:
                json_task = json.loads(binary_task.decode())
                task = SpaceoneTask(json_task)
                # Run task
                task.execute()

            except Exception as e:
                _LOGGER.error(
                    f"[{self._name_}] failed to decode task: {binary_task}, {e}"
                )
                continue
