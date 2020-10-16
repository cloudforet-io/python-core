from celery import Task, shared_task
from celery.utils.log import get_logger

from spaceone.core.error import ERROR_TASK_LOCATOR, ERROR_TASK_METHOD
from spaceone.core.locator import Locator
from spaceone.core.transaction import Transaction

_LOGGER = get_logger(__name__)


class BaseTask(Task):
    def __init__(self, *args, **kwargs):
        transaction = Transaction()
        self.locator = Locator(transaction)


class SingleTask:

    def __init__(self, single_task: dict):
        self.locator = single_task['locator']
        self.name = single_task['name']
        self.metadata = single_task['metadata']
        self.method = single_task['method']
        self.params = single_task['params']
        transaction = Transaction(meta=self.metadata)
        self._locator = Locator(transaction)

    def execute(self):
        """ Run method
        """
        print('run single task')
        try:
            if self.locator == 'SERVICE':
                caller = self._locator.get_service(self.name, self.metadata)
            elif self.locator == 'MANAGER':
                caller = self._locator.get_manager(self.name)

        except Exception as e:
            _LOGGER.debug(f'[SingleTask] fail at locator {e}')
            raise ERROR_TASK_LOCATOR(locator=self.locator, name=self.name)

        try:
            print(f'[SingleTask] request: {self.name}.{self.method} {self.params}')
            _LOGGER.debug(f'[SingleTask] request: {self.name}.{self.method} {self.params}')
            method = getattr(caller, self.method)
            resp = method(**self.params)
            _LOGGER.debug(f'[SingleTask] response: {resp}')
            return resp
        except Exception as e:
            _LOGGER.error(f'[SingleTask] fail to execute method: {self.method}, params: {self.params}, {e}')
            raise ERROR_TASK_METHOD(name=self.name, method=self.method, params=self.params)


@shared_task(bind=True)
def test_task(self, *args, **kwargs):
    print(self, args, kwargs, 'check arguments')
    return {
        "name": "test_task",
        "args": args,
        "kwargs": kwargs,
    }


@shared_task(bind=True, base=BaseTask)
def spaceone_task(self, task: dict, *args, **kwargs):
    print(self, task, args, kwargs, '인자 체크')
    for stage in task.get('stages', []):
        try:
            single_task = SingleTask(stage)
            result = single_task.execute()
            print(result)

        except Exception as e:
            print(f'[SpaceoneTask] fail to parse {stage}, {e}')
            if task.get('stop_on_failure', True):
                print(f'[SpaceoneTask] stop task, since stop on failure is enabled')
                break


class BaseSchedulerTask(BaseTask):

    def __call__(self):
        tasks = self.run()
        for task in tasks:
            spaceone_task.apply_async((task,))
