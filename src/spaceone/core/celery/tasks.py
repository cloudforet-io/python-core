from celery import Task, shared_task
from celery.utils.log import get_logger

from spaceone.core.error import ERROR_TASK_LOCATOR, ERROR_TASK_METHOD
from spaceone.core.locator import Locator
from spaceone.core.logger import set_logger
from spaceone.core.transaction import Transaction

_LOGGER = get_logger(__name__)


class BaseTask(Task):
    def __init__(self, *args, **kwargs):
        transaction = Transaction()
        set_logger(transaction)
        self.locator = Locator(transaction)
    def on_success(self, retval, task_id, args, kwargs):
        """Success handler.

        Run by the worker if the task executes successfully.

        Arguments:
            retval (Any): The return value of the task.
            task_id (str): Unique id of the executed task.
            args (Tuple): Original arguments for the executed task.
            kwargs (Dict): Original keyword arguments for the executed task.

        Returns:
            None: The return value of this handler is ignored.
        """
        _LOGGER.debug(f'[{task_id}] executes successfully')


    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Retry handler.

        This is run by the worker when the task is to be retried.

        Arguments:
            exc (Exception): The exception sent to :meth:`retry`.
            task_id (str): Unique id of the retried task.
            args (Tuple): Original arguments for the retried task.
            kwargs (Dict): Original keyword arguments for the retried task.
            einfo (~billiard.einfo.ExceptionInfo): Exception information.

        Returns:
            None: The return value of this handler is ignored.
        """
        _LOGGER.debug(f'[{task_id}] is retried')


    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Error handler.

        This is run by the worker when the task fails.

        Arguments:
            exc (Exception): The exception raised by the task.
            task_id (str): Unique id of the failed task.
            args (Tuple): Original arguments for the task that failed.
            kwargs (Dict): Original keyword arguments for the task that failed.
            einfo (~billiard.einfo.ExceptionInfo): Exception information.

        Returns:
            None: The return value of this handler is ignored.
        """
        _LOGGER.debug(f'[{task_id}] fails')
        _LOGGER.error(f"{einfo}")


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
        try:
            if self.locator == 'SERVICE':
                caller = self._locator.get_service(self.name, self.metadata)
            elif self.locator == 'MANAGER':
                caller = self._locator.get_manager(self.name)

        except Exception as e:
            _LOGGER.debug(f'[SingleTask] fail at locator {e}')
            raise ERROR_TASK_LOCATOR(locator=self.locator, name=self.name)

        try:
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
    for stage in task.get('stages', []):
        try:
            single_task = SingleTask(stage)
            result = single_task.execute()
            print(result)

        except Exception as e:
            _LOGGER.error(f'[SpaceoneTask] fail to parse {stage}, {e}')

            if task.get('stop_on_failure', True):
                _LOGGER.error('[SpaceoneTask] stop task, since stop on failure is enabled')
                break


class BaseSchedulerTask(BaseTask):

    def __call__(self):
        tasks = self.run()
        for task in tasks:
            spaceone_task.apply_async((task,))
