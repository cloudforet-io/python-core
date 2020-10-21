import os
from pprint import pprint
from random import randint
from time import sleep
from typing import List

from spaceone.core import config
from spaceone.core.command import _set_file_config, _set_python_path
from spaceone.core.locator import Locator

from spaceone.core.celery.types import SpaceoneTaskData

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
package = 'spaceone.work'
module_path = BASE_DIR
config_path = f"{BASE_DIR}/spaceone/work/conf/custom_beat.yaml"


def config_server():
    # 1. Set a python path
    _set_python_path(package, module_path)

    # 2. Initialize config from command argument
    config.init_conf(
        package=package,
        server_type='grpc',
        port='50051'
    )

    # 3. Get service config from global_conf.py
    config.set_service_config()

    # 4. Merge file conf
    with open(config_path, 'r') as f:
        _set_file_config(f)


def print_schedules(schedules: List[SpaceoneTaskData]):
    print(f"id | schedule | total_run_count | last_run")
    for sch in schedules:
        print(f"{sch.schedule_id} | {sch.schedule_info} | {sch.total_run_count} | {sch.last_run_at}")
    print('\n\n')


if __name__ == '__main__':
    config_server()
    locator = Locator()
    svc = locator.get_service('ScheduleService')
    print('list all schedules')
    print_schedules(svc.list())

    print('add schedule')
    sch_name = f"test_sche_{randint(1, 1000)}"
    svc.add({
        'domain_id': "sample",
        'enabled': True,
        'task': 'spaceone.core.celery.tasks.test_task',
        'name': sch_name,
        'interval': {
            'period': 'seconds',
            'every': randint(6, 12)
        }

    })
    print(f"Total Schedule : {len(svc.list())}")

    while True:
        sleep(6)
        print_schedules(svc.list())
