import sys

from celery import Celery
from spaceone.core import config
from spaceone.core.logger import set_logger

app = Celery('spaceone')


@app.task()
def print_conf(**kwargs):
    print(kwargs)
    print('get conf')
    print(config.get_global())


def update_celery_config(app):
    conf = config.get_global()
    default_que = f"{conf.get('SERVICE', 'spaceone')}_q"
    app.conf.update(task_default_queue=default_que)
    app.conf.update(**conf.get('CELERY', {}))

    # add default tasks
    app.autodiscover_tasks(['spaceone.core.celery', conf["PACKAGE"]], force=True)

    # add custom scheduler tasks
    app.autodiscover_tasks([conf["PACKAGE"]], related_name='scheduler', force=True)


def serve(args=None):
    set_logger()
    update_celery_config(app)
    celery_argv = ['celery', '-A', 'spaceone.core.celery.app.app']
    if args.celery_cmd:
        _index = sys.argv.index(args.celery_cmd)
        celery_argv += sys.argv[_index:]
    app.start(argv=celery_argv)
