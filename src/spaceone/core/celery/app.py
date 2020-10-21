import logging
from enum import Enum

import celery
from celery import Celery
from celery.apps.beat import Beat
from celery.apps.worker import Worker
from spaceone.core import config

DEFAULT_SPACEONE_BEAT = 'spaceone.core.celery.schedulers.SpaceOneScheduler'

@celery.signals.after_setup_logger.connect
def on_after_setup_logger(**kwargs):
    if config.get_global('CELERY',{}).get('debug_mode'):
        logger = logging.getLogger('celery')
        logger.propagate = True
        logger.level = logging.DEBUG
        logger = logging.getLogger('celery.app.trace')
        logger.propagate = True
        logger.level = logging.DEBUG

app = Celery('spaceone')


class SERVER_MODE_ENUM(Enum):
    WORKER = 'WORKER'
    BEAT = 'BEAT'
    SPACEONE_BEAT = 'SPACEONE_BEAT'




def update_celery_config(app):
    conf = config.get_global()
    default_que = f"{conf.get('SERVICE', 'spaceone')}_q"
    app.conf.update(task_default_queue=default_que)
    app.conf.update(task_cls='spaceone.core.celery.tasks:BaseTask')
    app.conf.update(**conf.get('CELERY', {}))

    # add default tasks
    app.autodiscover_tasks(['spaceone.core.celery', conf["PACKAGE"]], force=True)

    # add custom scheduler tasks
    app.autodiscover_tasks([conf["PACKAGE"]], related_name='scheduler', force=True)


def serve():
    # set_logger()
    update_celery_config(app)
    server_mode = app.conf.get('mode',SERVER_MODE_ENUM.WORKER.value)
    if server_mode == SERVER_MODE_ENUM.BEAT.value:
        Beat(app=app, loglevel='DEBUG').run()
    elif server_mode == SERVER_MODE_ENUM.SPACEONE_BEAT.value:
        app.conf.update(beat_scheduler=DEFAULT_SPACEONE_BEAT)
        Beat(app=app, loglevel='DEBUG').run()
    else:
        Worker(app=app).start()
