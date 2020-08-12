DATABASES = {
    'default': {
        'db': 'test',
        'host': 'localhost',
        'port': 27017,
        'username': '',
        'password': ''
    }
}

CACHES = {
    'default': {},
    'local': {
        'backend': 'spaceone.core.cache.local_cache.LocalCache',
        'max_size': 128,
        'ttl': 86400
    }
}

HANDLERS = {
}

ENDPOINTS = {}
LOG = {
    "loggers": {
        "spaceone": {
            "handlers": ['console']
        }
    }
}
QUEUES = {
    "test_q": {
        "backend": "spaceone.core.queue.redis_queue.RedisQueue",
        "host": "localhost",
        "port": 6379,
        "channel": "que"
    }
}
SCHEDULERS = {
    "cron_scheduler": {
        "backend": "spaceone.test.scheduler.test_scheduler.TestScheduler",
        "queue": "test_q",
    },
"hour_scheduler": {
        "backend": "spaceone.test.scheduler.hour_scheduler.HourScheduler",
        "queue": "test_q",
    }
}
WORKERS = {}
TOKEN = ""
TOKEN_INFO = {}
