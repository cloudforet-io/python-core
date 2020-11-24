DATABASES = {
    'default': {
        'db': 'poc_test_code',
        'host': 'localhost',
        'port': 27017,
        'username': '',
        'password': ''
    }
}
SET_LOGGING = True

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

CONNECTORS = {
}


ENDPOINTS = {}
LOG = {}
QUEUES = {}
SCHEDULERS = {}
WORKERS = {}
CELERY = {}
TOKEN = ""
