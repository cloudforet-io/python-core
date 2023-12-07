# Service Configuration
PACKAGE = None
SERVICE = None

# Server Configuration
PORT = 50051
HOST = '127.0.0.1'

MAX_WORKERS = 100
MAX_MESSAGE_LENGTH = 1024 * 1024 * 1024

# Global Configuration
GLOBAL_CONF_PATH = '{package}.conf.global_conf:global_conf'

# Unit Test Configuration
MOCK_MODE = False

# gRPC Configuration
GRPC_APP_PATH = '{package}.interface.grpc:app'

# gRPC Extension APIs
GRPC_EXTENSION_SERVICERS = {
    'spaceone.core.pygrpc.extension.grpc_health': ['GRPCHealth'],
    'spaceone.core.pygrpc.extension.server_info': ['ServerInfo']
}

# REST Configuration
REST_APP_PATH = '{package}.interface.rest:app'

# REST Application Options
REST_TITLE = ''
REST_DESCRIPTION = ''
REST_CONTACT = {}

# REST Middlewares
REST_MIDDLEWARES = []

# REST Extension Routers
REST_EXTENSION_ROUTERS = [
    {
        'router_path': 'spaceone.core.fastapi.extension.health:router',
        'router_options': {}
    }
]

# REST Uvicorn Options
UVICORN_OPTIONS = {
    'factory': True
}

# Plugin Configuration
PLUGIN_APP_PATH = '{package}.main:app'

# Handler Configuration
HANDLERS = {
    'authentication': [],
    'authorization': [],
    'mutation': [],
    'event': []
}

# Logging Configuration
LOG = {}

# OpenTelemetry Configuration
OTEL = {
    'endpoint': None
}

# Database Configuration
DATABASE_MODEL_PATH = 'model'
DATABASE_AUTO_CREATE_INDEX = True
DATABASE_NAME_PREFIX = ''
DATABASES = {
    'default': {
        'engine': 'MongoModel',
        # MongoDB Example
        # 'host': '<host>',
        # 'port': 27017,
        # 'db': '<db>',
        # 'username': '<user>',
        # 'password': '<password>'
    }
}

# Cache Configuration
CACHES = {
    'default': {
        'engine': 'RedisCache',
        # Redis Example
        # 'engine': 'RedisCache',
        # 'host': '<host>',
        # 'port': 6379,
        # 'db': 0
    },
    'local': {
        'engine': 'LocalCache',
        'max_size': 128,
        'ttl': 300
    }
}
