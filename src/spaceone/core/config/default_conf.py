# Service Description
TITLE = ''
DESCRIPTION = ''
CONTACT = {
}

# Service Configuration
PACKAGE = None
SERVICE = None

# Server Configuration
PORT = 50051
HOST = '127.0.0.1'

MAX_WORKERS = 100
MAX_MESSAGE_LENGTH = 1024 * 1024 * 1024

# Unit Test Configuration
MOCK_MODE = False

# gRPC Extension APIs
EXTENSION_APIS = {
    'spaceone.core.extension.grpc_health': ['GRPCHealth'],
    'spaceone.core.extension.server_info': ['ServerInfo']
}

# Uvicorn Options for Rest Server
UVICORN_OPTIONS = {
    'factory': True
}

# Rest Middlewares
REST_MIDDLEWARES = []

REST_EXTENSION_ROUTERS = [
    {
        'router_path': 'spaceone.core.fastapi.extension.health:router',
        'router_options': {}
    },
    {
        'router_path': 'spaceone.core.fastapi.extension.reflection:router',
        'router_options': {}
    }
]

# Handler Configuration
HANDLERS = {
    'authentication': [],
    'authorization': [],
    'mutation': [],
    'event': []
}

HANDLER_EXCLUDE_APIS = {
    'authentication': [],
    'authorization': [],
    'mutation': [],
    'event': []
}

# Logging Configuration
ENABLE_STACK_INFO = False
LOG = {}

# OpenTelemetry Configuration
OTEL = {
    'endpoint': None
}

# Database Configuration
DATABASE_AUTO_CREATE_INDEX = True
DATABASE_NAME_PREFIX = ''
DATABASES = {
    'default': {}
}

# Cache Configuration
CACHES = {
    'default': {},
    'local': {
        'backend': 'spaceone.core.cache.local_cache.LocalCache',
        'max_size': 128,
        'ttl': 300
    }
}
