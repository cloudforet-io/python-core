# Service Configuration
PACKAGE = None
SERVICE = None

# Server Configuration
SERVER_TYPE = None
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

# Logging Configuration
SET_LOGGING = True
LOG = {}

REST = {}
