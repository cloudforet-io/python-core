# Database Settings
DATABASES = {
    'default': {
        # MongoDB Example
        # 'host': '<host>',
        # 'port': 27017,
        # 'db': '<db>',
        # 'username': '<user>',
        # 'password': '<password>'
    }
}

# Cache Settings
CACHES = {
    'default': {
        # Redis Example
        # 'host': '<host>',
        # 'port': 6379,
        # 'db': 0
    }
}

# Handler Configuration
HANDLERS = {
    'authentication': [
        # Default Authentication Handler
        # {
        #     'backend': 'spaceone.core.handler.authentication_handler:AuthenticationGRPCHandler',
        #     'uri': 'grpc://identity:50051/v1/Domain/get_public_key'
        # }
    ],
    'authorization': [
        # Default Authorization Handler
        # {
        #     'backend': 'spaceone.core.handler.authorization_handler:AuthorizationGRPCHandler',
        #     'uri': 'grpc://identity:50051/v1/Authorization/verify'
        # }
    ],
    'mutation': [],
    'event': []
}

# Connector Settings
CONNECTORS = {
}

# Log Settings
LOG = {
    # 'loggers': {
    #     '<package>': {
    #         'level': 'DEBUG',
    #         'handlers': ['console']
    #     }
    # },
    # 'filters': {
    #     'masking': {
    #         'rules': {
    #             'HelloWorld.say_hello': [
    #                 'name'
    #             ]
    #         }
    #     }
    # }
}
