import logging
from dataclasses import dataclass

from spaceone.core.service import BaseService, append_keyword_filter, append_query_filter, authentication_handler, \
    authorization_handler, check_required, event_handler, transaction

_LOGGER = logging.getLogger(__name__)


@dataclass
class MockDomain:
    domain_id = 'mock'
    register_templates = []
    relation_resources = []


@authentication_handler
@authorization_handler
@event_handler
class DomainService(BaseService):

    @transaction
    @check_required(["domain_id"])
    def get(self, params):
        print('run get_domain')
        return MockDomain()

    @transaction
    @append_query_filter(["domain_id"])
    @append_keyword_filter(["domain_id"])
    def list(self, params):
        print('run list domain')
        return [MockDomain() for _ in range(10)]

    @transaction
    @check_required(["domain_id"])
    def enable(self, params):
        print('run enable domain')
        return MockDomain()

    @transaction
    @check_required(["domain_id"])
    def disable(self, params):
        print('run disalbe domain')
