from spaceone.api.report.v1 import domain_pb2, domain_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Domain(BaseAPI, domain_pb2_grpc.DomainServicer):
    pb2 = domain_pb2
    pb2_grpc = domain_pb2_grpc

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DomainService', metadata) as svc:
            return self.locator.get_info('DomainInfo', svc.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DomainService', metadata) as svc:
            svc_vos, total_count = svc.list(params)
            return self.locator.get_info('DomainsInfo', svc_vos, total_count)

    def enable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DomainService', metadata) as svc:
            return self.locator.get_info('DomainInfo', svc.enable(params))

    def disable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DomainService', metadata) as svc:
            svc.disable(params)
            return self.locator.get_info('EmptyInfo')
