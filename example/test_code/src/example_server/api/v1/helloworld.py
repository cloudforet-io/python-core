from spaceone.core.pygrpc import BaseAPI
from . import helloworld_pb2_grpc,helloworld_pb2


class Greeter(BaseAPI, helloworld_pb2_grpc.GreeterServicer):
    pb2 = helloworld_pb2
    pb2_grpc = helloworld_pb2_grpc

    def SayHello(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('GreeterService', metadata) as svc:
            return self.locator.get_info('HelloReply', svc.hello(params))

    def SayHelloGroup(self, request, context):
        request, metadata = self.parse_request(request, context)

        with self.locator.get_service('GreeterService', metadata) as svc:
            for msg in svc.hello_group(request):
                yield self.locator.get_info('HelloReply', msg)

    def HelloEveryone(self, request_iterator, context):
        requests, metadata = self.parse_request(request_iterator, context)

        with self.locator.get_service('GreeterService', metadata) as svc:
            return self.locator.get_info('HelloReply', svc.hello_everyone(requests))

    def SayHelloOneByOne(self, request_iterator, context):
        params, metadata = self.parse_request(request_iterator, context)

        with self.locator.get_service('GreeterService', metadata) as svc:
            for msg in svc.hello_one_by_one(params):
                yield self.locator.get_info('HelloReply', msg)
