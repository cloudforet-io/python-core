from spaceone.api.sample.v1 import helloworld_pb2, helloworld_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class HelloWorld(BaseAPI, helloworld_pb2_grpc.HelloWorldServicer):

    pb2 = helloworld_pb2
    pb2_grpc = helloworld_pb2_grpc

    def say_hello(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('HelloWorldService', metadata) as helloworld_svc:
            return self.locator.get_info('HelloWorldInfo', helloworld_svc.say_hello(params))
