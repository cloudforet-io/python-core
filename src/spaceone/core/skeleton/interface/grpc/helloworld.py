from spaceone.api.sample.v1 import helloworld_pb2, helloworld_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from ...service.helloworld_service import HelloWorldService
from ...info.helloworld_info import HelloWorldInfo


class HelloWorld(BaseAPI, helloworld_pb2_grpc.HelloWorldServicer):

    pb2 = helloworld_pb2
    pb2_grpc = helloworld_pb2_grpc

    def say_hello(self, request, context):
        params, metadata = self.parse_request(request, context)

        with HelloWorldService(metadata) as helloworld_svc:
            return HelloWorldInfo(helloworld_svc.say_hello(params))
