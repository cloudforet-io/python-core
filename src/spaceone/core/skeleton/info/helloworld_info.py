from spaceone.api.sample.v1 import helloworld_pb2

__all__ = ['HelloWorldInfo']


def HelloWorldInfo(message):
    info = {
        'message': message
    }

    return helloworld_pb2.HelloReply(**info)
