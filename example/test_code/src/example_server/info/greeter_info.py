from spaceone.core.pygrpc.message_type import *
from ..api.v1.helloworld_pb2 import HelloReply as HelloReplyProto

__all__ = ['HelloReply']


def HelloReply(vo):
    return HelloReplyProto(message=vo['message'])

