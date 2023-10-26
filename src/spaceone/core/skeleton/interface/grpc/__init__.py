from spaceone.core.pygrpc.server import GRPCServer
from .helloworld import HelloWorld

_all_ = ['app']

app = GRPCServer()
app.add_service(HelloWorld)
