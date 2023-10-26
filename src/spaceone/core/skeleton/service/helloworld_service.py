from spaceone.core.service import *
from ..manager.helloworld_manager import HelloWorldManager

__all__ = ['HelloWorldService']


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class HelloWorldService(BaseService):

    @transaction
    @check_required(['name'])
    def say_hello(self, params):
        helloworld_mgr = HelloWorldManager()
        return helloworld_mgr.say_hello(params['name'])
