from spaceone.core.service import *

__all__ = ['HelloWorldService']


@authentication_handler
@authorization_handler
@event_handler
class HelloWorldService(BaseService):

    @transaction
    @check_required(['name'])
    def say_hello(self, params):
        helloworld_mgr = self.locator.get_manager('HelloWorldManager')
        return helloworld_mgr.say_hello(params['name'])
