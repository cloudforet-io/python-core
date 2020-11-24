import logging

from spaceone.core.service import BaseService, authentication_handler, \
    authorization_handler, event_handler, transaction

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class GreeterService(BaseService):

    @transaction
    def hello(self, params):
        print(f"{params['name']} is request SayHello")
        return {"message": params['name']}

    @transaction
    def hello_group(self, params):
        print(params)
        print(f"{params['name']} is request SayHelloGroup")

        names = ['a', 'b', 'c', 'd']
        for name in names:
            yield dict(message=f"Hello {name}!")

    def hello_everyone(self, params):
        names = []
        for reqs in params:
            names.append(reqs['name'])
        return dict(message=f"Hello everyone {names}!")

    @transaction
    def hello_one_by_one(self, params):
        for req in params:
            name = req['name']
            print(f"{name} say to you hello")
            yield dict(message=f"Hello {name}!")
