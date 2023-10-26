from spaceone.core.base import CoreObject


class BaseManager(CoreObject):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
