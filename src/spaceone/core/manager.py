from spaceone.core.base import CoreObject


class BaseManager(CoreObject):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
