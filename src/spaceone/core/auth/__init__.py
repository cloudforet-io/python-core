import abc


class Authenticator(object):

    def __init__(self, key):
        self._key = key

    @abc.abstractmethod
    def validate(self, token):
        raise NotImplementedError('Valid must be implemented')
