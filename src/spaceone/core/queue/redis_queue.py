import redis
import time

from spaceone.core.error import *
from spaceone.core.queue import BaseQueue

class RedisQueue(BaseQueue):
    """
    This is multiprocessing Queue for python multiprocessing
    Does not support Queue between different processes
    """
    def __init__(self, conf):
        self._conf = conf
        try:
            self.channel = conf.pop('channel')
            pool = redis.ConnectionPool(**conf)
            self.conn = redis.Redis(connection_pool=pool)
            self.conn.ping()

            self.pubsub = self.conn.pubsub()
            self.pubsub.subscribe(self.channel)
            self.initialized = True
        except Exception:
            self.initialized = False
            raise ERROR_CONFIGURATION(key="backend")

    def initialize(self):
        """ Sometime initilization may be fail when creation time
        Use re initialize function
        """
        try:
            self.channel = self._conf.pop('channel')
            pool = redis.ConnectionPool(**self._conf)
            self.conn = redis.Redis(connection_pool=pool)
            self.conn.ping()

            self.pubsub = self.conn.pubsub()
            self.pubsub.subscribe(self.channel)
            self.initialized = True
        except Exception:
            self.initialized = False
            raise ERROR_CONFIGURATION(key="backend")


    ######################
    # FIFO Queue
    ######################
    def put(self, item):
       self.conn.rpush(self.channel, item) 

    def get(self):
        """
        blpop waits until item occurs
        """
        item = self.conn.blpop(self.channel)
        return item[1]

    ######################
    # Pub / Sub
    ######################
    def subscribe(self):
        # This is redis.subscribe
        while True:
            m = self.pubsub.get_message()
            if m != None:
                return m
            time.sleep(0.01)

    def publish(self, item):
        # This is redis.publish
        try:
            return self.conn.publish(self.channel, item)
        except Exception as e:
            raise ERROR_UNKNOWN(message=e)
