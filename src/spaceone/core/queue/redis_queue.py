import redis
import time
import logging

from spaceone.core.error import *
from spaceone.core.queue import BaseQueue

_LOGGER = logging.getLogger(__name__)

# Wait max 1 minute to recover
MAX_TRY = 6
WAIT_INTERVAL = 10


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
            for i in range(MAX_TRY):
                _LOGGER.error(f"######### queue connection check : {i} ###########")
                self.initialize()
                if self.initialized:
                    return
                time.sleep(WAIT_INTERVAL)
            self.initialized = False
            _LOGGER.error(f"run without configuration for late recover: {conf}")
            #raise ERROR_CONFIGURATION(key="backend")

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

        return self.initialized


    ######################
    # FIFO Queue
    ######################
    def put(self, item):
        try:
            self.conn.rpush(self.channel, item)

        except redis.exceptions.ConnectionError as e:
            _LOGGER.error("####### Redis Queue put failed #############")
            _LOGGER.error(f"Redis connection error, reconnect after {WAIT_INTERVAL} sec ....")
            time.sleep(WAIT_INTERVAL)
            self.initialize()

        except Exception as e:
            _LOGGER.error("######## Contact to admin ##########")
            _LOGGER.error(f"Unknown error: {e}")


    def get(self):
        """
        blpop waits until item occurs
        """
        try:
            item = self.conn.blpop(self.channel)
            return item[1]
        except redis.exceptions.ConnectionError as e:
            _LOGGER.error("####### Redis Queue get failed #############")
            _LOGGER.error(f"Redis connection error, reconnect after {WAIT_INTERVAL} sec ....")
            time.sleep(WAIT_INTERVAL)
            self.initialize()

        except Exception as e:
            _LOGGER.error("######## Contact to admin ##########")
            _LOGGER.error(f"Unknown error: {e}")

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
