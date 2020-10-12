import logging
from threading import Event, Lock
from collections import deque
from typing import Any, Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class NotSerializableObject(Exception):
    pass


class Monitor:

    def __init__(self, conn: Any, id: str, sync_obj: Any):
        self.id = id
        self.conn = conn
        self.conn.register(self)
        self.lock_event = Event()
        self.lock_event.clear()
        self.reply_counter = 0
        self.request_queue = deque()
        self.requesting = False
        self.req_timestamp: float = 0
        self.guard = Lock()
        self.sync_obj = sync_obj
        self.is_in_synchronized = False
        if getattr(self.sync_obj, 'to_dict', None) is None:
            raise NotSerializableObject
        if getattr(self.sync_obj, 'from_dict', None) is None:
            raise NotSerializableObject

    def call(self, func: str, *args, **kwargs) -> Any:
        self.lock()
        method = getattr(self.sync_obj, func)
        ret = method(*args, **kwargs)
        self.conn.update(self.id)
        self.unlock()
        return ret

    def notifyAll(self):
        self.conn.notifyAll(self.id)

    def wait(self):
        if self.is_in_synchronized:
            self.conn.update(self.id)
            self.unlock()

        event = self.conn.get_event(self.id)
        event.wait()

        if self.is_in_synchronized:
            self.lock()

    @contextmanager
    def get_synchronized(self):
        self.is_in_synchronized = True
        self.lock()
        yield self.sync_obj
        self.conn.update(self.id)
        self.unlock()
        self.is_in_synchronized = False

    def lock(self):
        self.guard.acquire()
        self.reply_counter = 0
        self.requesting = True
        self.req_timestamp = self.conn.request(self.id)
        self.guard.release()
        self.lock_event.wait()
        self.requesting = False
        logger.debug('lock acquired')

    def unlock(self):
        self.guard.acquire()
        logger.debug('releasing lock')
        self.lock_event.clear()
        while True:
            try:
                peer = self.request_queue.popleft()
                self.conn.reply(self.id, peer)
            except IndexError:
                break
        self.guard.release()
