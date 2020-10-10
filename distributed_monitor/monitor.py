import logging
from threading import Event, Lock
from collections import deque
from typing import Any, Optional

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
        self.unlock_guard = Lock()
        self.sync_obj = sync_obj
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

    def lock(self):
        self.unlock_guard.acquire()
        # send request
        # wait for replies (sem)
        # logger.debug('requesting lock')
        self.reply_counter = 0
        self.requesting = True
        self.req_timestamp = self.conn.request(self.id)
        self.unlock_guard.release()
        self.lock_event.wait()
        self.requesting = False
        logger.debug('lock acquired')

    def unlock(self):
        self.unlock_guard.acquire()
        # reply for first queued request
        logger.debug('releasing lock')
        self.lock_event.clear()
        while True:
            try:
                peer = self.request_queue.popleft()
                self.conn.reply(self.id, peer)
            except IndexError:
                break
        self.unlock_guard.release()
