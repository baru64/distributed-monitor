import logging
from threading import Event, Lock
from collections import deque
from typing import Any, Optional

logger = logging.getLogger(__name__)


class DistMutex:

    def __init__(self, conn: Any, id: str):
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

    def lock(self):
        self.guard.acquire()
        self.reply_counter = 0
        self.requesting = True
        self.req_timestamp = self.conn.request(self.id)
        self.guard.release()
        self.lock_event.wait()
        self.requesting = False
        logger.debug('lock acquired')

    def unlock(self, sync_obj: Optional[Any] = None):
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
