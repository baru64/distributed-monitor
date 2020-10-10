import logging
import threading
from enum import Enum
from typing import Dict, Optional, List, Union
from time import time

import zmq

from .dist_mutex import DistMutex
from .monitor import Monitor


logger = logging.getLogger(__name__)


class MessageType(int, Enum):
    Request = 1
    Reply = 2
    Update = 3
    Notify = 4


class Message:

    def __init__(self,
                 mutex_id: str,
                 msg_type: MessageType,
                 address: str,
                 timestamp: Optional[float] = None,
                 obj: Dict = None):
        self.mutex_id = mutex_id
        self.msg_type = msg_type
        self.address = address
        if timestamp is None:
            self.timestamp = time()
        else:
            self.timestamp = timestamp
        self.obj = obj

    @staticmethod
    def to_dict(message) -> Dict:
        return {
            'mutex_id': message.mutex_id,
            'msg_type': message.msg_type,
            'address': message.address,
            'timestamp': message.timestamp,
            'obj': message.obj
        }

    @staticmethod
    def from_dict(data: Dict):
        return Message(
            data['mutex_id'],
            data['msg_type'],
            data['address'],
            data['timestamp'],
            data['obj'])


class ConnectionManager:

    def __init__(self, id: int, peers: List, bind_address: str, address: str):
        self.id = id
        self.context = zmq.Context()
        self.peers = peers
        self.address = address
        self.bind_address = bind_address
        self.mutexes = {}
        self.send_socket = self.context.socket(zmq.PUSH)
        self.recv_socket = self.context.socket(zmq.PULL)
        self.waiters = {}

        self.receiver = threading.Thread(target=self._receive)
        self.receiver.start()

    def register(self, mutex: Union[DistMutex, Monitor]):
        self.mutexes[mutex.id] = mutex

    def get_event(self, mutex_id: str) -> threading.Event:
        self.waiters[mutex_id] = threading.Event()
        self.waiters[mutex_id].clear()
        return self.waiters[mutex_id]

    def notifyAll(self, mutex_id):
        # broadcast notification
        notification = Message(mutex_id, MessageType.Notify, self.address)
        for peer in self.peers:
            self.send_socket.connect(peer)
            self.send_socket.send_json(Message.to_dict(notification))
            self.send_socket.disconnect(peer)

    def request(self, mutex_id: str) -> float:
        # broacast request
        request = Message(mutex_id, MessageType.Request, self.address)
        logger.info(f'peer {self.id} requesting lock')
        for peer in self.peers:
            self.send_socket.connect(peer)
            self.send_socket.send_json(Message.to_dict(request))
            self.send_socket.disconnect(peer)
        return request.timestamp

    def reply(self, mutex_id: str, peer_address: str):
        # reply to peer
        logger.info(f'peer {self.id} reply to {peer_address[-1]}')
        reply = Message(mutex_id, MessageType.Reply, self.address)
        self.send_socket.connect(peer_address)
        self.send_socket.send_json(Message.to_dict(reply))
        self.send_socket.disconnect(peer_address)

    def update(self, mutex_id):
        # broadcast new state to other peers
        logger.info(f'peer {self.id} updating obj, mutex_id: {mutex_id}')
        obj = self.mutexes[mutex_id].sync_obj.to_dict(
            self.mutexes[mutex_id].sync_obj
        )
        update = Message(mutex_id, MessageType.Update, self.address, obj=obj)
        for peer in self.peers:
            self.send_socket.connect(peer)
            self.send_socket.send_json(Message.to_dict(update))
            self.send_socket.disconnect(peer)

    def _receive(self):
        # handle messages
        self.recv_socket.bind(self.bind_address)
        while True:
            msg_data = self.recv_socket.recv_json()
            message = Message.from_dict(msg_data)
            logger.debug(
                f'peer {self.id} received: type:{message.msg_type} '
                f'from:{message.address[-1]} {message.timestamp}'
            )

            # reply message
            if message.msg_type == MessageType.Reply:
                self.mutexes[message.mutex_id].reply_counter += 1
                logger.debug(
                    f'peer {self.id} mutex:{message.mutex_id} replies:'
                    f'{self.mutexes[message.mutex_id].reply_counter}'
                )
                if (self.mutexes[message.mutex_id].reply_counter
                        == len(self.peers)):
                    # allow locking local mutex
                    self.mutexes[message.mutex_id].lock_event.set()

            # request message
            elif message.msg_type == MessageType.Request:
                self.mutexes[message.mutex_id].guard.acquire()

                # Ricart-Agrawala algorithm
                if not self.mutexes[message.mutex_id].requesting and \
                        not self.mutexes[message.mutex_id].lock_event.is_set():
                    # not requesting and not in critical section
                    self.reply(message.mutex_id, message.address)
                elif self.mutexes[message.mutex_id].requesting and \
                    self.mutexes[message.mutex_id].req_timestamp \
                        > message.timestamp:
                    # requesting but later than received request
                    self.reply(message.mutex_id, message.address)
                else:
                    # defer replies
                    logger.debug(
                        f'peer {self.id} queue p{message.address[-1]} r:'
                        f'{self.mutexes[message.mutex_id].requesting} cs:'
                        f'{self.mutexes[message.mutex_id].lock_event.is_set()}'
                        f'{self.mutexes[message.mutex_id].req_timestamp}>'
                        f' {message.timestamp}')
                    self.mutexes[message.mutex_id].request_queue \
                        .append(message.address)

                self.mutexes[message.mutex_id].guard.release()

            # update message
            elif message.msg_type == MessageType.Update:
                self.mutexes[message.mutex_id].sync_obj = \
                    self.mutexes[message.mutex_id].sync_obj \
                        .from_dict(message.obj)

            # notification message
            elif message.msg_type == MessageType.Notify:
                event = self.waiters.pop(message.mutex_id, None)
                if event is not None:
                    event.set()
