import logging
import threading
from enum import Enum
from typing import Dict, Optional, List
from time import time

import zmq

from .dist_mutex import DistMutex


logger = logging.getLogger(__name__)


class MessageType(int, Enum):
    Request = 1
    Reply = 2


class Message:

    def __init__(self,
                 mutex_id: str,
                 msg_type: MessageType,
                 address: str,
                 timestamp: Optional[float] = None):
        self.mutex_id = mutex_id
        self.msg_type = msg_type
        self.address = address
        if timestamp is None:
            self.timestamp = time()
        else:
            self.timestamp = timestamp

    @staticmethod
    def to_dict(message) -> Dict:
        return {
            'mutex_id': message.mutex_id,
            'msg_type': message.msg_type,
            'address': message.address,
            'timestamp': message.timestamp
        }

    @staticmethod
    def from_dict(data: Dict):
        return Message(
            data['mutex_id'],
            data['msg_type'],
            data['address'],
            data['timestamp'])


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

        self.receiver = threading.Thread(target=self._receive)
        self.receiver.start()

    def register(self, mutex: DistMutex):
        self.mutexes[mutex.id] = mutex

    def request(self, mutex_id: str) -> float:
        # broacast request
        request = Message(mutex_id, MessageType.Request, self.address)
        logger.info(f'peer {self.id} requesting lock '
                    f'ts:{request.timestamp}')
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

    def _receive(self):
        # handle requests
        logger.info(f'peer {self.id} binds to {self.bind_address}')
        self.recv_socket.bind(self.bind_address)
        while True:
            msg_data = self.recv_socket.recv_json()
            message = Message.from_dict(msg_data)
            logger.debug(
                f'peer {self.id} received: type:{message.msg_type} '
                f'from:{message.address[-1]} {message.timestamp}'
            )
            if message.msg_type == MessageType.Reply:
                self.mutexes[message.mutex_id].reply_counter += 1
                logger.info(f'peer {self.id} mutex:{message.mutex_id} replies:'
                            f'{self.mutexes[message.mutex_id].reply_counter}')
                if (self.mutexes[message.mutex_id].reply_counter
                        == len(self.peers)):
                    # allow locking local mutex
                    logger.debug(
                        f'peer {self.id} received {len(self.peers)} replies'
                    )
                    self.mutexes[message.mutex_id].lock_event.set()

            elif message.msg_type == MessageType.Request:
                self.mutexes[message.mutex_id].unlock_guard.acquire()
                if not self.mutexes[message.mutex_id].requesting and \
                        not self.mutexes[message.mutex_id].lock_event.is_set():
                    # send reply
                    self.reply(message.mutex_id, message.address)
                elif self.mutexes[message.mutex_id].requesting and \
                    self.mutexes[message.mutex_id].req_timestamp \
                        > message.timestamp:
                    logger.debug(
                        f'p{self.id} timestamp diff: '
                        f'{self.mutexes[message.mutex_id].req_timestamp}>'
                        f'{message.timestamp}')
                    self.reply(message.mutex_id, message.address)
                else:
                    logger.debug(
                        f'peer {self.id} queue p{message.address[-1]} r:'
                        f'{self.mutexes[message.mutex_id].requesting} cs:'
                        f'{self.mutexes[message.mutex_id].lock_event.is_set()}'
                        f'{self.mutexes[message.mutex_id].req_timestamp}>'
                        f' {message.timestamp}')
                    self.mutexes[message.mutex_id].request_queue \
                        .append(message.address)
                self.mutexes[message.mutex_id].unlock_guard.release()
