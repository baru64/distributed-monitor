import logging
import sys
from typing import List, Dict, Optional
from multiprocessing import Process
from collections import deque

from distributed_monitor.connection_manager import ConnectionManager
from distributed_monitor.monitor import Monitor


logger = logging.getLogger(__name__)


class SerializableBuffer:

    def __init__(self, buffer: List = None):
        if buffer is None:
            self.buffer = deque()
        else:
            self.buffer = deque(buffer)

    def write(self, n: int):
        self.buffer.append(n)

    def read(self) -> Optional[int]:
        if len(self.buffer) == 0:
            return None
        else:
            return self.buffer.popleft()

    @staticmethod
    def to_dict(buff) -> Dict:
        return {
            'buffer': list(buff.buffer)
        }

    @staticmethod
    def from_dict(data: Dict):
        return SerializableBuffer(data['buffer'])


def producer(id: int, others: List, bind_address: str, address: str):
    cm = ConnectionManager(id, others, bind_address, address)
    buff = SerializableBuffer()
    dmonitor = Monitor(cm, 'test', buff)
    for i in range(10):
        dmonitor.call('write', i)
        print(f'producer {id} ---> {i}')
        dmonitor.notifyAll()


def consumer(id: int, others: List, bind_address: str, address: str):
    cm = ConnectionManager(id, others, bind_address, address)
    buff = SerializableBuffer()
    dmonitor = Monitor(cm, 'test', buff)
    while True:
        val = dmonitor.call('read')
        if val is None:
            dmonitor.wait()
            continue
        print(f'consumer ---> {val}')


if __name__ == '__main__':
    # set logging
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger.info('starting demo')
    # set peers
    peer1_recv = "tcp://*:2001"
    peer2_recv = "tcp://*:2002"
    peer3_recv = "tcp://*:2003"
    peer1_send = "tcp://localhost:2001"
    peer2_send = "tcp://localhost:2002"
    peer3_send = "tcp://localhost:2003"
    others1 = [peer2_send, peer3_send]
    others2 = [peer1_send, peer3_send]
    others3 = [peer1_send, peer2_send]

    p1 = Process(target=producer, args=(1, others1, peer1_recv, peer1_send))
    p2 = Process(target=producer, args=(2, others2, peer2_recv, peer2_send))
    p3 = Process(target=consumer, args=(3, others3, peer3_recv, peer3_send))

    try:
        p1.start()
        p2.start()
        p3.start()

        p1.join()
        p2.join()
        p3.join()
    except KeyboardInterrupt:
        p1.terminate()
        p2.terminate()
        p3.terminate()
