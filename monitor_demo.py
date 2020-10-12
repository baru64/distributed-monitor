import logging
import sys
from typing import List, Dict
from multiprocessing import Process

from distributed_monitor.connection_manager import ConnectionManager
from distributed_monitor.monitor import Monitor


logger = logging.getLogger(__name__)


class SerializableCounter:

    def __init__(self, value: int = 0):
        self.value = value

    def add(self, n: int):
        self.value += n

    @staticmethod
    def to_dict(counter) -> Dict:
        return {
            'value': counter.value
        }

    @staticmethod
    def from_dict(data: Dict):
        return SerializableCounter(data['value'])


def peer(id: int, others: List, bind_address: str, address: str):
    cm = ConnectionManager(id, others, bind_address, address)
    counter = SerializableCounter()
    dmonitor = Monitor(cm, 'test', counter)
    for i in range(10):
        dmonitor.call('add', 1)
        print(f'---> {dmonitor.sync_obj.value}')


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

    p1 = Process(target=peer, args=(1, others1, peer1_recv, peer1_send))
    p2 = Process(target=peer, args=(2, others2, peer2_recv, peer2_send))
    p3 = Process(target=peer, args=(3, others3, peer3_recv, peer3_send))

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
