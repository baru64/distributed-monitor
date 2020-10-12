import logging
import sys
from typing import List
from multiprocessing import Process

from distributed_monitor.connection_manager import ConnectionManager
from distributed_monitor.dist_mutex import DistMutex

logger = logging.getLogger(__name__)


def peer(id: int, others: List, bind_address: str, address: str):
    cm = ConnectionManager(id, others, bind_address, address)
    dmut = DistMutex(cm, 'test')
    for i in range(10):
        dmut.lock()
        print(f'---> PEER {id} IN CRITICAL SECTION N:{i}')
        dmut.unlock()


if __name__ == '__main__':
    # set logging
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
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
