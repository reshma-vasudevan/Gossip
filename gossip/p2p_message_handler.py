import logging
import socket
from threading import Thread, Lock
import gossip.codes as c
from gossip.server import P2PClientThread


class P2PMessageHandler(Thread):
    """
        Thread to wait on the p2p queue and process messages
        to either send response to a pull request
        or to broadcast messages to all known peers
    """
    def __init__(self, p2p_queue, p2p_connections, peer_list, incoming_queue):
        Thread.__init__(self)
        self.queue = p2p_queue
        self.lock = Lock()
        self.connections = p2p_connections
        self.peer_list = peer_list
        self.incoming_queue = incoming_queue

    def run(self) -> None:
        while True:
            # Processing one message from p2p queue
            m = self.queue.get()
            logging.info("Got message {}".format(m))
            with self.lock:
                if m['action'] == c.P2P_ACTION_PULL_RESPONSE:
                    logging.info("Starting thread to send")
                    p = PeerSenderThread(m['to_address'], m['message'], self.connections, self.incoming_queue)
                    p.start()
                elif m['action'] == c.P2P_ACTION_SEND_ALL:
                    for peer in self.peer_list:
                        s = PeerSenderThread(peer, m['message'], self.connections, self.incoming_queue)
                        s.start()

            self.queue.task_done()


class PeerSenderThread(Thread):
    """
    Thread to send messages to given peer
    """
    def __init__(self, to_addr, message, connections, incoming_queue):
        Thread.__init__(self)
        self.to_addr = to_addr
        self.message = message
        self.connections = connections
        self.incoming_queue = incoming_queue

    def run(self):
        logging.info("Inside sender thread")
        new_conn = False
        if self.to_addr in self.connections.keys():
            conn = self.connections[self.to_addr]
        else:
            new_conn = True
            # TODO use new server address to build new connection
            host, port = self.to_addr.split(":")
            logging.info("Creating new conn for {} {}".format(host, port))
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect((host, int(port)))
            self.connections[self.to_addr] = conn

            #also start a new client to handle further messages
            c = P2PClientThread(conn,
                                host,
                                port,
                                self.connections,
                                self.incoming_queue)

            logging.info("starting client")
            c.start()

            # TODO dont close new conn add to conn list

        conn.sendall(self.message)


