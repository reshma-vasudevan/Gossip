import logging
import socket
from threading import Thread, Lock
import gossip.codes as c
from gossip.server import P2PClientThread
from gossip.message import GossipSendContentMessage


class P2PMessageHandler(Thread):
    """
        Thread to wait on the p2p queue and process messages
        to either send response to a pull request
        or to broadcast messages to all known peers
    """
    def __init__(self, p2p_queue, p2p_connections, peer_list, incoming_queue, degree):
        Thread.__init__(self)
        self.queue = p2p_queue
        self.lock = Lock()
        self.connections = p2p_connections
        self.peer_list = peer_list
        self.incoming_queue = incoming_queue
        self.degree = degree

    def run(self) -> None:
        while True:
            # Processing one message from p2p queue
            m = self.queue.get()
            logging.info("Received message {}".format(m))
            with self.lock:
                if m['action'] == c.P2P_ACTION_SEND:
                    p = PeerSenderThread(m['to_address'], m['message'], self.connections, self.incoming_queue, self.degree)
                    p.start()
                # Only when messages are announce messages
                elif m['action'] == c.P2P_ACTION_SEND_ALL:
                    # Sending to all open p2p connections
                    a = GossipSendContentMessage(msg_to_send=m['message']).prepare_message(inner_msg_type=c.GOSSIP_ANNOUNCE)
                    for addr in self.connections.keys():
                        logging.info("Sending to {}".format(addr))
                        s = PeerSenderThread(addr, a, self.connections, self.incoming_queue, self.degree)
                        s.start()

            self.queue.task_done()


class PeerSenderThread(Thread):
    """
    Thread to send messages to given peer
    """
    def __init__(self, to_addr, message, connections, incoming_queue, degree):
        Thread.__init__(self)
        self.to_addr = to_addr
        self.message = message
        self.connections = connections
        self.incoming_queue = incoming_queue
        self.degree = degree

    def run(self):
        if self.to_addr in self.connections.keys():
            conn = self.connections[self.to_addr]['connection']
            conn.sendall(self.message)
        else:
            if len(self.connections) < self.degree:
                # TODO use new server address to build new connection
                host, port = self.to_addr.split(":")
                logging.info("Creating new conn for {} {}".format(host, port))
                conn = None
                try:
                    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    conn.connect((host, int(port)))
                    self.connections[self.to_addr] = {'connection': conn, 'p2p_server_address': self.to_addr}
                    conn.sendall(self.message)
                    # also start a new client to handle further messages
                    c = P2PClientThread(conn,
                                        host,
                                        port,
                                        self.connections,
                                        self.incoming_queue)

                    c.start()
                except ConnectionRefusedError as error:
                    logging.error("Connection refused")
                except Exception as error:
                    logging.error("Could not establish connection to {} {}".format(self.to_addr, error))
            else:
                logging.info("Could not add socket for {}, connection limit exceeded".format(self.to_addr))
            logging.info("Exiting thread for {}".format(self.to_addr))



