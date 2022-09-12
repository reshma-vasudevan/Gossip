import socket, logging
from threading import Thread, Lock
import gossip.exceptions as e
import gossip.codes as c
from gossip.message import *
from gossip.utils import parse_header


############################ API ############################

class APIServerThread(Thread):
    """Server thread for the API. Accepts connections and creates new API client threads.
    """
    def __init__(self, address, port, connections, queue, message_storage):
        """Constructor.

        :param address: address to bind to
        :param port: port to bind to
        :param connections: dict of active connections with address as key
        :param queue: Queue to put received announce messages
        :param message_storage: cache to store messages and subscribers
        """
        Thread.__init__(self)
        self.address = address
        self.port = port
        self.queue = queue
        self.message_storage = message_storage
        self.connections = connections

    def run(self):
        logging.info("Started API Server at {}:{}" \
                     .format(self.address, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.address, self.port))

            while True:
                s.listen(5)
                (conn, (ip, port)) = s.accept()

                c = APIClientThread(conn,
                                    self.address,
                                    self.port,
                                    ip,
                                    port,
                                    self.queue,
                                    self.message_storage,
                                    self.connections)

                c.start()
            s.close()
        except:
            logging.error("API server crashed at {}:{}" \
                          .format(self.address, self.port))


class APIClientThread(Thread):
    """
        Client thread to handle a client that connects to API server
    """
    def __init__(self, connection, ip, port, oip, oport, queue, message_storage, connections):
        """Constructor.

        :param connection: connection to use
        :param ip: address to bind to
        :param port: port to bind to
        :param oip: address of the requesting client
        :param oport: port of the requesting client
        :param queue: Queue to put received announce messages
        :param message_storage: cache to store messages and subscribers
        :param connections: dict of active connections with address as key
        """
        Thread.__init__(self)
        self.connection = connection
        self.ip = ip
        self.port = port
        self.oip = oip
        self.oport = oport
        self.queue = queue
        self.message_storage = message_storage
        self.connections = connections
        self.lock = Lock()

    def run(self):
        logging.info("Connection from {}:{}" \
                     .format(self.ip, self.port))
        logging.info("Started API Client for {}:{}" \
                     .format(self.oip, self.oport))
        oaddr = self.oip + ":" + str(self.oport)
        with self.lock:
            self.connections[oaddr] = self.connection
        try:
            while True:
                msg = parse_header(self.connection)
                msg_type = msg["type"]

                if msg_type == c.GOSSIP_ANNOUNCE:
                    # if announce message, add it to shared queue
                    self.queue.put({'message':msg["data"], 'resend': True})

                elif msg_type == c.GOSSIP_NOTIFY:
                    # if notify, add ip address of sender to subscriber list
                    message = NotifyMessage(msg["data"])
                    with self.lock:
                        self.message_storage.add_subscriber(message.data_type, oaddr)
                elif msg_type == c.GOSSIP_VALIDATION:
                    # if validation, if false, update validity of message
                    message = ValidationMessage(msg["data"])
                    if not message.valid:
                        with self.lock:
                            self.message_storage.make_invalid(message.msg_id)

        except e.ClientDisconnected as error:
            logging.debug("Client disconnected: {}".format(error))
        except e.InvalidHeader as error:
            logging.error("Invalid header: {}".format(error))
        except e.InvalidSize as error:
            logging.error("Invalid size: {}".format(error))
        except e.InvalidMessageType as error:
            logging.error("Invalid message type: {}".format(error))
        except Exception as error:
            logging.error("API Client crashed: {}".format(error))
        with self.lock:
            self.connections.pop(oaddr)
        self.connection.close()
        logging.info("API Client completed {}:{}" \
                     .format(self.oip, self.oport))


############################ P2P ############################

class P2PServerThread(Thread):
    """Server thread for P2P. Accepts connections and creates new P2P client threads.
    """
    def __init__(self, address, port, connections, incoming_queue, p2p_queue):
        """Constructor.

        :param address: address to bind to
        :param port: port to bind to
        :param connections: dict of active connections with address as key
        :param incoming_queue: queue to put messages received from other peers
        :param p2p_queue: queue to put messages for internal processing
        """
        Thread.__init__(self)
        self.address = address
        self.port = port
        self.connections = connections
        self.incoming_queue = incoming_queue
        self.p2p_queue = p2p_queue

    def run(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.address, self.port))

            logging.info("Started P2P Server at {}:{}" \
                         .format(self.address, self.port))

            while True:
                s.listen(5)
                (conn, (ip, port)) = s.accept()

                c = P2PClientThread(conn,
                                    ip,
                                    port,
                                    self.connections,
                                    self.incoming_queue)

                c.start()
            s.close()
        except:
            logging.error("P2P server crashed at {}:{}" \
                          .format(self.address, self.port))
        finally:
            s.close()


class P2PClientThread(Thread):
    """
        Client thread to handle a client that connects to P2P server
    """
    def __init__(self, connection, oip, oport, connections, incoming_queue):
        """Constructor.

        :param connection: connection to use
        :param oip: address of the requesting client
        :param oport: port of the requesting client
        :param connections: dict of active connections with address as key
        :param incoming_queue: queue to put messages received from other peers
        """
        Thread.__init__(self)
        self.connection = connection
        self.oip = oip
        self.oport = oport
        self.connections = connections
        self.incoming_queue = incoming_queue
        self.lock = Lock()

    def run(self) -> None:
        logging.info("Started P2P Client for {}:{}" \
                     .format(self.oip, self.oport))
        oaddr = self.oip + ":" + str(self.oport)

        with self.lock:
            self.connections[oaddr] = {'connection': self.connection, 'p2p_server_address': oaddr}

        try:
            while True:
                msg = parse_header(self.connection)
                msg_type = msg["type"]

                # add p2p message received on the connection to shared queue
                self.incoming_queue.put({'sender': oaddr, 'msg_type': msg_type, 'msg_body': msg["data"]})

        except e.ClientDisconnected as error:
            logging.debug("Client disconnected")
        except e.InvalidHeader as error:
            logging.error("Invalid header: {}".format(error))
        except e.InvalidSize as error:
            logging.error("Invalid size: {}".format(error))
        except e.InvalidMessageType as error:
            logging.error("Invalid message type: {}".format(error))
        except Exception as error:
            logging.error("P2P Client crashed: {}".format(error))
        with self.lock:
            self.connections.pop(oaddr)
        self.connection.close()
        logging.info("P2P Client completed {}:{}" \
                     .format(self.oip, self.oport))
        self.incoming_queue.put({'sender':oaddr, 'msg_type': c.P2P_CONNECTION_CLOSED, 'msg_body': None})