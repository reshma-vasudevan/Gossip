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
    def __init__(self, address, port, queue, message_storage, connections):
        """Constructor.

        :param address: address to bind to
        :param port: port to bind to
        :param type: server type, either API or P2P
        :param queue: Queue to put received announce messages
        :param message_storage: cache to store messages and subscribers
        :param connections: dict of active connections with address as key
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
                logging.info("starting client")

                c = APIClientThread(conn,
                                    self.address,
                                    self.port,
                                    ip,
                                    port,
                                    self.queue,
                                    self.message_storage,
                                    self.connections)

                logging.info("starting client")
                c.start()
        except:
            logging.error("API server crashed at {}:{}" \
                          .format(self.address, self.port))


class APIClientThread(Thread):
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
                    announce_message = AnnounceMessage(msg["data"])
                    self.queue.put(announce_message)

                    # TODO send to peer layer from where we can send to all other peers

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
            logging.debug("Client disconnected")
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
    def __init__(self, address, port, connections, incoming_queue, p2p_queue, bootstrapper_address, bootstrapper_port):
        """Constructor.

        :param address: address to bind to
        :param port: port to bind to
        """
        Thread.__init__(self)
        self.address = address
        self.port = port
        self.connections = connections
        self.incoming_queue = incoming_queue
        self.p2p_queue = p2p_queue
        self.bootstrapper_address = bootstrapper_address
        self.bootstrapper_port = bootstrapper_port

    def run(self):
        logging.info("Started P2P Server at {}:{}" \
                     .format(self.address, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.address, self.port))

            logging.info("Started P2P Server at {}:{}" \
                         .format(self.address, self.port))
            # TODO move bootstrap inside client handler as well?
            # connect to a bootstrapper and get first list of peers
            bootstrapper = "{}:{}".format(self.bootstrapper_address, self.bootstrapper_port)
            created_pull_message = GossipPullMessage(self_ip=self.address, self_port=self.port).prepare_message()
            self.p2p_queue.put(
                {'action': P2P_ACTION_PULL_RESPONSE, 'to_address': bootstrapper, 'message': created_pull_message})

            while True:
                s.listen(5)
                (conn, (ip, port)) = s.accept()
                logging.info("starting client")

                c = P2PClientThread(conn,
                                    ip,
                                    port,
                                    self.connections,
                                    self.incoming_queue)

                logging.info("starting client")
                c.start()
        except:
            logging.error("P2P server crashed at {}:{}" \
                          .format(self.address, self.port))


# TODO: write P2PClientThread
class P2PClientThread(Thread):
    def __init__(self, connection, oip, oport, connections, incoming_queue):
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
            self.connections[oaddr] = self.connection

        try:
            while True:
                msg = parse_header(self.connection)
                msg_type = msg["type"]

                self.incoming_queue.put({'msg_type': msg_type, 'msg_body': msg["data"]})

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
