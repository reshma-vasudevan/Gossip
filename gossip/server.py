import socket, logging
from threading import Thread
from interface import identify_msg_type, notification
import exceptions as e
import codes as c

class ServerThread(Thread):
    """Server thread for the API. Accepts connections and creates new API client threads.
    """
    def __init__(self, stype, address, port):
        """Constructor.

        :param address: address to bind to
        :param port: port to bind to
        :param type: server type, either API or P2P
        """
        Thread.__init__(self)
        self.address = address
        self.port = port
        self.stype = stype

    def run(self):
        logging.info("Started {} Server at {}:{}" \
                     .format(self.stype, self.address, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.address, self.port))
            while True:
                s.listen(5)
                (conn, (ip, port)) = s.accept()
                logging.info("starting client")
                if self.stype=="API":
                    c = APIClientThread(conn, self.address, self.port, ip, port)
                elif self.stype=="P2P":
                    c = APIClientThread(conn, self.address, self.port, ip, port)
                logging.info("starting client")
                c.start()
        except:
            logging.error("{} server crashed at {}:{}" \
                          .format(self.stype, self.address, self.port))

class APIClientThread(Thread):
    def __init__(self, connection, ip, port, oip, oport):
        """Constructor.

        :param connection: connection to use
        :param ip: address to bind to
        :param port: port to bind to
        :param oip: address of the requesting client
        :param oport: port of the requesting client
        """
        Thread.__init__(self)
        self.connection = connection
        self.ip=ip
        self.port=port
        self.oip=oip
        self.oport=oport

    def run(self):
        logging.info("Connection from {}:{}" \
                     .format(self.ip, self.port))
        logging.info("Started API Client for {}:{}" \
                     .format(self.oip, self.oport))
        try:
            while True:
                msg = api_accept(self.connection)

                #r = identify_msg_type(msg["size"] + msg["type"] + msg["data"])
                # TODO do stuff

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
        self.connection.close()
        logging.info("API Client completed {}:{}" \
                     .format(self.oip, self.oport))

def api_accept(conn):
    """Reads from conn and parses header

    :param conn: connection to receive from
    :return: msg header, size and data in a dictonary
    """
    header = conn.recv(4)
    if len(header) == 0:
        raise e.ClientDisconnected("")
    if len(header) < 4:
        raise e.InvalidHeader("Length of header: {}".format(len(header)))

    size = int.from_bytes(header[:2], byteorder='big')
    msgtype = int.from_bytes(header[2:4], byteorder='big')
    if size < 4:
        raise e.InvalidSize("Size: {}".format(size))
    if msgtype > c.MAX or msgtype < c.MIN:
        raise e.InvalidMessageType("Message Type: {}".format(msgtype))

    msg=conn.recv(size - 4)
    print(msg)
    print(size)
    print(msgtype)
    return {"type": msgtype, "size": size, "data": msg}
