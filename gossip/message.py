# Classes for the different message types
import struct
from gossip.codes import *
import logging


class AnnounceMessage:
    """
    Class to unpack and store an announce message.
    Accepts the message body as input.
    """
    def __init__(self, message_body):
        self.msg_type = GOSSIP_ANNOUNCE
        self.message_body = message_body
        self.ttl, self.res, self.data_type = struct.unpack(">BBH", message_body[:4])
        self.data = message_body[4:]

    def get_all_data(self):
        return {'ttl': self.ttl, 'res': self.res, 'data_type': self.data_type, 'data': self.data}

    def get_storage_data(self):
        return {'data_type': self.data_type, 'data': self.data, 'ttl': self.ttl}

    def reduce_ttl(self):
        size = 8 + len(self.data)
        modified_ttl = self.ttl
        # If ttl is 0, hops are unlimited
        if self.ttl != 0:
            modified_ttl -= 1
        updated_message = struct.pack(">HHBBH", size, self.msg_type, modified_ttl, self.res, self.data_type)
        updated_message += self.data

        return updated_message


class NotifyMessage:
    """
        Class to unpack and store a notify message.
        Accepts the message body as input.
    """
    def __init__(self, message_body):
        self.res, self.data_type = struct.unpack(">HH", message_body)

    def get_all_data(self):
        return {'res': self.res, 'data_type': self.data_type}


class NotificationMessage:
    """
        Class to pack a notification message.
    """
    def __init__(self, msg_id, data_type, data):
        """
        Constructor
        :param msg_id: Unique identifier of message
        :param data_type: Data type of the data to be sent
        :param data: Data to be sent
        """
        self.msg_type = GOSSIP_NOTIFICATION
        self.msg_id = msg_id
        self.data_type = data_type
        self.data = data
        self.size = 8 + len(data)

    def prepare_message(self):
        self.message = struct.pack(">HHHH", self.size, self.msg_type, self.msg_id, self.data_type)
        self.message += self.data

        return self.message


class ValidationMessage:
    """
        Class to unpack and store a validation message.
        Accepts the message body as input.
    """
    def __init__(self, message_body):
        self.msg_id, self.res, self.valid = struct.unpack(">HB?", message_body)

    def get_all_data(self):
        return {'msg_id': self.msg_id, 'res': self.res, 'valid': self.valid}


class GossipPullMessage:
    """
        Class to handle either receiving of a pull message or sending a pull message
    """
    def __init__(self, self_ip=None, self_port=None, message_body=None):
        if self_ip:
            self.msg_type = GOSSIP_P2P_PULL
            self.ip = self_ip
            self.port = self_port
            self.size = 10
        if message_body:
            self.message = message_body

    def prepare_message(self):
        ip1, ip2, ip3, ip4 = (int(x) for x in self.ip.split("."))
        message = struct.pack(">HHBBBBH", self.size, self.msg_type, ip1, ip2, ip3, ip4, self.port)
        return message

    def get_requester_address(self):
        ip1, ip2, ip3, ip4, port = struct.unpack(">BBBBH", self.message)
        requester_addr = "{}.{}.{}.{}:{}".format(ip1, ip2, ip3, ip4, port)

        return requester_addr


class GossipPullResponseMessage:
    """
        Class to handle Response messages received to our pull request or
        to send a Response to a Pull message received
    """
    def __init__(self, peer_list, message_body=None):
        self.peer_list = peer_list
        if peer_list:
            self.msg_type = GOSSIP_P2P_PULL_RESPONSE
        if message_body:
            self.message = message_body

    def prepare_message(self):
        peer_count = len(self.peer_list)
        size = 4 + 2 + 6*peer_count
        message = struct.pack(">HHH", size, self.msg_type, peer_count)
        for peer in self.peer_list:
            ip, port = peer.split(":")
            ip1, ip2, ip3, ip4 = (int(x) for x in ip.split("."))
            message += struct.pack(">BBBBH", ip1, ip2, ip3, ip4, int(port))

        return message

    def update_peer_list(self):
        peer_count = int.from_bytes(self.message[0:2], byteorder='big')
        for i in range(peer_count):
            ip1, ip2, ip3, ip4, port = struct.unpack(">BBBBH", self.message[2+6*i: 2+6*(i+1)])
            peer = "{}.{}.{}.{}:{}".format(ip1, ip2, ip3, ip4, int(port))
            if peer not in self.peer_list:
                self.peer_list.append(peer)
        logging.info("New peer list: {}".format(self.peer_list))
        return self.peer_list


# TODO change documentation for push to send just your own id
# TODO think about having pull request and push message as a single one
# TODO merge with impl of Pull message?
class GossipPushMessage:
    """
        Class to handle received messages which are Pushed and
        also to send Push messages with our ip
    """
    def __init__(self, peer_list=None, self_ip=None, self_port=None, message_body=None):
        if self_ip:
            self.msg_type = GOSSIP_P2P_PUSH
            self.ip = self_ip
            self.port = self_port
            self.size = 10
        if message_body:
            self.peer_list = peer_list
            self.message = message_body

    def prepare_message(self):
        ip1, ip2, ip3, ip4 = (int(x) for x in self.ip.split("."))
        message = struct.pack(">HHBBBBH", self.size, self.msg_type, ip1, ip2, ip3, ip4, self.port)

        return message

    def add_received_peer(self):
        ip1, ip2, ip3, ip4, port = struct.unpack(">BBBBH", self.message)
        received_peer = "{}.{}.{}.{}:{}".format(ip1, ip2, ip3, ip4, port)
        self.peer_list.append(received_peer)

        return received_peer


class GossipSendContentMessage:
    """
        Class to handle received arbitrary content and
        to send arbitrary content
    """
    def __init__(self, msg_to_send=None, message_body=None):
        if msg_to_send:
            self.msg_type = GOSSIP_P2P_SEND_CONTENT
            self.size = 4 + len(msg_to_send)
            self.msg_body = msg_to_send

        if message_body:
            self.message = message_body
            self.size, self.msg_type = struct.unpack(">HH", message_body[0:2])
            self.msg_body = message_body[2:]

    def get_content_type(self):
        return self.msg_type

    def get_content_body(self):
        return self.msg_body

    def prepare_message(self):
        message = struct.pack(">HH", self.size, self.msg_type)
        message += self.msg_body

        return message
