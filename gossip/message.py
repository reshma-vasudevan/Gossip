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
        self.ttl, self.res, self.data_type = struct.unpack(">BBH", message_body[:4])
        self.data = message_body[4:]

    def get_all_data(self):
        return {'ttl': self.ttl, 'res': self.res, 'data_type': self.data_type, 'data': self.data}

    def get_storage_data(self):
        return {'data_type': self.data_type, 'data': self.data, 'ttl': self.ttl}

    def reduce_ttl(self):
        modified_ttl = self.ttl
        # If ttl is 0, hops are unlimited
        if self.ttl != 0:
            modified_ttl -= 1
        updated_message = struct.pack(">BBH", modified_ttl, self.res, self.data_type)
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
        """
        :param self_ip: to send a pull message, use this parameter for own ip
        :param self_port: to send a pull message, use this parameter for own port
        :param message_body: if pull message was received, use this parameter
        """
        if self_ip:
            self.msg_type = GOSSIP_P2P_PULL
            self.ip = self_ip
            self.port = self_port
            self.size = 10
        if message_body:
            self.message = message_body

    def prepare_message(self):
        """
        Pack a pull message with our own address
        :return: packed message
        """
        ip1, ip2, ip3, ip4 = (int(x) for x in self.ip.split("."))
        message = struct.pack(">HHBBBBH", self.size, self.msg_type, ip1, ip2, ip3, ip4, self.port)
        return message

    def get_requester_address(self):
        """
        Extract peer who sent pull message
        :return: extracted peer address
        """
        ip1, ip2, ip3, ip4, port = struct.unpack(">BBBBH", self.message)
        requester_addr = "{}.{}.{}.{}:{}".format(ip1, ip2, ip3, ip4, port)

        return requester_addr


class GossipPullResponseMessage:
    """
        Class to handle Response messages received to our pull request or
        to send a Response to a Pull message received
    """
    def __init__(self, peer_list, message_body=None):
        """
        :param peer_list: known list of peers
        :param message_body: if pull response message was received, use this parameter
        """
        self.peer_list = peer_list
        self.msg_type = GOSSIP_P2P_PULL_RESPONSE
        if message_body:
            self.message = message_body

    def prepare_message(self):
        """
        Pack a pull response message with our known list of peers
        :return: packed message
        """
        peer_count = len(self.peer_list)
        size = 4 + 2 + 6*peer_count
        message = struct.pack(">HHH", size, self.msg_type, peer_count)
        for peer in self.peer_list:
            ip, port = peer.split(":")
            ip1, ip2, ip3, ip4 = (int(x) for x in ip.split("."))
            message += struct.pack(">BBBBH", ip1, ip2, ip3, ip4, int(port))
        return message

    def update_peer_list(self):
        """
        Extract peers from received pull response message and add them to our known peer list
        :return: peers received from this response message
        """
        peer_count = int.from_bytes(self.message[0:2], byteorder='big')
        obtained_peers = []
        for i in range(peer_count):
            ip1, ip2, ip3, ip4, port = struct.unpack(">BBBBH", self.message[2+6*i: 2+6*(i+1)])
            peer = "{}.{}.{}.{}:{}".format(ip1, ip2, ip3, ip4, int(port))
            obtained_peers.append(peer)
            if peer not in self.peer_list:
                self.peer_list.append(peer)
        logging.info("New peer list: {}".format(self.peer_list))
        return obtained_peers


class GossipPushMessage:
    """
        Class to handle received messages which are Pushed and
        also to send Push messages with our ip
    """
    def __init__(self, peer_list=None, self_ip=None, self_port=None, message_body=None):
        """
        :param peer_list: if pull message was received, use this parameter to add received peer address
        :param self_ip: to send a pull message, use this parameter for own ip
        :param self_port: to send a pull message, use this parameter for own port
        :param message_body: if pull message was received, use this parameter
        """
        if self_ip:
            self.msg_type = GOSSIP_P2P_PUSH
            self.ip = self_ip
            self.port = self_port
            self.size = 10
        if message_body:
            self.peer_list = peer_list
            self.message = message_body

    def prepare_message(self):
        """
        Pack a push message with our own address
        :return: packed message
        """
        ip1, ip2, ip3, ip4 = (int(x) for x in self.ip.split("."))
        message = struct.pack(">HHBBBBH", self.size, self.msg_type, ip1, ip2, ip3, ip4, self.port)
        return message

    def add_received_peer(self):
        """
        Add pushed peer from received message to known list of peers
        :return: peer received in this message
        """
        ip1, ip2, ip3, ip4, port = struct.unpack(">BBBBH", self.message)
        received_peer = "{}.{}.{}.{}:{}".format(ip1, ip2, ip3, ip4, port)
        self.peer_list.append(received_peer)
        logging.info("New peer list: {}".format(self.peer_list))
        return received_peer


class GossipSendContentMessage:
    """
        Class to handle received arbitrary content and
        to send arbitrary content
    """
    def __init__(self, msg_to_send=None, message_body=None):
        """
        :param msg_to_send: to send message, use this parameter
        :param message_body: if message was received, use this parameter
        """
        if msg_to_send:
            # pack outer type as p2p message type GOSSIP_P2P_SEND_CONTENT
            self.outer_msg_type = GOSSIP_P2P_SEND_CONTENT
            self.outer_size = 4 + len(msg_to_send)
            self.msg_body = msg_to_send

        if message_body:
            self.inner_size, self.inner_msg_type = struct.unpack(">HH", message_body[0:4])
            self.msg_body = message_body[4:]

    def get_outer_content_type(self):
        return self.outer_msg_type

    def get_inner_content_type(self):
        return self.inner_msg_type

    def get_content_body(self):
        return self.msg_body

    def prepare_message(self, inner_msg_type=None):
        """
        Pack an arbitrary message
        :param inner_msg_type: message type of data to be sent in this packet
        :return: packed message
        """
        if inner_msg_type:
            inner_msg_size = 4 + len(self.msg_body)
            self.outer_size += 4
            message = struct.pack(">HHHH", self.outer_size, self.outer_msg_type, inner_msg_size, inner_msg_type)
        else:
            message = struct.pack(">HH", self.outer_size, self.outer_msg_type)
        message += self.msg_body
        logging.info("Created arbitrary msg: {}".format(message))
        return message
