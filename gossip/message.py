# Classes for the different message types
import struct
from gossip.codes import GOSSIP_NOTIFICATION


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

