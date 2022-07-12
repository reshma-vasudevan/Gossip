# Classes for the different message types : to be completed
import struct


class Message():
    def __init__(self, message):
        self.size, self.msg_type = struct.unpack(">HH", message[:4])
        self.body = message[4:self.size]

    def get_size(self):
        return self.size

    def get_message_type(self):
        return self.msg_type


class AnnounceMessage(Message):
    def __init__(self, message):
        super.__init__(message)
        self.ttl, self.res, self.data_type = struct.unpack(">BBH", self.body[:4])

    def store_message(self):
        pass


class NotifyMessage(Message):
    def __init__(self, message):
        super.__init__(message)
        self.res, self.data_type = struct.unpack(">HH", self.body)

    def store_subscribers(self):
        pass

    def make_message_valid(self):
        pass


class NotificationMessage(Message):
    def __init__(self, message):
        super.__init__(message)

    def send_message(self):
        pass


class ValidationMessage(Message):
    def __init__(self, message):
        super.__init__(message)
        self.msg_id, self.res, self.valid = struct.unpack(">HB?", self.body)

    def store_subscribers(self):
        pass

    def is_message_well_formed(self):
        pass
