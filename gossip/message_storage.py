# Deals with cache to store announced Data and subscribers to data types
from collections import defaultdict
from random import randint


class MessageStorage:
    """
    Class to maintain a storage for messages.

    Attributes:
        data_types: Stores message ids of all announced messages of given data_type
            dict of list, index: data_type, value: list of message ids
        messages: Stores message info of given message_id
            dict of dict, index: message id, value of format: {"message":message, "ttl": ttl, "valid":0}
        subscribers: Stores list of subscribers to given data_type
            dict of list, index: data_type, value: list of subscribers
    """
    def __init__(self):
        self.data_types = defaultdict(list)
        self.messages = {}
        self.subscribers = defaultdict(list)

    def add_data(self, data_type, data, ttl):
        msg_id = self.get_random_msg_id()
        self.data_types[data_type].append(msg_id)
        self.messages[msg_id] = {"message":data, "ttl": ttl, "valid":0}
        return msg_id

    def add_subscriber(self, data_type, subscriber):
        self.subscribers[data_type].append(subscriber)

    def get_message_ids(self, data_type):
        return self.data_types[data_type]

    def get_subscribers(self, data_type):
        return self.subscribers[data_type]

    def get_random_msg_id(self):
        r = randint(0, 100)
        while r in self.messages.keys():
            r = randint(0, 100)
        return r

    def make_invalid(self, msg_id):
        self.messages[msg_id]["valid"] = False
