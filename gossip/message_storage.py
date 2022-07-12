# Deals with cache to store announced Data and subscribers to data types
from collections import defaultdict


class MessageStorage():
    def __init__(self):
        self.data_types = defaultdict(list)
        self.messages = {}
        self.subscribers = defaultdict(list)

    def add_data(self, data_type, data, ttl):
        msg_id = self.get_random_msg_id()
        self.data_types[data_type].append(msg_id)
        self.messages[msg_id] = {"message":data, "ttl": ttl, "valid":0}

    def add_subscriber(self, data_type, subscriber):
        self.subscribers[data_type].append(subscriber)

    def get_message_ids(self, data_type):
        return self.data_types[data_type]

    def get_subscribers(self, data_type):
        return self.subscribers[data_type]

    def get_random_msg_id(self):
        pass

    def update_validity(self):
        pass


if __name__ == '__main__':
    # Testing the class
    message_storage = MessageStorage()
    message_storage.add_data("conn", "New connection found", 2)
    message_storage.add_data("conn", "Old connection", 3)
    msg_ids = message_storage.get_message_ids("conn")
    for id in msg_ids:
        msg = message_storage.messages[id]
        print("Message:", msg["message"], "ttl:", msg["ttl"], "validity:", msg["valid"])
    message_storage.add_subscriber("conn", "res1")
    message_storage.add_subscriber("conn", "res5")
    subs = message_storage.get_subscribers("conn")
    if subs is not None:
        print("Type is valid and subscribers are : ")
        for sub in subs:
            print(sub)
