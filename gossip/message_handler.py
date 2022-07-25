# Handles received announce messages and creates threads to send corresp. notification messages
from message import *
from threading import Thread, Lock
import socket


class AnnounceMessageHandler(Thread):
    """
    Thread to wait on queue of announce messages and take req. action
    """
    def __init__(self, queue, message_storage, connections):
        """
        Constructor

        :param queue: Queue from which announced messages are retrieved
        :param message_storage: cache to store messages and subscribers
        :param connections: connections that already exist
        """
        Thread.__init__(self)
        self.queue = queue
        self.message_storage = message_storage
        self.lock = Lock()
        self.connections = connections

    def run(self):
        while True:
            # Processing one announce message from the queue
            m = self.queue.get()

            with self.lock:
                msg_id = self.message_storage.add_data(m.data_type, m.data, m.ttl)
                msg = self.message_storage.messages[msg_id]

                # Find all subscribers of that announce message and create threads
                for sub in self.message_storage.get_subscribers(m.data_type):
                    n = NotifThread(sub, m.data_type, msg_id, msg, self.connections)
                    n.start()
            self.queue.task_done()


class NotifThread(Thread):
    """
    Thread to create and send Notification message
    """
    def __init__(self, addr, data_type, msg_id, msg, connections):
        """
        Constructor

        :param addr: addr of subscriber in format <host>:<port>
        :param data_type: data type of message to be created
        :param msg_id: id of created message
        :param msg: message to be sent
        :param connections: connections that already exist
        """
        Thread.__init__(self)
        self.addr = addr
        self.data_type = data_type
        self.msg_id = msg_id
        self.msg = msg
        self.connections = connections

    def run(self):
        new_conn = False
        if self.addr in self.connections.keys():
            conn = self.connections[self.addr]
        else:
            new_conn = True
            host, port = self.addr.split(":")
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect((host, port))

        notif = NotificationMessage(self.msg_id, self.data_type, self.msg["message"])
        m = notif.prepare_message()

        conn.sendall(m)

        if new_conn:
            conn.close()
