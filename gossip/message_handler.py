import codes as c
from message import *


class MessageHandler:
    def __init__(self, message_storage):
        self.message_storage = message_storage

    def msg_type_actions(self, message_type, message_body: bytes):
        """
        Method to assign message class instance based on the message type of received message
        and perform according actions.

        :param message_type: type of received message
        :param message_body: body of message
        :return: Instance of respective message class
        """
        if message_type == c.GOSSIP_ANNOUNCE:
            # if announce message, add the data to cache and start creating notification messages
            message = AnnounceMessage(message_body)
            msg_id = self.message_storage.add_data(message.get_storage_data())
            # TODO call thread to send gossip messages
            # create_notification(message.data_type, msg_id, message_storage)
        elif message_type == c.GOSSIP_NOTIFY:
            # if notify, just add ip address of sender and keep
            message = NotifyMessage(message_body)
            sub = ""
            self.message_storage.add_subscriber(message.data_type, sub)
        elif message_type == c.GOSSIP_VALIDATION:
            # if validation, if false, update validity of message
            message = ValidationMessage(message_body)
            if not message.valid:
                self.message_storage.make_invalid(message.msg_id)

        return message

    def create_notification(self, data_type, msg_id):
        """
        Method to create and send notification messages

        :param data_type: data type of data to be sent
        :param msg_id: message id of stored message
        """
        m = self.message_storage.messages[msg_id]
        notif = NotificationMessage(msg_id, data_type, m["message"])
        notif_message = notif.prepare_message()
        # Now start thread to send it for as many ttls:
        subs = self.message_storage.get_subscribers()
        for sub in subs:
            pass
            # sender(notif_message)
