import struct
import unittest
from gossip.message import AnnounceMessage, NotifyMessage, NotificationMessage, ValidationMessage
from gossip.codes import GOSSIP_NOTIFICATION


class TestMessage(unittest.TestCase):
    def setUp(self) -> None:
        self.ttl = 3
        self.data_type = 1001
        self.data = b"Test Message"
        self.msg_id = 14

    def test_announce_values(self):
        message_body = struct.pack(">BBH", self.ttl, 0, self.data_type)
        message_body += self.data

        expected = {'ttl': 3, 'res': 0, 'data_type': 1001, 'data': b"Test Message"}
        announce_message = AnnounceMessage(message_body)
        self.assertEqual(announce_message.get_all_data(), expected)

    def test_notify_values(self):
        message_body = struct.pack(">HH", 0, self.data_type)

        expected = {'res': 0, 'data_type': 1001}
        notify_message = NotifyMessage(message_body)
        self.assertEqual(notify_message.get_all_data(), expected)

    def test_notification_message(self):
        size = 8 + len(self.data)
        message = struct.pack(">HHHH", size, GOSSIP_NOTIFICATION, self.msg_id, self.data_type)
        message += self.data

        notif_message = NotificationMessage(self.msg_id, self.data_type, self.data)
        self.assertEqual(notif_message.prepare_message(), message)

    def test_validation_values(self):
        message_body = struct.pack(">HB?", self.msg_id, 0, True)

        expected = {'msg_id': 14, 'res': 0, 'valid': True}
        validation_message = ValidationMessage(message_body)
        self.assertEqual(validation_message.get_all_data(), expected)
