# Test class to test functionality of class MessageStorage
import unittest
from gossip.message_storage import MessageStorage


class TestMessageStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.message_storage = MessageStorage()

    def test_add_new_data(self):
        msg_id1 = self.message_storage.add_data("conn", "Connection 1 found", 2)
        msg_id2 = self.message_storage.add_data("conn", "Connected", 3)
        expected1 = {"message":"Connection 1 found", "ttl": 2, "valid":0}
        expected2 = {"message":"Connected", "ttl": 3, "valid":0}
        self.assertEqual(self.message_storage.messages[msg_id1], expected1)
        self.assertEqual(self.message_storage.messages[msg_id2], expected2)

    def test_add_new_subscriber(self):
        self.message_storage.add_subscriber("conn", "res1")
        self.message_storage.add_subscriber("conn", "res5")
        subs = self.message_storage.get_subscribers("conn")
        self.assertIsNotNone(subs)
        self.assertEqual(subs[0], "res1")
        self.assertEqual(subs[1], "res5")
