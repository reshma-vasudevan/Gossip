from threading import Thread, Lock
import gossip.codes as c
from gossip.message import *
import logging


class P2PClientHandler(Thread):
    def __init__(self, incoming_queue, peer_list, announce_queue, p2p_queue):
        Thread.__init__(self)
        self.incoming_queue = incoming_queue
        self.peer_list = peer_list
        self.announce_queue = announce_queue
        self.p2p_queue = p2p_queue
        self.lock = Lock()

    def run(self) -> None:
        while True:
            msg = self.incoming_queue.get()
            msg_type = msg["msg_type"]
            msg_body = msg["msg_body"]

            with self.lock:
                if msg_type == c.GOSSIP_P2P_PUSH:
                    push_message = GossipPushMessage(message_body=msg_body)
                    push_message.add_received_peer()
                elif msg_type == c.GOSSIP_P2P_PULL_RESPONSE:
                    logging.info("Received message type GOSSIP_P2P_PULL_RESPONSE")
                    pull_response_message = GossipPullResponseMessage(peer_list=self.peer_list, message_body=msg_body)
                    pull_response_message.update_peer_list()
                elif msg_type == c.GOSSIP_P2P_PULL:
                    pull_message = GossipPullMessage(message_body=msg_body)
                    requester_addr = pull_message.get_requester_address()

                    # TODO send response back
                    # TODO either close existing conn and create new one to real address or
                    # TODO save real id along with conn details
                    # update_p2p_address()
                    created_pull_response_message = GossipPullResponseMessage(self.peer_list)
                    self.p2p_queue.put({'action': c.P2P_ACTION_PULL_RESPONSE, 'to_address': requester_addr,
                                        'message': created_pull_response_message})

                elif msg_type == c.GOSSIP_P2P_SEND_CONTENT:
                    received_content = GossipSendContentMessage(message_body=msg_body)
                    # If we received an announce message, put in api announce queue
                    # and send it to other peers after reducing ttl by 1
                    if received_content.get_content_type() == c.GOSSIP_ANNOUNCE:
                        announce_message = AnnounceMessage(received_content.get_content_body())
                        self.announce_queue.put(announce_message)

                        updated_announce_message = announce_message.reduce_ttl()
                        self.p2p_queue.put({'action': c.P2P_ACTION_SEND_ALL, 'message': updated_announce_message})

            self.incoming_queue.task_done()