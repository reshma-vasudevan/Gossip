from threading import Thread, Lock
import gossip.codes as c
from gossip.message import *
import logging
import random


class P2PHandler(Thread):
    """
        Thread to process incoming messages from P2P connections and take corresponding actions based on message type
    """
    def __init__(self, incoming_queue, peer_list, announce_queue, p2p_queue, p2p_connections, self_address, self_port,
                 bootstrapper_address, bootstrapper_port, degree):
        """

        :param incoming_queue: contains messages from connections along with sender info
        :param peer_list: peers known by own P2P server
        :param announce_queue: queue to put announce messages to be later processed by API handler
        :param p2p_queue: queue to put messages to be sent to other peers
        :param p2p_connections: dict of active p2p connections with address as key
        :param self_address: address of own p2p server
        :param self_port: port of own p2p server
        :param bootstrapper_address: address of bootstrapper server
        :param bootstrapper_port: port of bootstrapper server
        :param degree: maximum connections that can be handled by this peer
        """
        Thread.__init__(self)
        self.incoming_queue = incoming_queue
        self.peer_list = peer_list
        self.announce_queue = announce_queue
        self.p2p_queue = p2p_queue
        self.address = self_address
        self.port = self_port
        self.connections = p2p_connections
        self.bootstrapper_address = bootstrapper_address
        self.bootstrapper_port = bootstrapper_port
        self.degree = degree
        self.lock = Lock()

    def run(self) -> None:
        # connect to a bootstrapper and get first list of peers
        bootstrapper = "{}:{}".format(self.bootstrapper_address, self.bootstrapper_port)
        created_pull_message = GossipPullMessage(self_ip=self.address, self_port=self.port).prepare_message()
        self.p2p_queue.put(
            {'action': P2P_ACTION_SEND, 'to_address': bootstrapper, 'message': created_pull_message})

        while True:
            msg = self.incoming_queue.get()
            sender = msg['sender']
            msg_type = msg['msg_type']
            msg_body = msg['msg_body']

            with self.lock:
                if msg_type == c.GOSSIP_P2P_PUSH:
                    logging.info("Received message type GOSSIP_P2P_PUSH")
                    push_message = GossipPushMessage(peer_list=self.peer_list, message_body=msg_body)
                    # Adding pushed peer address to our list of known peers
                    received_peer = push_message.add_received_peer()
                    # Correcting p2p server address of this connection by adding separate attribute
                    self.connections[sender]['p2p_server_address'] = received_peer

                elif msg_type == c.GOSSIP_P2P_PULL_RESPONSE:
                    logging.info("Received message type GOSSIP_P2P_PULL_RESPONSE")
                    pull_response_message = GossipPullResponseMessage(peer_list=self.peer_list, message_body=msg_body)
                    # Adding received list of peers to our known peer list
                    obtained_peers = pull_response_message.update_peer_list()
                    # Creating connections to the received peer list
                    self.add_new_connections(obtained_peers)

                elif msg_type == c.GOSSIP_P2P_PULL:
                    logging.info("Received message type GOSSIP_P2P_PULL")
                    pull_message = GossipPullMessage(message_body=msg_body)
                    requester_server_addr = pull_message.get_requester_address()

                    # Updating p2p server address of this connection
                    self.connections[sender]['p2p_server_address'] = requester_server_addr
                    # Send pull response with our known peers to this connection
                    created_pull_response_message = GossipPullResponseMessage(self.peer_list).prepare_message()
                    self.p2p_queue.put({'action': c.P2P_ACTION_SEND, 'to_address': sender,
                                        'message': created_pull_response_message})

                elif msg_type == c.GOSSIP_P2P_SEND_CONTENT:
                    logging.info("Received Announce message")
                    send_content_message = GossipSendContentMessage(message_body=msg_body)

                    # If we received an announce message, put in api announce queue
                    # and send it to other peers after reducing ttl by 1
                    if send_content_message.get_inner_content_type() == c.GOSSIP_ANNOUNCE:
                        announce_message_body = send_content_message.get_content_body()
                        announce_message = AnnounceMessage(announce_message_body)
                        self.announce_queue.put({'message':announce_message_body, 'resend': False})

                        updated_announce_message = announce_message.reduce_ttl()
                        self.p2p_queue.put({'action': c.P2P_ACTION_SEND_ALL, 'message': updated_announce_message})

                elif msg_type == c.P2P_CONNECTION_CLOSED:
                    # Adding new connection from known peer list
                    logging.info("Received connection closed message {}".format(sender))
                    self.add_new_connections(self.peer_list)

            self.incoming_queue.task_done()

    def add_new_connections(self, available_peers):
        # collect server ips of open connections
        server_addresses = []
        for key in self.connections.keys():
            server_addresses.append(self.connections[key]['p2p_server_address'])
        for address in available_peers:
            if len(self.connections.keys()) >= self.degree:
                break
            else:
                # only checking peer address is not enough as connection might be in diff name
                if address not in self.connections.keys() and address not in server_addresses:
                    # create new connection and add to list of connections
                    # with equal probability send either a PUSH or PULL message to the new peer
                    r = random.randint(0,1)
                    actions = ['PUSH', 'PULL']
                    message = None
                    if actions[r] == 'PUSH':
                        message = GossipPushMessage(self_ip=self.address, self_port=self.port).prepare_message()
                    elif actions[r] == 'PULL':
                        message = GossipPullMessage(self_ip=self.address, self_port=self.port).prepare_message()
                    self.p2p_queue.put({'action': P2P_ACTION_SEND, 'to_address': address, 'message': message})