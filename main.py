import sys, logging, os
from gossip.config_parser import parse_config
from gossip.server import APIServerThread, P2PServerThread
import queue
from gossip.api_message_handler import AnnounceMessageHandler
from gossip.message_storage import MessageStorage
from gossip.p2p_message_handler import P2PMessageHandler
from gossip.p2p_client_handler import P2PClientHandler

logging.basicConfig(level=logging.DEBUG)


def main():
    logging.debug('Starting Gossip')

    config_path='config/config.ini'

    # check if -c or --config were provided and if so set config_path
    setnext=False
    for i in sys.argv:
        if not setnext and (i=='-c' or i=='--config'):
            setnext=True
            continue;
        if setnext:
            config_path=i
            break;

    # check if configuration file exists
    if not os.path.isfile(config_path):
        logging.error('Configuration file {} doesn\'t exist'.format(config_path))
        logging.error('Exiting Gossip')
        return

    # parse configuration file
    config=parse_config(config_path)

    # initializing objects
    message_storage = MessageStorage()
    announce_queue = queue.Queue()
    p2p_queue = queue.Queue()
    incoming_queue = queue.Queue()

    api_connections = {}
    p2p_connections = {}

    peer_list = []

    announce_message_handler = AnnounceMessageHandler(announce_queue, message_storage, api_connections, p2p_queue)
    announce_message_handler.start()

    logging.debug('Starting API server thread')

    apiserverthread = APIServerThread(
                                   config['api_address']['address'],
                                   config['api_address']['port'],
                                   announce_queue,
                                   message_storage,
                                   api_connections)

    apiserverthread.start()

    p2p_message_handler = P2PMessageHandler(p2p_queue, p2p_connections, peer_list, incoming_queue, config['degree'])
    p2p_message_handler.start()

    logging.debug('Starting P2P server thread')
    p2pserverthread = P2PServerThread(
                                   config['p2p_address']['address'],
                                   config['p2p_address']['port'],
                                   p2p_connections,
                                   incoming_queue,
                                   p2p_queue)
    p2pserverthread.start()

    p2p_client_handler = P2PClientHandler(incoming_queue, peer_list, announce_queue, p2p_queue, config['p2p_address']['address'],
                                          config['p2p_address']['port'], p2p_connections,
                                          config['bootstrapper']['address'],config['bootstrapper']['port'])
    p2p_client_handler.start()

    # join the threads

    logging.debug('Joining API server thread')
    apiserverthread.join()
    announce_queue.join()

    logging.debug('Joining P2P server thread')
    p2pserverthread.join()
    p2p_queue.join()
    incoming_queue.join()

    logging.debug('Exiting Gossip')


if __name__ == '__main__':
    main()
