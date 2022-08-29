import sys, logging, os
from gossip.config_parser import parse_config
from gossip.server import ServerThread
import queue
from gossip.message_handler import AnnounceMessageHandler
from gossip.message_storage import MessageStorage

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
    q = queue.Queue()
    connections = {}

    announce_message_handler = AnnounceMessageHandler(q, message_storage, connections)
    announce_message_handler.start()

    logging.debug('Starting API server thread')
    apiserverthread = ServerThread("API",
                                   config['api_adress']['address'],
                                   config['api_adress']['port'],
                                   q,
                                   message_storage,
                                   connections)
    apiserverthread.start()

    logging.debug('Starting P2P server thread')
    p2pserverthread = ServerThread("P2P",
                                   config['api_adress']['address'],
                                   config['api_adress']['port'])
    p2pserverthread.start()


    # join the threads

    logging.debug('Joining API server thread')
    apiserverthread.join()
    q.join()

    logging.debug('Joining P2P server thread')
    p2pserverthread.join()

    logging.debug('Exiting Gossip')

if __name__ == '__main__':
    main()
