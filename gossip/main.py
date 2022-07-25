import sys, logging, os
from config_parser import parse_config
from server import ServerThread
import queue
from message_handler import AnnounceMessageHandler
from message_storage import MessageStorage

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

    apiserverthread = ServerThread("API", "localhost", 8888, q, message_storage, connections)
    apiserverthread.start()

    apiserverthread.join()
    q.join()

    logging.debug('Exiting Gossip')

if __name__ == '__main__':
    main()
