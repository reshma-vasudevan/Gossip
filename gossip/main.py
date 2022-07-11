import sys, logging, os
from config_parser import parse_config
from server import ServerThread

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

    # TODO do some other stuff
    apiserverthread = ServerThread("API", "localhost", 8888)
    apiserverthread.start()

    apiserverthread.join()

    logging.debug('Exiting Gossip')

if __name__ == '__main__':
    main()
