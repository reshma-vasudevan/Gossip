import ini, logging

def parse_address(host):
    """ Split host into address and port.
    
    :param host: the hostname as a string in either ipv4:port or [ipv6]:port or domain:port form
    :return: Dict in form {'address': <ipv4/ipv6/domain>, 'port': <port as int>}
    """
    if ':' not in host:
        return {}
    s = host.replace('[','').replace(']','').split(':')
    port = int(s[-1])
    address = ':'.join(s[:-1])
    return {'address': address, 'port': port}

def parse_config(path_to_config_file):
    """ Parse configuration file and return a dictonary with all relevant data
    
    :param path_to_config_file: string which stores the path to the configuration file which is to be parsed. Caller makes sure that the file actually exists
    :return: Dict which contains all relevant entries for gossip, so the entire [gossip] section and everything which doesn't belong to any section. 'bootstrapper', 'p2p_address' and 'api_address' get parsed using parse_address
    """
    logging.info('Reading in configuration file {}'.format(path_to_config_file))
    with open(path_to_config_file, 'r') as f:
        config = ini.parse(f.read())
        config['gossip']['bootstrapper'] = parse_address(config['gossip']['bootstrapper'])
        config['gossip']['p2p_address'] = parse_address(config['gossip']['p2p_address'])
        config['gossip']['api_address'] = parse_address(config['gossip']['api_address'])

    retconf={'hostkey': config['hostkey']}
    for i in config['gossip'].keys():
        retconf[i] = config['gossip'][i]
    logging.info('Configuration is: {}'.format(retconf))
    return retconf

if __name__ == '__main__':
    print(parse_config('config/config.ini'))
