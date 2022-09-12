#!/usr/bin/python3

import argparse
import socket
import sys

from gossip.codes import *
from gossip.message import *
from gossip.utils import parse_header


# Extracting announce message for logging
def extract_announce(message):
    size = int.from_bytes(message[0:2], byteorder='big')
    msgtype = int.from_bytes(message[2:4], byteorder='big')
    msg = message[4:]
    return {"type": msgtype, "size": size, "data": msg}


def send_arbitrary_message(dest_addr, dest_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((dest_addr, dest_port))
    print("Connected to peer : ", dest_addr, dest_port)

    type = 3352
    data = b"hello"
    ttl = 4
    size = 4 + 4 + len(data)
    osize = size + 4

    msg = struct.pack(">HHHHBBH", osize, GOSSIP_P2P_SEND_CONTENT, size, GOSSIP_ANNOUNCE, ttl, 0, type)
    msg += data

    s.send(msg)
    print("Sent announce message: type: {}, data: {}, ttl: {}".format(type, data, ttl))
    s.close()


def run_server(address, port, peer_list):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((address, port))
    print("Server running at {}:{}".format(address, port))

    try:
        while True:
            s.listen(5)
            (conn, (ip, port)) = s.accept()
            print("Received request from {}:{}".format(ip, port))

            t = parse_header(conn)
            print("Received message :",t)
            if t['type'] == GOSSIP_P2P_PUSH:
                print("Received a PUSH message")
                m = GossipPushMessage(peer_list=peer_list, message_body=t['data'])
                p = m.add_received_peer()
                print('Pushed peer address:', p)

            if t['type'] == GOSSIP_P2P_PULL:
                print("Received a PULL message")
                response_message = GossipPullResponseMessage(peer_list).prepare_message()
                conn.sendall(response_message)
                print("Sent pull response message")

            # Block to receive an announce message after push or pull
            t = parse_header(conn)
            print("Received message : ",t)
            a = extract_announce(t['data'])
            if a['type'] == GOSSIP_ANNOUNCE:
                am = AnnounceMessage(a['data'])
                print("Announced data: ", am.get_all_data())

    except KeyboardInterrupt:
        print("Exiting")
    except Exception as error:
        print("Client crashed: {}".format(error))

    s.close()


def push_to_peer(address, port, dest_addr, dest_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((dest_addr, dest_port))
    print("Connected to peer : ", dest_addr, dest_port)

    print("Sending PUSH message")
    m = GossipPushMessage(self_ip=address, self_port=port).prepare_message()
    s.sendall(m)
    s.close()


def pull_from_peer(address, port, dest_addr, dest_port, peer_list):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((dest_addr, dest_port))
    print("Connected to peer : ", dest_addr, dest_port)

    print("Sending PULL message")
    m = GossipPullMessage(self_ip=address, self_port=port).prepare_message()
    s.sendall(m)
    print("Waiting for Pull response message")
    while True:
        try:
            t = parse_header(s)
            print("Received message : ",t)
            m = GossipPullResponseMessage(peer_list=peer_list, message_body=t['data'])
            p = m.update_peer_list()
            print("Received peer list", p)
        except KeyboardInterrupt:
            break
    s.close()


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description='Runs a sample simulation of gossip protocol using push and pull')
    parser.add_argument('-a', dest='address', type=str, help='Server address to bind to', required=True)
    parser.add_argument('-p', dest='port', type=str, help='Server port to bind to', required=True)
    parser.add_argument('-pr', dest='peers', type=str, nargs='*', help='Sample list of peers the server knows')
    parser.add_argument('-ac', dest='action', type=str, help='The action you want to see this server perform',
                        choices=['push', 'pull', 'send_announce'])
    parser.add_argument('-da', dest='dest_address', type=str, help='Address of specific peer to perform an action on')
    parser.add_argument('-dp', dest='dest_port', type=str, help='Port of specific peer to perform an action on')

    args = parser.parse_args()

    address = args.address
    port = int(args.port)
    if not args.peers:
        peer_list = []
    else:
        peer_list = args.peers
    action = args.action
    dest_address = args.dest_address
    if args.dest_port:
        dest_port = int(args.dest_port)

    if action == 'push':
        push_to_peer(address, port, dest_address, dest_port)
        sys.exit()

    if action == 'pull':
        pull_from_peer(address, port, dest_address, dest_port, peer_list)
        sys.exit()

    if action == 'send_announce':
        send_arbitrary_message(dest_address, dest_port)
        sys.exit()

    run_server(address, port, peer_list)


if __name__ == "__main__":
    main()