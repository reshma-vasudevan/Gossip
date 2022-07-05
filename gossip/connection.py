# Handle the socket connections

import socket

from interface import identify_msg_type, notification


def get_connection(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    return s


if __name__ == '__main__':
    host = "127.0.0.1"
    port = 7001

    s = get_connection(host, port)
    s.listen(1)
    conn, addr = s.accept()
    print("Connected")

    # Testing receiving gossip announce
    message = conn.recv(4096)
    identify_msg_type(message)

    # Testing receiving gossip notify
    message = conn.recv(4096)
    identify_msg_type(message)

    # Testing sending gossip notification
    message = notification()
    conn.sendall(message)

    # Testing receiving gossip validation
    message = conn.recv(4096)
    identify_msg_type(message)

    conn.close()
    s.close()
