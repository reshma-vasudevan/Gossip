import gossip.exceptions as e
import gossip.codes as c


def parse_header(conn):
    """Reads from conn and parses header

    :param conn: connection to receive from
    :return: msg header, size and data in a dictonary
    """
    header = conn.recv(4)
    if len(header) == 0:
        raise e.ClientDisconnected("")
    if len(header) < 4:
        raise e.InvalidHeader("Length of header: {}".format(len(header)))

    size = int.from_bytes(header[:2], byteorder='big')
    msgtype = int.from_bytes(header[2:4], byteorder='big')
    if size < 4:
        raise e.InvalidSize("Size: {}".format(size))
    if msgtype > c.MAX or msgtype < c.MIN:
        raise e.InvalidMessageType("Message Type: {}".format(msgtype))

    msg=conn.recv(size - 4)
    return {"type": msgtype, "size": size, "data": msg}