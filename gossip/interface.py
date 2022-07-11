# List of APIs that can be used to communicate with the module

import struct
import codes as c

def announce(message: bytes):
    size, msg_type, ttl, res, data_type = struct.unpack(">HHBBH", message[:8])
    data = message[8:size]

    print("Received announce message", size, ttl, data_type, data)

    # TODO save data in cache


def notify(message: bytes):
    size, msg_type, res, data_type = struct.unpack(">HHHH", message)

    print("Received notify message", size, data_type)

    # TODO save interested receivers
    #  make a list of valid data types


# Sent by us
def notification() -> bytes:
    msg_id = 7
    data_type = 1337
    data = b"mydata"
    size = 8 + len(data)

    message = struct.pack(">HHHH", size, c.GOSSIP_NOTIFICATION, msg_id, data_type)
    message += data

    # TODO need to check for well-formedness of data
    #  generation of message ID
    print("Sending message", data_type, data)
    return message


def validation(message: bytes):
    size, msg_type, msg_id, res, valid = struct.unpack(">HHHB?", message)

    print("Received validation message", size, msg_id, valid)

    if valid:
        print("Valid message")
        pass
    else:
        print("Invalid message")
        pass

    # TODO store validity of notification message


def identify_msg_type(message: bytes):
    message_type = int.from_bytes(message[2:4], byteorder='big')
    if message_type == c.GOSSIP_ANNOUNCE:
        announce(message)
    elif message_type == c.GOSSIP_NOTIFY:
        notify(message)
    elif message_type == c.GOSSIP_VALIDATION:
        validation(message)
