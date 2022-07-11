class ClientDisconnected(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class InvalidHeader(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class InvalidSize(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class InvalidMessageType(Exception):
    def __init__(self, msg):
        super().__init__(msg)
