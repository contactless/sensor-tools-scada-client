class TRequestException(Exception):
    pass

class TServerConnectionBase(object):

    def reconnect(self):
        raise NotImplementedError

    def connect(self):
        raise NotImplementedError

    def prepare_request(self, timestamp, live, channels):
        raise NotImplementedError

    def do_request(self, req):
        """ sends request and returns response (without trailing newline).
            if socket is closed, returns None
            throws socket.timeout on timeout """
        raise NotImplementedError

    def get_channels(self):
        raise NotImplementedError

