import Queue
import socket
import math
import logging
import time
import utils
from backends import TServerConnectionBase, TRequestException


class TSensorToolsServerConnection(TServerConnectionBase):
    MAX_RECONNECT_INTERVAL = 60
    MIN_RECONNECT_INTERVAL = 0.1
    RESPONSE_OK = '@OK'
    RESPONSE_FAIL = '@FAIL'
    POLLING_INTERVAL = 60
    DEFAULT_SAVING_INTERVAL = 300
    DEFAULT_TCP_TIMEOUT = 20

    def __init__(self, config):
        self.config = config

        utils.ensure_config_var(self.config, 'server_host')
        utils.ensure_config_var(self.config, 'server_port')
        utils.ensure_config_var(self.config, 'channels')

        self.host = self.config['server_host']
        self.port = self.config['server_port']
        self.timeout = self.config.get('tcp_timeout', self.DEFAULT_TCP_TIMEOUT)
        self.saving_interval = self.config.get('saving_interval_prop',
                                               self.DEFAULT_SAVING_INTERVAL)

        self.client_id = self.config['client_id']

        self.channel_map = {}
        for channel_id, channel in self.config['channels'].iteritems():
            device_id, control_id = channel
            self.channel_map[(str(device_id), str(control_id))] = int(channel_id)

        self.sock = None
        self.fd = None

    def get_channels(self):
        return [(device_id, control_id) for device_id, control_id in self.channel_map.iterkeys()]

    def reconnect(self):
        if self.sock:
            try:
                self.sock.close()
            except:
                pass

        reconnect_interval = self.MIN_RECONNECT_INTERVAL
        while True:
            try:
                self.connect()
            except socket.error, err:
                logging.exception("connect failed")
                time.sleep(reconnect_interval)
                reconnect_interval = min(self.MAX_RECONNECT_INTERVAL, reconnect_interval * 2)
            else:
                return

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.sock.settimeout(self.timeout)
        self.fd = self.sock.makefile()

    def _escape(self, var):
        if isinstance(var, unicode):
            var = var.encode('utf-8', 'ignore')
        if not isinstance(var, str):
            var = str(var)

        return var.replace(':', '').replace(';', '').replace('#', '')

    def prepare_request(self, timestamp, live, channels):
        parts = [("id", self._escape(self.client_id)),
                 ('datetime', timestamp.strftime('%Y%m%d%H%M%S')),
                 ('saving_interval', self.saving_interval),
                 ('live', int(live)),
                 ]

        for k, v in channels.iteritems():
            try:
                _v = float(v)
            except ValueError:
                pass
            else:
                if not math.isnan(_v) and not math.isinf(_v):
                    channel_id = self.channel_map[k]
                    if channel_id is not None:
                        parts.append(('t%d' % channel_id, v))

        var_data = ";".join("%s:%s" % (k, self._escape(v)) for k, v in parts)

        return "".join(("#", var_data, "#"))

    def do_request(self, req):
        """ sends request and returns response (without trailing newline).
            throws TRequestException in case socket is closed or on socket timeout"""

        logging.debug("Sending request: '%s'" % req)

        try:
            self.fd.write(req)
            self.fd.write('\n')
            self.fd.flush()

            response = self.fd.readline()
            if response:
                # strip \r\n
                response = response[:-2]
            else:
                response = None

            if response == self.RESPONSE_OK:
                return True
            elif response == self.RESPONSE_FAIL:
                return False
            elif response is None:
                logging.warning("server closed the connection")
                raise TRequestException()
            else:
                logging.warning("unexpected answer from server: '%s'" % response)
                raise TRequestException()

        except (socket.timeout, socket.error):
            logging.warning("request timeout")
            raise TRequestException()


ServerConnection = TSensorToolsServerConnection
