from backends import TServerConnectionBase, TRequestException
import utils
import time
import math
import os


import json
import logging
import threading
import requests


import httplib

# httplib.HTTPConnection.debuglevel = 1

logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True


class TCmdFeedHandler(object):
    LONG_POLL_TIMEOUT = 120

    def __init__(self, server_connection):
        self.server_connection = server_connection

        self.cmd_feed_sess = requests.Session()
        self.cmd_feed_sess.auth = (self.server_connection.client_id, self.server_connection.password)
        self.cmd_feed_sess.headers.update({'Content-type': 'application/json'})

        self.ts_fname = os.path.join(self.server_connection.data_dir, "cmd_feed_ts.dat")
        if os.path.exists(self.ts_fname):
            self.last_ts = open(self.ts_fname).read().strip()
        else:
            self.last_ts = None

    def make_single_request(self):
        resp = self.cmd_feed_sess.get(self.server_connection.url_base + "cmd_feed/%s" % self.server_connection.hw_id,
                                      params={'last_ts': self.last_ts}, timeout=self.LONG_POLL_TIMEOUT)
        resp.raise_for_status()
        logging.debug("got response from cmd feed: %s" % resp.text)
        return resp.text

    def process_cmd_feed_response(self, resp):
        parsed_resp = json.loads(resp)
        self.last_ts = parsed_resp['ts']

        open(self.ts_fname, "wt").write("%s\n" % self.last_ts)

        if 'push_mqtt' in parsed_resp:
            channels = parsed_resp['push_mqtt']['channels']
            for channel, value in channels.iteritems():
                device_id, control_id = channel.split('/', 1)
                topic = "/devices/%s/controls/%s/on" % (device_id, control_id)
                self.server_connection.mqtt_client.publish(topic,
                                                           payload=value.encode('utf8'),
                                                           qos=2,
                                                           retain=False)
                print "%s==>%s" % (channel, value)

    def thread_func(self):

        while True:
            try:
                resp = self.make_single_request()
            except (requests.exceptions.Timeout,):
                logging.info("timeout on long poll channel, ignoring")
                continue
            except (requests.exceptions.RequestException, ):
                logging.exception("Error requesting cmd feed from server")
                time.sleep(10)
                continue

            try:
                self.process_cmd_feed_response(resp)
            except:
                logging.exception("error processing cmd feed response from server:" + resp)
                time.sleep(10)
                continue

    def start(self):
        self.thread = threading.Thread(target=self.thread_func)
        self.thread.daemon = True
        self.thread.start()


class TWBSCADAServerConnection(object):
    MAX_RECONNECT_INTERVAL = 60
    MIN_RECONNECT_INTERVAL = 0.1
    POLLING_INTERVAL = 60
    DEFAULT_SAVING_INTERVAL = 300
    DEFAULT_TCP_TIMEOUT = 20

    def __init__(self, config, **kwargs):
        self.config = config
        self.mqtt_client = kwargs['mqtt_client']
        self.data_dir = kwargs['data_dir']

        #FIXME:
        self.parent = kwargs['parent']

        utils.ensure_config_var(self.config, 'server_url_base')
        utils.ensure_config_var(self.config, 'channels')
        utils.ensure_config_var(self.config, 'client_id')
        utils.ensure_config_var(self.config, 'password')

        self.url_base = self.config['server_url_base']
        if not self.url_base.endswith('/'):
            self.url_base += '/'

        self.timeout = self.config.get('tcp_timeout', self.DEFAULT_TCP_TIMEOUT)

        self.client_id = self.config['client_id']
        self.password = self.config['password']

        self.channels = set()
        for item in self.config['channels']:
            device_id, control_id = item["device"], item["control"]
            self.channels.add(
                (device_id.encode('utf8'), control_id.encode('utf8')))

        self.sock = None
        self.fd = None

        self.req_sess = requests.Session()
        self.req_sess.auth = (self.client_id, self.password)
        self.req_sess.headers.update({'Content-type': 'application/json'})

        self.hw_id = utils.get_stored_serial()#.replace(':', '')
        self.req_counter = 0

        self.meta_sent = set()

        self.cmd_feed_handler = TCmdFeedHandler(self)
        self.cmd_feed_handler.start()

    def get_channels(self):
        return [(device_id, control_id)
                for device_id, control_id in self.channels]

    def reconnect(self):
        logging.info("reconnect!")

        reconnect_interval = self.MIN_RECONNECT_INTERVAL
        while True:
            try:
                self.connect()
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.HTTPError):

                logging.exception("connect failed")
                time.sleep(reconnect_interval)
                reconnect_interval = min(
                    self.MAX_RECONNECT_INTERVAL, reconnect_interval * 2)
            else:
                return

    def connect(self):
        self.meta_sent.clear()

        body = json.dumps({
            'client_version': '0.1',
            'wb_version': utils.get_wb_version(),
            'eth_ip': utils.get_iface_ip('eth0'),
            'wlan_ip': utils.get_iface_ip('wlan0'),
            'gprs_ip': utils.get_iface_ip('ppp0')
        })
        print body
        resp = self.req_sess.post(
            self.url_base + "post_client_info/%s" % self.hw_id, body, timeout=self.timeout)
        print "conn resp: ", resp
        print "text:", resp.text
        resp.raise_for_status()


    def post_channels_meta(self, channels):
        # FIXME: fix stub here
    
        meta_payload = {"channels": {}}

        for channel in channels:
            if channel not in self.meta_sent:
                meta_type = self.parent.channels_meta[channel].get('type')
                if meta_type is None:
                    logging.warning("Meta type unknown for channel %s/%s" % channel)
                else:
                    meta_payload["channels"]["%s/%s" % channel] = {
                            "type": meta_type
                        }

        if not meta_payload["channels"]:
            return

        print "meta_payload: ", json.dumps(meta_payload)
        resp = self.req_sess.post(self.url_base + "post_channel_meta/%s" % self.hw_id,
                                  json.dumps(meta_payload), timeout=self.timeout)

        print "meta conn resp: ", resp
        print "text:", resp.text
        resp.raise_for_status()

        for channel in channels:
            self.meta_sent.add(channel)

    def prepare_request(self, timestamp, live, channels):
        self.req_counter += 1
        payload = {
            'id': self.req_counter,
            'live': live,
            'data': {},
        }

        for k, v in channels.iteritems():
            try:
                _v = float(v)
            except ValueError:
                pass
            else:
                if not math.isnan(_v) and not math.isinf(_v):
                    if k in self.channels:
                        if k not in self.meta_sent:
                            logging.warning("Meta type not sent for channel %s/%s, ingoring" % k)
                        else:
                            device, control = k
                            channel_id = "%s/%s" % (device, control)
                            payload['data'].setdefault(channel_id, [])
                            payload['data'][channel_id].append(
                                {
                                    "v": v,
                                    "t": time.mktime(timestamp.timetuple()) +
                                    timestamp.microsecond / 1E6,
                                }
                            )

        return json.dumps(payload)

    def do_request(self, timestamp, live, channels):
        """ sends request and returns response (without trailing newline).
            if socket is closed, returns None
            throws socket.timeout on timeout """

        self.post_channels_meta(self.channels)
        req = self.prepare_request(timestamp, live, channels)
        logging.debug("Sending request: '%s'" % req)

        try:
            resp = self.req_sess.post(self.url_base + "post_data/%s" % self.hw_id,
                                      req,
                                      timeout=self.timeout)
        except requests.exceptions.ConnectionError:
            raise
            raise TRequestException()
        else:
            print "conn resp: ", resp
            if resp.status_code != requests.codes.no_content:
                    print "text:", resp.text
            resp.raise_for_status()
            return True


ServerConnection = TWBSCADAServerConnection
