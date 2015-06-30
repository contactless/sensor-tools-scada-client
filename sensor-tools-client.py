#!/usr/bin/python
#coding: utf-8
import json
import argparse
import Queue
import socket
import datetime
import time
import os
import mosquitto
import logging
import logging.handlers
import math

from mqttrpc.client import TMQTTRPCClient
from jsonrpc.exceptions import JSONRPCError



logger = logging.getLogger('')
handler = logging.handlers.SysLogHandler(address = '/dev/log')
formatter = logging.Formatter('sensor-tools-client: %(levelname)s:%(name)s:%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

ch = logging.StreamHandler()
ch_formatter = logging.Formatter('%(asctime)s - %(name)s:%(levelname)s:%(message)s')
ch.setFormatter(ch_formatter)
logger.addHandler(ch)

logging.getLogger('').setLevel(logging.WARNING)

class TServerConnection(object):
    MAX_RECONNECT_INTERVAL = 60
    MIN_RECONNECT_INTERVAL = 0.1
    RESPONSE_OK = '@OK'
    RESPONSE_FAIL = '@FAIL'
    POLLING_INTERVAL = 60
    DEFAULT_SAVING_INTERVAL = 300

    def __init__(self, host, port, timeout=1, saving_interval = None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None
        self.fd = None

        self.saving_interval = saving_interval or self.DEFAULT_SAVING_INTERVAL


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
        if  isinstance(var, unicode):
            var = var.encode('utf-8', 'ignore')
        if not isinstance(var, str):
            var = str(var)


        return var.replace(':', '').replace(';','').replace('#','')

    def format_request(self, client_id, timestamp, live, channels):
        parts = [ ("id", self._escape(client_id)),
                  ('datetime', timestamp.strftime('%Y%m%d%H%M%S')),
                  ('saving_interval' , self.saving_interval),
                  ('live' , int(live)),
                ]

        for k, v in channels.iteritems():
            try:
                _v = float(v)
            except ValueError:
                pass
            else:
                if not math.isnan(_v) and not math.isinf(_v):
                    parts.append(('t%d' % k, v))


        var_data = ";".join( "%s:%s" % (k, self._escape(v)) for k, v in parts)

        return "".join( ("#", var_data, "#"))

    def do_request(self, req):
        """ sends request and returns response (without trailing newline).
            if socket is closed, returns None
            throws socket.timeout on timeout """

        logging.debug("Sending request: '%s'" % req)

        self.fd.write(req)
        self.fd.write('\n')
        self.fd.flush()

        response = self.fd.readline()
        if response:
            # strip \r\n
            return response[:-2]
        else:
            return None


class TSensorToolsClient(object):
    DEFAULT_TCP_TIMEOUT = 20
    DEFAULT_QUEUE_SIZE = 100
    DEFAULT_RPC_TIMEOUT = 10
    RPC_MAX_ROWS_PER_QUERY = 100

    SERVER_FAIL_RETRY_TIMEOUT = 10
    SERVER_FAIL_RETRY_ATTEMPTS = 5

    RPC_ERROR_RETRY_TIMEOUT = 10

    def _ensure_config_var(self, obj, var):
        if var not in obj:
            raise RuntimeError('Missing mandatory option %s in config' % var)





    def load_last_success_item(self):
        if os.path.exists(self.last_success_item_fname):
            try:
                parts = open(self.last_success_item_fname).read().strip().split()
                self.last_success_timestamp = float(parts[0])
                self.last_success_uid  = int(parts[1])
                return
            except:
                logging.warning("can't load last sucessfull item data from file")


    def set_last_success_item(self, timestamp, uid = None):
        self.last_success_timestamp = timestamp
        if uid is not None:
            self.last_success_uid = uid

        if not os.path.exists(os.path.dirname(self.last_success_item_fname)):
            os.makedirs(os.path.dirname(self.last_success_item_fname))

        open(self.last_success_item_fname, 'wt').write("%f %d\n" % (self.last_success_timestamp, self.last_success_uid))


    def __init__(self, config):
        self.mqtt_client = mosquitto.Mosquitto()


        self.config = config

        self.live_mode = False
        self.live_queue = Queue.Queue(maxsize=int(self.config.get('queue_size', self.DEFAULT_QUEUE_SIZE)))

        self._ensure_config_var(self.config, 'server_host')
        self._ensure_config_var(self.config, 'server_port')

        self._ensure_config_var(self.config, 'channels')

        self.scada_client_id = self.config['client_id']

        self.last_success_timestamp = 0
        self.last_success_uid  = -1
        self.last_success_item_fname = self.config.get('last_success_file', '/var/lib/sensor-tools/last_success_item.dat')
        self.load_last_success_item()


        self.channel_map = {}
        for channel_id, channel in self.config['channels'].iteritems():
            device_id, control_id = channel
            self.channel_map[(str(device_id), str(control_id))]  = int(channel_id)


        self.scada_conn = TServerConnection(self.config['server_host'], self.config['server_port'],
                                            self.config.get('tcp_timeout', self.DEFAULT_TCP_TIMEOUT),
                                            saving_interval = self.config.get('saving_interval_prop'))


        if self.config.get('mqtt_username'):
            self.mqtt_client.username_pw_set(self.config['mqtt_username'], self.config.get('mqtt_password', ''))


        self.rpc_client = TMQTTRPCClient(self.mqtt_client)

        self.rpc_timeout = float(self.config.get('rpc_timeout', self.DEFAULT_RPC_TIMEOUT))
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.on_connect = self.on_mqtt_connect

        self.mqtt_client.connect(self.config.get('mqtt_host', 'localhost'), self.config.get('mqtt_port', 1883))
        self.mqtt_client.loop_start()

    def on_mqtt_message(self, mosq, obj, msg):
        if self.rpc_client.on_mqtt_message(mosq, obj, msg):
            return


        if not self.live_mode:
            return

        if not mosquitto.topic_matches_sub('/devices/+/controls/+', msg.topic):
            return

        parts = msg.topic.split('/')
        device_id = parts[2]
        control_id = parts[4]

        channel_id  = self.channel_map.get((device_id, control_id))
        if channel_id:
            try:
                self.live_queue.put_nowait((datetime.datetime.now(), channel_id, msg.payload))
            except Queue.Full, exc:
                logging.info("Setting live_mode to false")
                self.live_mode = False

                # do not call Queue methods inside 'with' block!
                # Queue.join() won't work after clearing in this way
                with self.live_queue.mutex:
                    self.live_queue.queue.clear()



    def on_mqtt_connect(self, mosq, obj, rc):
        for device_id, control_id in self.channel_map.iterkeys():
            topic = "/devices/%s/controls/%s" % (device_id, control_id)
            self.mqtt_client.subscribe(topic)

    def process_data_item(self, channel_id, dt, value, live_mode):
        req = self.scada_conn.format_request(self.scada_client_id, dt, live=live_mode, channels={channel_id : value})

        # пытаемся отправить строку на сервер. Если словили timeout, пытаемся ещё, пока находимся в live mode
        # если получили FAIL, то (пока) делаем то же самое, но без реконнекта

        server_fail_number = 0

        while True:
            if live_mode:
                if not self.live_mode:
                    return False

            try:
                resp = self.scada_conn.do_request(req)
            except (socket.timeout, socket.error):
                logging.warning("request timeout, reconnecting")
                self.scada_conn.reconnect()
            else:
                if resp == self.scada_conn.RESPONSE_OK:
                    return True
                elif resp == self.scada_conn.RESPONSE_FAIL:
                    server_fail_number += 1

                    if server_fail_number >= self.SERVER_FAIL_RETRY_ATTEMPTS:
                        logging.warning("server returned @FAIL %d times, giving up" % server_fail_number)
                        return False
                    else:
                        logging.warning("server returned @FAIL, trying again")
                        time.sleep(self.SERVER_FAIL_RETRY_TIMEOUT)
                elif resp is None:
                    logging.warning("server closed the connection")
                    self.scada_conn.reconnect()
                else:
                    logging.warning("unexpected answer from server: '%s'" % resp)
                    self.scada_conn.reconnect()


    def do_archive_mode_work(self):
        """ return True if there is no more data to send"""

        resp =  self.rpc_client.call('db_logger', 'history', 'get_values', {
                                        'channels': self.channel_map.keys(),
                                        'timestamp' : {
                                            'gt': self.last_success_timestamp,
                                        },

                                        'uid' : {
                                            'gt': self.last_success_uid,
                                        },
                                        'limit' : self.RPC_MAX_ROWS_PER_QUERY,
                                    }, self.rpc_timeout)


        for row in resp.get(u'values', ()):
                channel_id = self.channel_map.get((str(row[u'device']),str(row[u'control'])))

                assert channel_id is not None

                value = str(row[u'value'])
                dt = datetime.datetime.fromtimestamp(row[u'timestamp'])

                if self.process_data_item(channel_id, dt, value, live_mode=False):
                    self.set_last_success_item(row[u'timestamp'], row[u'uid'])

        if not resp.get(u'has_more'):
            return True


    def loop(self):
        """ main loop"""

        self.scada_conn.reconnect()

        while 1:
            if self.live_mode:
                dt, channel_id, value  = self.live_queue.get(timeout=1E100)
                if self.process_data_item(channel_id, dt, value, live_mode=True):
                    self.set_last_success_item(time.mktime(dt.timetuple()))

            else:
                logging.debug("not in live mode")
                try:
                    done = self.do_archive_mode_work()
                except AssertionError:
                    raise
                except:
                    logging.exception("error in archive mode handler")
                    time.sleep(self.RPC_ERROR_RETRY_TIMEOUT)
                else:
                    if done:
                        logging.debug("going back to live mode")
                        self.live_mode = True





def main():
    parser = argparse.ArgumentParser(description='Sensor-Tools TCP SCADA client', add_help=False)

    parser.add_argument('config_file', type=str,
                     help='Config file location')

    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                     help='debug')

    args = parser.parse_args()
    config = json.load(open(args.config_file))


    if args.debug or config.get('debug', None):
        logging.getLogger().setLevel(logging.DEBUG)


    scada_client = TSensorToolsClient(config)
    scada_client.loop()


if __name__ == "__main__":
    main()
