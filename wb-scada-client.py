#!/usr/bin/python
# coding: utf-8
import json
import argparse
import Queue
import datetime
import time
import os
import collections

from mqttclient import TMQTTClient, mosquitto

import logging
import logging.handlers

from mqttrpc.client import TMQTTRPCClient
# from jsonrpc.exceptions import JSONRPCError

from backends import TServerConnectionBase, TRequestException
import sensor_tools_backend
import wbscada_http_backend
import utils

scada_backends = {
    'sensor-tools': sensor_tools_backend,
    'wbscada-http': wbscada_http_backend
}


logger = logging.getLogger('')
handler = logging.handlers.SysLogHandler(address='/dev/log')
formatter = logging.Formatter('wb-scada-client: %(levelname)s:%(name)s:%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

ch = logging.StreamHandler()
ch_formatter = logging.Formatter('%(asctime)s - %(name)s:%(levelname)s:%(message)s')
ch.setFormatter(ch_formatter)
logger.addHandler(ch)

logging.getLogger('').setLevel(logging.WARNING)



class TSCADAClient(object):
    DEFAULT_QUEUE_SIZE = 100
    DEFAULT_RPC_TIMEOUT = 10
    RPC_MAX_ROWS_PER_QUERY = 100

    SERVER_FAIL_RETRY_TIMEOUT = 10
    SERVER_FAIL_RETRY_ATTEMPTS = 5

    RPC_ERROR_RETRY_TIMEOUT = 10

    def load_last_success_item(self):
        if os.path.exists(self.last_success_item_fname):
            try:
                parts = open(self.last_success_item_fname).read().strip().split()
                self.last_success_timestamp = float(parts[0])
                self.last_success_uid = int(parts[1])
                return
            except:
                logging.warning("can't load last sucessfull item data from file")

    def set_last_success_item(self, timestamp, uid=None):
        self.last_success_timestamp = timestamp
        if uid is not None:
            self.last_success_uid = uid

        if not os.path.exists(os.path.dirname(self.last_success_item_fname)):
            os.makedirs(os.path.dirname(self.last_success_item_fname))

        open(self.last_success_item_fname, 'wt').write("%f %d\n" %
                                                       (self.last_success_timestamp,
                                                        self.last_success_uid))

    def __init__(self, config):
        self.mqtt_client = TMQTTClient()

        self.config = config

        self.live_mode = False
        self.live_queue = Queue.Queue(
            maxsize=int(self.config.get('queue_size', self.DEFAULT_QUEUE_SIZE)))

        self.last_success_timestamp = 0
        self.last_success_uid = -1

        self.data_dir = self.config.get('data_dir', "/var/lib/wb-scada-client")

        self.last_success_item_fname = os.path.join(self.data_dir, 'last_success_item.dat')
        self.load_last_success_item()

        utils.ensure_config_var(self.config, 'connection_backend')

        if self.config['connection_backend'] not in scada_backends:
            raise RuntimeError("Unknown connection backend %s" % self.config['connection_backend'])

        backend = scada_backends[self.config['connection_backend']]

        self.scada_conn = backend.ServerConnection(
            self.config,
            mqtt_client=self.mqtt_client,
            data_dir=self.data_dir,
            parent = self,
        )

        if self.config.get('mqtt_username'):
            self.mqtt_client.username_pw_set(
                self.config['mqtt_username'], self.config.get('mqtt_password', ''))

        self.rpc_client = TMQTTRPCClient(self.mqtt_client)

        self.rpc_timeout = float(self.config.get('rpc_timeout', self.DEFAULT_RPC_TIMEOUT))
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.on_connect = lambda mosq, obj, rc: self.on_mqtt_connect(mosq, obj, rc)

        self.mqtt_client.connect(
            self.config.get('mqtt_host', 'localhost'), self.config.get('mqtt_port', 1883))
        self.mqtt_client.loop_start()

        self.channels_meta = collections.defaultdict(dict)

    def on_mqtt_message(self, mosq, obj, msg):
        logging.debug("got mqtt message on topic %s" % msg.topic)
        if self.rpc_client.on_mqtt_message(mosq, obj, msg):
            return

        if not mosquitto.topic_matches_sub('/devices/+/controls/#', msg.topic):
            return

        parts = msg.topic.split('/')
        device_id = parts[2].decode('utf8')
        control_id = parts[4].decode('utf8')

        channel = (device_id, control_id)

        if mosquitto.topic_matches_sub('/devices/+/controls/+/meta/+', msg.topic):
            meta = parts[6]
            self.channels_meta[channel][meta] = msg.payload
            logging.debug("got meta %s %s %s " % (channel, meta, msg.topic))

        elif mosquitto.topic_matches_sub('/devices/+/controls/+', msg.topic):
            if not self.live_mode:
                return

            try:
                self.live_queue.put_nowait((datetime.datetime.now(),
                                            channel, msg.payload))
            except Queue.Full:
                logging.info("Setting live_mode to false")
                self.live_mode = False

                # do not call Queue methods inside 'with' block!
                # Queue.join() won't work after clearing in this way
                with self.live_queue.mutex:
                    self.live_queue.queue.clear()

    def on_mqtt_connect(self, mosq, obj, rc):
        for device_id, control_id in self.scada_conn.get_channels():
            topic = u"/devices/%s/controls/%s" % (device_id, control_id)
            topic = topic.encode('utf8')
            self.mqtt_client.subscribe(topic + "/meta/+")
            self.mqtt_client.subscribe(topic)

    def process_data_item(self, channel, dt, value, live_mode):

        # пытаемся отправить строку на сервер. Если словили timeout, пытаемся ещё,
        #  пока находимся в live mode
        # если получили FAIL, то (пока) делаем то же самое, но без реконнекта

        server_fail_number = 0

        while True:
            if live_mode:
                if not self.live_mode:
                    return False

            try:
                resp = self.scada_conn.do_request(dt, live=live_mode,
                                                      channels={channel: value})
            except TRequestException:
                logging.warning("error while performing request, reconnecting")
                self.scada_conn.reconnect()
            else:
                if resp:
                    return True
                else:
                    server_fail_number += 1

                    if server_fail_number >= self.SERVER_FAIL_RETRY_ATTEMPTS:
                        logging.warning("server wasn't able to process request %d times, giving up" %
                                        server_fail_number)
                        return False
                    else:
                        logging.warning("server can't process request, trying again")
                        time.sleep(self.SERVER_FAIL_RETRY_TIMEOUT)

    def do_archive_mode_work(self):
        """ return True if there is no more data to send"""

        resp = self.rpc_client.call('db_logger', 'history', 'get_values',
                                    {
                                        'channels': self.scada_conn.get_channels(),
                                        'timestamp': {
                                            'gt': self.last_success_timestamp,
                                        },

                                        'uid': {
                                            'gt': self.last_success_uid,
                                        },
                                        'limit': self.RPC_MAX_ROWS_PER_QUERY,
                                    }, self.rpc_timeout)

        for row in resp.get(u'values', ()):
            channel = (row[u'device'], row[u'control'])

            value = row[u'value']
            dt = datetime.datetime.fromtimestamp(row[u'timestamp'])

            if self.process_data_item(channel, dt, value, live_mode=False):
                self.set_last_success_item(row[u'timestamp'], row[u'uid'])

        if not resp.get(u'has_more'):
            return True

    def loop(self):
        """ main loop"""

        self.scada_conn.reconnect()

        while 1:
            if self.live_mode:
                dt, channel, value = self.live_queue.get(timeout=1E100)
                if self.process_data_item(channel, dt, value, live_mode=True):
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
    parser = argparse.ArgumentParser(description='WB SCADA client', add_help=False)

    parser.add_argument('config_file', type=str,
                        help='Config file location')

    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='debug')

    args = parser.parse_args()
    config = json.load(open(args.config_file))

    if args.debug or config.get('debug', None):
        logging.getLogger().setLevel(logging.DEBUG)

    scada_client = TSCADAClient(config)
    scada_client.loop()


if __name__ == "__main__":
    main()
