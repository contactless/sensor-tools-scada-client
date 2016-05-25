#coding: utf8
import sys, struct
try:
    import mosquitto
    TMQTTClient = mosquitto.Mosquitto
except ImportError:
    import paho.mqtt.client as mosquitto
    from paho.mqtt.client import MQTT_ERR_NO_CONN

    class TPahoMosquittoUnicodeFix(mosquitto.Mosquitto):
        def _pack_str16(self, packet, data):
            """ this version doesn't try to reencode the strings"""
            if sys.version_info[0] < 3:
                if isinstance(data, bytearray):
                    packet.extend(struct.pack("!H", len(data)))
                    packet.extend(data)
                elif isinstance(data, str):
                    udata = data
                    pack_format = "!H" + str(len(udata)) + "s"
                    packet.extend(struct.pack(pack_format, len(udata), udata))
                elif isinstance(data, unicode):
                    udata = data.encode('utf-8')
                    pack_format = "!H" + str(len(udata)) + "s"
                    packet.extend(struct.pack(pack_format, len(udata), udata))
                else:
                    raise TypeError
            else:
                if isinstance(data, bytearray) or isinstance(data, bytes):
                    packet.extend(struct.pack("!H", len(data)))
                    packet.extend(data)
                elif isinstance(data, str):
                    udata = data.encode('utf-8')
                    pack_format = "!H" + str(len(udata)) + "s"
                    packet.extend(struct.pack(pack_format, len(udata), udata))
                else:
                    raise TypeError

        def subscribe(self, topic, qos=0):
            topic_qos_list = None
            if isinstance(topic, str):
                if qos<0 or qos>2:
                    raise ValueError('Invalid QoS level.')
                if topic is None or len(topic) == 0:
                    raise ValueError('Invalid topic.')
                topic_qos_list = [(topic, qos)]

            if isinstance(topic, unicode):
                if qos<0 or qos>2:
                    raise ValueError('Invalid QoS level.')
                if topic is None or len(topic) == 0:
                    raise ValueError('Invalid topic.')
                topic_qos_list = [(topic.encode('utf8'), qos)]

            elif isinstance(topic, tuple):
                if topic[1]<0 or topic[1]>2:
                    raise ValueError('Invalid QoS level.')
                if topic[0] is None or len(topic[0]) == 0 or not isinstance(topic[0], str):
                    raise ValueError('Invalid topic.')
                topic_qos_list = [(topic[0].encode('utf-8'), topic[1])]
            elif isinstance(topic, list):
                topic_qos_list = []
                for t in topic:
                    if t[1]<0 or t[1]>2:
                        raise ValueError('Invalid QoS level.')
                    if t[0] is None or len(t[0]) == 0 or not isinstance(t[0], str):
                        raise ValueError('Invalid topic.')
                    topic_qos_list.append((t[0].encode('utf-8'), t[1]))

            if topic_qos_list is None:
                raise ValueError("No topic specified, or incorrect topic type.")

            if self._sock is None and self._ssl is None:
                return (MQTT_ERR_NO_CONN, None)

            return self._send_subscribe(False, topic_qos_list)


    TMQTTClient = TPahoMosquittoUnicodeFix

if __name__ == '__main__':
    m = TMQTTClient()
    m.connect('localhost')
    m.subscribe('sdfsdf')
    m.subscribe('sdfsdf')
    m.subscribe('sdfsdf')
    m.subscribe(u'ываыва')
    m.subscribe(u'ываыва')
    m.subscribe('ываыва')
    m.subscribe('ываыва')
    m.publish('ываыва','ываыва')
