.PHONY: all clean

all:
clean :

install: all
	install -m 0755 sensor-tools-client.py  $(DESTDIR)/usr/bin/sensor-tools-client
	install -m 0644 config.json  $(DESTDIR)/etc/sensor-tools-client.conf






