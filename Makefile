.PHONY: all clean

all:
clean :

install: all
	install -m 0755 wb-scada-client.py  $(DESTDIR)/usr/bin/wb-scada-client
	install -m 0644 config.json  $(DESTDIR)/etc/wb-scada-client.conf






