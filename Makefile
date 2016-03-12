.PHONY: all clean

all:
clean :

install: all
	install -m 0644 *.py  $(DESTDIR)/usr/lib/wb-scada-client
	chmod a+x $(DESTDIR)/usr/lib/wb-scada-client/wb-scada-client.py
	ln -s /usr/lib/wb-scada-client/wb-scada-client.py  $(DESTDIR)/usr/bin/wb-scada-client	

	install -m 0644 configwb.json  $(DESTDIR)/etc/wb-scada-client.conf
	install -m 0644 wb-scada-client.schema.json  $(DESTDIR)/usr/share/wb-mqtt-confed/schemas






