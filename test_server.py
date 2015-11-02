#!/usr/bin/env python

import socket


TCP_IP = '127.0.0.1'
TCP_PORT = 7116
BUFFER_SIZE = 20  # Normally 1024, but we want fast response

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

s.bind((TCP_IP, TCP_PORT))
s.listen(1)

conn, addr = s.accept()
print 'Connection address:', addr
fd = conn.makefile()
while 1:
    data = fd.readline()
    print "received data:", data
    fd.write("@OK\r\n")  # echo
    fd.flush()
conn.close()
