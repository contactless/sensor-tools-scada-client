#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import cgi

PORT_NUMBER = 7020

# This class will handles any incoming request from
# the browser


class HttpHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    # Handler for the GET requests
    def do_GET(self):
        print self.headers
        print self.client_address
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        message = "Hello World !"
        self.send_header("Content-length", str(len(message)))
        self.end_headers()
        # Send the html message
        self.wfile.write(message)
        return

    # Handler for the GET requests
    def do_POST(self):
        print self.headers
        print self.client_address

        length = int(self.headers.getheader('content-length'))
        payload = self.rfile.read(length)
        print "got payload", payload

        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        message = "Hello POST World !"
        self.send_header("Content-length", str(len(message)))
        self.end_headers()
        # Send the html message
        self.wfile.write(message)
        return


server = HTTPServer(('', PORT_NUMBER), HttpHandler)
print 'Started httpserver on port ', PORT_NUMBER

# Wait forever for incoming htto requests
server.serve_forever()

server.socket.close()
