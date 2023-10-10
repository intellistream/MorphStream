#!/usr/bin/env python3

import time, sys, os, socket, threading
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from socketserver import ThreadingMixIn

class Handler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.do_POST()

  def do_POST(self):
    content_length = int(self.headers['Content-Length'])
    body = self.rfile.read(content_length)
    # print(self.headers)
    # print(body)
    self.send_response(200)
    self.end_headers()
    response = BytesIO()
    response.write(body)
    self.wfile.write(response.getvalue())

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


try:
  ip_addr = sys.argv[1]
  port = int(sys.argv[2])
  server = ThreadedHTTPServer((ip_addr, port), Handler)
  print('Starting server, use <Ctrl-C> to stop')
  server.serve_forever()
except IndexError:
  print('Usage: python3 {} <ip-addr> <port>'.format(sys.argv[0]))  
except Exception as e:
  print(e)
  exit(-1)
