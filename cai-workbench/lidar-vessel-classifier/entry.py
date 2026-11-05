import http.server
import os
import socketserver

PORT = os.getenv("CDSW_APP_PORT", "8090")

Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("127.0.0.1", int(PORT)), Handler) as httpd:
    print("serving at port", PORT)
    httpd.serve_forever()
