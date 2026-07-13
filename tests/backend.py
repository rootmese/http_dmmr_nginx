import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

class BackendHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    server = HTTPServer(('localhost', port), BackendHandler)
    print(f'Backend on port {port}')
    server.serve_forever()
