#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Suite de testes automatizados para o Gateway HTTP DMMR (Cache Service + Nginx module).

Requisitos:
- Python 3.6+
- Bibliotecas: requests, psutil (opcional)
- O binário 'dmmr_cache' deve estar compilado.
- O Nginx DEVE estar em execução com o módulo DMMR carregado e configurado adequadamente.
- As portas 8080 (TCP) e 9081 (Unix socket) devem estar configuradas no Nginx.
- Os backends são iniciados por um servidor HTTP customizado (não o http.server).
"""

import os
import sys
import time
import signal
import socket
import struct
import unittest
import subprocess
import tempfile
import shutil
import threading
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import Optional, Tuple, List

# Tenta importar psutil (opcional)
try:
    import psutil
except ImportError:
    psutil = None

# ----------------------------------------------------------------------
# Configurações
# ----------------------------------------------------------------------
CACHE_BIN = "../http_dmmr_cache/dmmr_cache"          # caminho para o binário do cache
CACHE_SOCK = "/tmp/dmmr_cache.sock"
CACHE_TCP_PORT = 9080
BACKEND1_PORT = 8001
BACKEND2_PORT = 8002
NGINX_TCP_PORT = 8080      # porta para TCP
NGINX_UNIX_PORT = 9081     # porta para Unix socket
RATE_LIMIT = 120
RATE_WINDOW_MS = 60000
DMMR_API_KEY = os.environ.get("DMMR_API_KEY", "123456")

# ----------------------------------------------------------------------
# Servidor HTTP backend customizado (substituto do http.server)
# ----------------------------------------------------------------------
class BackendHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(self.server.backend_message)))
        self.end_headers()
        self.wfile.write(self.server.backend_message.encode())

    def do_POST(self):
        self.do_GET()

    def log_message(self, format, *args):
        # Suprime logs para não poluir a saída dos testes
        pass

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True
    def __init__(self, address, handler_class, backend_message):
        self.backend_message = backend_message
        super().__init__(address, handler_class)

def start_backend_server(port, message):
    """Inicia um servidor HTTP customizado em background."""
    server = ThreadedHTTPServer(("127.0.0.1", port), BackendHandler, message)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread

# ----------------------------------------------------------------------
# Utilitários para o protocolo binário
# ----------------------------------------------------------------------
DMMR_MAGIC = 0xD4D4
DMMR_VERSION = 1
OP_GET = 1
OP_SET = 2
OP_DEL = 3
OP_SYNC = 4
FLAG_NONE = 0

def pack_frame(opcode: int, key: bytes, value: bytes = b'', timestamp: int = 0) -> bytes:
    frame = struct.pack(
        '!HHHHIIQ',
        DMMR_MAGIC, DMMR_VERSION, opcode, FLAG_NONE, len(key), len(value), timestamp
    )
    return frame + key + value

def unpack_response(data: bytes) -> Tuple[int, bytes]:
    if len(data) < 8:
        raise ValueError("Resposta muito curta")
    status = struct.unpack('!H', data[:2])[0]
    payload_len = struct.unpack('!I', data[2:6])[0]
    payload = data[6:6+payload_len]
    return status, payload

def pack_legacy_get(key: bytes) -> bytes:
    return struct.pack('!HH', 1, len(key)) + key

def unpack_legacy_response(data: bytes) -> Tuple[int, bytes]:
    if len(data) < 4:
        raise ValueError("Resposta legada muito curta")
    status = struct.unpack('!H', data[:2])[0]
    payload_len = struct.unpack('!H', data[2:4])[0]
    payload = data[4:4+payload_len]
    return status, payload

# ----------------------------------------------------------------------
# Cliente do Cache Service
# ----------------------------------------------------------------------
class CacheClient:
    def __init__(self, path: str = None, host: str = '127.0.0.1', port: int = 9080,
                 use_unix: bool = True):
        self.use_unix = use_unix
        self.path = path or CACHE_SOCK
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) if self.use_unix else \
                    socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.use_unix:
            self.sock.connect(self.path)
        else:
            self.sock.connect((self.host, self.port))
        self.sock.settimeout(5.0)
        return self

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def send_frame(self, opcode: int, key: bytes, value: bytes = b'', timestamp: int = 0) -> Tuple[int, bytes]:
        if self.sock is None:
            self.connect()
        frame = pack_frame(opcode, key, value, timestamp)
        self.sock.sendall(frame)
        header = self._recv_exact(8)
        status = struct.unpack('!H', header[:2])[0]
        payload_len = struct.unpack('!I', header[2:6])[0]
        payload = self._recv_exact(payload_len) if payload_len else b''
        return status, payload

    def send_legacy_get(self, key: bytes) -> Tuple[int, bytes]:
        if self.sock is None:
            self.connect()
        frame = pack_legacy_get(key)
        self.sock.sendall(frame)
        header = self._recv_exact(4)
        status = struct.unpack('!H', header[:2])[0]
        payload_len = struct.unpack('!H', header[2:4])[0]
        payload = self._recv_exact(payload_len) if payload_len else b''
        return status, payload

    def _recv_exact(self, n: int) -> bytes:
        data = b''
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Conexão fechada")
            data += chunk
        return data

# ----------------------------------------------------------------------
# Funções auxiliares de espera ativa
# ----------------------------------------------------------------------
def wait_for_socket(path, timeout=10, step=0.2):
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(path):
            return True
        time.sleep(step)
    return False

def wait_for_http(url, timeout=10, step=0.2):
    start = time.time()
    while time.time() - start < timeout:
        try:
            requests.get(url, timeout=1)
            return True
        except requests.ConnectionError:
            time.sleep(step)
    return False

def wait_for_port(host, port, timeout=10, step=0.2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(step)
    start = time.time()
    while time.time() - start < timeout:
        try:
            s.connect((host, port))
            s.close()
            return True
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(step)
    return False

# ----------------------------------------------------------------------
# Suite de Testes
# ----------------------------------------------------------------------
class DMMRTestSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache_proc = None
        cls.backend_servers = []   # lista de (server, thread)
        cls.log_files = []         # para armazenar logs em caso de falha

        cls._start_backends()
        cls._start_cache(['--unix', '--tcp'])

        # Verifica se o Nginx está acessível em ambas as portas
        for port in (NGINX_TCP_PORT, NGINX_UNIX_PORT):
            if not wait_for_http(f'http://127.0.0.1:{port}/', timeout=3):
                raise RuntimeError(
                    f"Nginx não está respondendo na porta {port}. "
                    "Certifique-se de que o Nginx está em execução com o módulo DMMR carregado."
                )

    @classmethod
    def tearDownClass(cls):
        cls._stop_cache()
        cls._stop_backends()
        # Remove arquivos de log temporários se não houve falha
        for f in cls.log_files:
            if os.path.exists(f):
                try:
                    os.unlink(f)
                except:
                    pass

    # ------------------------------------------------------------------
    # Gerenciamento de backends (servidor HTTP customizado)
    # ------------------------------------------------------------------
    @classmethod
    def _start_backends(cls):
        """Inicia dois backends HTTP customizados."""
        messages = {
            BACKEND1_PORT: "Backend 8001",
            BACKEND2_PORT: "Backend 8002",
        }
        for port, msg in messages.items():
            server, thread = start_backend_server(port, msg)
            cls.backend_servers.append((server, thread))
            if not wait_for_port("127.0.0.1", port, timeout=5):
                raise RuntimeError(f"Backend na porta {port} não iniciou")

    @classmethod
    def _stop_backends(cls):
        for server, thread in cls.backend_servers:
            server.shutdown()
            server.server_close()
            thread.join(timeout=1)
        cls.backend_servers.clear()

    # ------------------------------------------------------------------
    # Gerenciamento do Cache Service
    # ------------------------------------------------------------------
    @classmethod
    def _start_cache(cls, args: List[str] = None):
        # Mata qualquer processo anterior
        if cls.cache_proc and cls.cache_proc.poll() is None:
            cls._stop_cache()
            # Aguarda a porta ser liberada
            time.sleep(0.5)

        cmd = [CACHE_BIN] + (args or ['--unix', '--tcp'])
        stdout_tmp = tempfile.NamedTemporaryFile(delete=False, prefix="cache_stdout_", suffix=".log")
        stderr_tmp = tempfile.NamedTemporaryFile(delete=False, prefix="cache_stderr_", suffix=".log")
        cls.log_files.extend([stdout_tmp.name, stderr_tmp.name])

        cls.cache_proc = subprocess.Popen(
            cmd,
            stdout=stdout_tmp,
            stderr=stderr_tmp,
            preexec_fn=os.setsid if os.name != 'nt' else None
        )
        stdout_tmp.close()
        stderr_tmp.close()

        if not wait_for_socket(CACHE_SOCK, timeout=10):
            cls._dump_logs(stdout_tmp.name, stderr_tmp.name)
            raise RuntimeError("Timeout aguardando cache server iniciar")
        time.sleep(0.5)

    @classmethod
    def _stop_cache(cls):
        if cls.cache_proc and cls.cache_proc.poll() is None:
            if os.name != 'nt':
                os.killpg(os.getpgid(cls.cache_proc.pid), signal.SIGINT)
            else:
                cls.cache_proc.terminate()
            cls.cache_proc.wait(timeout=5)
        cls.cache_proc = None
        if os.path.exists(CACHE_SOCK):
            os.unlink(CACHE_SOCK)

    @classmethod
    def _dump_logs(cls, stdout_path, stderr_path):
        if os.path.exists(stdout_path):
            with open(stdout_path, 'r') as f:
                sys.stderr.write(f"=== Cache STDOUT ===\n{f.read()}\n")
        if os.path.exists(stderr_path):
            with open(stderr_path, 'r') as f:
                sys.stderr.write(f"=== Cache STDERR ===\n{f.read()}\n")

    # ------------------------------------------------------------------
    # Métodos auxiliares para testar ambas as portas do Nginx
    # ------------------------------------------------------------------
    def _test_on_both_ports(self, test_func):
        """Executa test_func para ambas as portas (TCP e Unix)."""
        for port in (NGINX_TCP_PORT, NGINX_UNIX_PORT):
            with self.subTest(port=port):
                test_func(port)

    # ==================================================================
    # Testes
    # ==================================================================

    # 1. Cache Service (não usa Nginx)
    def test_cache_service_start_stop(self):
        self._stop_cache()
        self._start_cache(['--unix', '--tcp'])
        self.assertTrue(os.path.exists(CACHE_SOCK))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            result = s.connect_ex(('127.0.0.1', CACHE_TCP_PORT))
            self.assertEqual(result, 0)
        self._stop_cache()
        self.assertFalse(os.path.exists(CACHE_SOCK))
        if self.cache_proc:
            self.assertIsNotNone(self.cache_proc.poll())
        # Reinicia para os próximos testes
        self._start_cache(['--unix', '--tcp'])

    def test_cache_service_modes(self):
        modes = [
            (['--unix'], True, False),
            (['--tcp'], False, True),
            (['--both'], True, True),
        ]
        for args, unix_exp, tcp_exp in modes:
            with self.subTest(args=args):
                self._stop_cache()
                self._start_cache(args)
                self.assertEqual(os.path.exists(CACHE_SOCK), unix_exp)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    result = s.connect_ex(('127.0.0.1', CACHE_TCP_PORT))
                    self.assertEqual(result == 0, tcp_exp)
                self._stop_cache()
        # Reinicia no modo normal
        self._start_cache(['--unix', '--tcp'])

    # 2. Protocolo moderno (testa cache diretamente)
    def test_protocol_modern_set_get_del(self):
        with CacheClient(use_unix=True) as client:
            status, _ = client.send_frame(OP_SET, b'key', b'val')
            self.assertEqual(status, 0)
            status, payload = client.send_frame(OP_GET, b'key')
            self.assertEqual(status, 0)
            self.assertEqual(payload, b'val')
            status, _ = client.send_frame(OP_DEL, b'key')
            self.assertEqual(status, 0)
            status, _ = client.send_frame(OP_GET, b'key')
            self.assertEqual(status, 1)

    def test_protocol_modern_cases(self):
        with CacheClient(use_unix=True) as client:
            cases = [(b'k', b'v'), (b'x'*100, b'y'*200), (b'large', b'z'*4096), (b'empty', b'')]
            for k, v in cases:
                status, _ = client.send_frame(OP_SET, k, v)
                self.assertEqual(status, 0)
                status, payload = client.send_frame(OP_GET, k)
                self.assertEqual(status, 0)
                self.assertEqual(payload, v)

            # Opcode inválido
            try:
                status, _ = client.send_frame(99, b'key', b'val')
                self.assertNotEqual(status, 0)
            except (socket.timeout, ConnectionError):
                pass

            # Pacote incompleto
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(CACHE_SOCK)
            s.sendall(pack_frame(OP_GET, b'key')[:10])
            try:
                data = s.recv(8, socket.MSG_DONTWAIT)
                if data:
                    status = struct.unpack('!H', data[:2])[0]
                    self.assertNotEqual(status, 0)
            except socket.timeout:
                pass
            finally:
                s.close()

            with CacheClient(use_unix=True) as client2:
                status, _ = client2.send_frame(OP_GET, b'k')
                self.assertEqual(status, 0)

    def test_protocol_legacy_get(self):
        with CacheClient(use_unix=True) as client:
            client.send_frame(OP_SET, b'legacy_key', b'legacy_value')
            status, payload = client.send_legacy_get(b'legacy_key')
            self.assertEqual(status, 0)
            self.assertEqual(payload, b'legacy_value')
        # SET legado deve ser rejeitado
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(CACHE_SOCK)
            s.sendall(struct.pack('!HH', 2, 3) + b'abc')
            data = s.recv(4)
            status = struct.unpack('!H', data[:2])[0]
            self.assertNotEqual(status, 0)

    # 3. Nginx - Roteamento (testa ambas as portas)
    def test_nginx_routing(self):
        headers = {'Authorization': f'Bearer {DMMR_API_KEY}'}

        def _test(port):
            resp1 = requests.get(f'http://127.0.0.1:{port}/api/v1', headers=headers)
            self.assertEqual(resp1.status_code, 200)
            self.assertIn(b'Backend 8001', resp1.content)
            self.assertEqual(resp1.headers.get('Content-Type'), 'text/plain')

            resp2 = requests.get(f'http://127.0.0.1:{port}/api/v2', headers=headers)
            self.assertEqual(resp2.status_code, 200)
            self.assertIn(b'Backend 8002', resp2.content)

            resp3 = requests.get(f'http://127.0.0.1:{port}/api/v3', headers=headers)
            self.assertEqual(resp3.status_code, 404)

        self._test_on_both_ports(_test)

    # 4. Nginx - Backend failure (testa ambas)
    def test_nginx_backend_failure(self):
        # Mata o primeiro backend
        server, _ = self.backend_servers[0]
        server.shutdown()
        server.server_close()
        time.sleep(0.5)

        headers = {'Authorization': f'Bearer {DMMR_API_KEY}'}

        def _test(port):
            try:
                resp = requests.get(f'http://127.0.0.1:{port}/api/v1', headers=headers, timeout=2)
                self.assertIn(resp.status_code, (500, 502, 503, 504))
            except requests.ConnectionError:
                self.fail(f"Nginx na porta {port} deve responder mesmo com backend morto")

        self._test_on_both_ports(_test)

        # Restaura backend
        self._stop_backends()
        self._start_backends()
        time.sleep(0.5)

        # Verifica recuperação em ambas
        def _test_recovery(port):
            resp = requests.get(f'http://127.0.0.1:{port}/api/v1', headers=headers)
            self.assertEqual(resp.status_code, 200)

        self._test_on_both_ports(_test_recovery)

    # 5. Nginx - Cache unavailable (testa ambas)
    def test_nginx_cache_unavailable(self):
        self._stop_cache()
        headers = {'Authorization': f'Bearer {DMMR_API_KEY}'}

        def _test(port):
            try:
                resp = requests.get(f'http://127.0.0.1:{port}/api/v1', headers=headers, timeout=2)
                self.assertIn(resp.status_code, (500, 502, 503))
            except requests.ConnectionError:
                self.fail(f"Nginx na porta {port} deve responder mesmo com cache indisponível")

        self._test_on_both_ports(_test)

        # Restaura cache
        self._start_cache(['--unix', '--tcp'])

    # 6. Autenticação (testa ambas)
    def test_authentication(self):
        headers_valid = {'Authorization': f'Bearer {DMMR_API_KEY}'}
        headers_invalid = {'Authorization': 'Bearer invalid'}
        headers_none = {}

        def _test(port):
            resp = requests.get(f'http://127.0.0.1:{port}/api/v1', headers=headers_valid)
            self.assertNotIn(resp.status_code, (401, 403))

            resp = requests.get(f'http://127.0.0.1:{port}/api/v1', headers=headers_invalid)
            self.assertIn(resp.status_code, (401, 403))

            resp = requests.get(f'http://127.0.0.1:{port}/api/v1', headers=headers_none)
            self.assertEqual(resp.status_code, 401)

            special_key = 'áéíóú!@#'
            resp = requests.get(f'http://127.0.0.1:{port}/api/v1',
                                headers={'Authorization': f'Bearer {special_key}'})
            self.assertIn(resp.status_code, (401, 403))

        self._test_on_both_ports(_test)

    # 7. Rate Limiting (testa ambas)
    def test_rate_limiting(self):
        headers = {'Authorization': f'Bearer {DMMR_API_KEY}'}

        def _test(port):
            url = f'http://127.0.0.1:{port}/api/v1'
            success_count = 0
            for _ in range(130):
                resp = requests.get(url, headers=headers)
                if resp.status_code != 429:
                    success_count += 1
                else:
                    break
            self.assertGreaterEqual(success_count, 115)
            self.assertLessEqual(success_count, 130)

            time.sleep(61)
            resp = requests.get(url, headers=headers)
            self.assertNotEqual(resp.status_code, 429)

        self._test_on_both_ports(_test)

    # 8. Carga (testa ambas)
    def test_load_simulation(self):
        headers = {'Authorization': f'Bearer {DMMR_API_KEY}'}

        def _test(port):
            url = f'http://127.0.0.1:{port}/api/v1'
            stats = {'200': 0, '4xx': 0, '5xx': 0, 'timeout': 0, 'exception': 0}
            lock = threading.Lock()

            def worker():
                for _ in range(50):
                    try:
                        resp = requests.get(url, headers=headers, timeout=2)
                        with lock:
                            if resp.status_code == 200:
                                stats['200'] += 1
                            elif 400 <= resp.status_code < 500:
                                stats['4xx'] += 1
                            elif 500 <= resp.status_code < 600:
                                stats['5xx'] += 1
                    except requests.Timeout:
                        with lock:
                            stats['timeout'] += 1
                    except Exception:
                        with lock:
                            stats['exception'] += 1

            threads = []
            for _ in range(20):
                t = threading.Thread(target=worker)
                t.start()
                threads.append(t)
            for t in threads:
                t.join()

            total = sum(stats.values())
            self.assertGreater(total, 0)
            self.assertGreater(stats['200'] / total, 0.8)
            self.assertLess(stats['5xx'] / total, 0.1)

        self._test_on_both_ports(_test)

    # 9. Estabilidade (testa ambas)
    def test_stability_short(self):
        if not psutil:
            self.skipTest("psutil não instalado")

        headers = {'Authorization': f'Bearer {DMMR_API_KEY}'}

        def _test(port):
            url = f'http://127.0.0.1:{port}/api/v1'
            stop_event = threading.Event()

            def load_gen():
                while not stop_event.is_set():
                    try:
                        requests.get(url, headers=headers, timeout=1)
                    except Exception:
                        pass
                    time.sleep(0.01)

            t = threading.Thread(target=load_gen)
            t.start()

            cache_proc = psutil.Process(self.cache_proc.pid)
            mem_samples = []
            for _ in range(6):
                time.sleep(10)
                mem_samples.append(cache_proc.memory_info().rss)

            stop_event.set()
            t.join()

            if len(mem_samples) > 1:
                start, end = mem_samples[0], mem_samples[-1]
                growth = (end - start) / start if start else 0
                self.assertLess(growth, 0.20)

        # Executa para ambas as portas (pode demorar 2 minutos, mas é aceitável)
        for port in (NGINX_TCP_PORT, NGINX_UNIX_PORT):
            with self.subTest(port=port):
                _test(port)

    # 10. Recuperação (testa ambas)
    def test_recovery_cache_restart(self):
        headers = {'Authorization': f'Bearer {DMMR_API_KEY}'}

        def _test(port):
            stop_event = threading.Event()
            errors = []

            def loader():
                while not stop_event.is_set():
                    try:
                        requests.get(f'http://127.0.0.1:{port}/api/v1',
                                     headers=headers, timeout=1)
                    except Exception as e:
                        errors.append(str(e))
                    time.sleep(0.1)

            t = threading.Thread(target=loader)
            t.start()
            time.sleep(2)
            self._stop_cache()
            time.sleep(1)
            self._start_cache(['--unix', '--tcp'])
            time.sleep(2)
            stop_event.set()
            t.join()
            self.assertLess(len(errors), 20)
            resp = requests.get(f'http://127.0.0.1:{port}/api/v1', headers=headers)
            self.assertNotIn(resp.status_code, (500, 502, 503))

        self._test_on_both_ports(_test)

    def test_recovery_socket_removal(self):
        headers = {'Authorization': f'Bearer {DMMR_API_KEY}'}

        def _test(port):
            if os.path.exists(CACHE_SOCK):
                os.unlink(CACHE_SOCK)
            time.sleep(2)
            self._stop_cache()
            self._start_cache(['--unix', '--tcp'])
            time.sleep(2)
            resp = requests.get(f'http://127.0.0.1:{port}/api/v1', headers=headers)
            self.assertNotIn(resp.status_code, (500, 502, 503))

        self._test_on_both_ports(_test)

    # 11. Segurança (testa cache diretamente, não Nginx)
    def test_security_malformed_payloads(self):
        with CacheClient(use_unix=True) as client:
            # key_len gigante
            frame = struct.pack('!HHHHIIQ',
                                DMMR_MAGIC, DMMR_VERSION, OP_GET, FLAG_NONE,
                                0xFFFFFFFF, 0, 0)
            client.sock.sendall(frame)
            time.sleep(0.5)
            status, _ = client.send_frame(OP_GET, b'key')
            self.assertEqual(status, 1)
        # bytes aleatórios
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(CACHE_SOCK)
            s.sendall(os.urandom(1024))
            s.close()
            time.sleep(0.5)
        with CacheClient(use_unix=True) as client:
            status, _ = client.send_frame(OP_GET, b'key')
            self.assertEqual(status, 1)

    # 12. Suite existente (README)
    def test_cache_suite(self):
        with CacheClient(use_unix=True) as client:
            client.send_frame(OP_SET, b'test_key', b'test_value')
            status, payload = client.send_frame(OP_GET, b'test_key')
            self.assertEqual(status, 0)
            self.assertEqual(payload, b'test_value')
            client.send_frame(OP_DEL, b'test_key')
            status, _ = client.send_frame(OP_GET, b'test_key')
            self.assertEqual(status, 1)
            client.send_frame(OP_SET, b'legacy', b'data')
            status, payload = client.send_legacy_get(b'legacy')
            self.assertEqual(status, 0)
            self.assertEqual(payload, b'data')


# ----------------------------------------------------------------------
# Execução
# ----------------------------------------------------------------------
if __name__ == '__main__':
    if not os.path.exists(CACHE_BIN):
        print(f"AVISO: {CACHE_BIN} não encontrado.", file=sys.stderr)
    unittest.main(argv=sys.argv + ['-v'])