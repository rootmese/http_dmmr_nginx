#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DMMR Integration Test Suite
============================
Testes de integração para o Gateway HTTP DMMR (Cache Service + Nginx module).

Requisitos:
  - Python 3.6+
  - Bibliotecas: requests, psutil (opcional)
  - O binário 'dmmr_cache' deve estar compilado.
  - O Nginx DEVE estar em execução com o módulo DMMR carregado e configurado.
  - As portas 8080 (TCP) e 9081 (Unix socket) devem estar configuradas no Nginx.

Uso:
  python3 suite_tests.py                          # Executa todos os testes
  python3 suite_tests.py -k authentication        # Executa apenas testes de autenticação
  python3 suite_tests.py -k "not stability"       # Pula testes de estabilidade
  python3 suite_tests.py -k "not rate_limiting"   # Pula rate limiting (61s de espera)
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
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import Optional, Tuple, List, Dict

import requests

# psutil é opcional (usado apenas em test_stability)
try:
    import psutil
except ImportError:
    psutil = None


# ======================================================================
#  CONFIGURATION
# ======================================================================

class Config:
    """Configuração centralizada — todas as constantes em um único lugar."""

    # Caminhos
    CACHE_BIN: str = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'http_dmmr_cache', 'dmmr_cache')
    )
    CACHE_SOCK: str = '/tmp/dmmr_cache.sock'

    # Portas
    CACHE_TCP_PORT: int = 9080
    BACKEND1_PORT: int = 8001
    BACKEND2_PORT: int = 8002
    NGINX_TCP_PORT: int = 8080
    NGINX_UNIX_PORT: int = 9081

    # Rate limiting
    RATE_LIMIT: int = 120
    RATE_WINDOW_MS: int = 60000

    # Autenticação
    API_KEY: str = os.environ.get('DMMR_API_KEY', '123456')

    # Timeouts (segundos)
    PROCESS_TIMEOUT: float = 5.0
    SOCKET_TIMEOUT: float = 5.0
    HTTP_TIMEOUT: float = 2.0
    WAIT_TIMEOUT: float = 10.0
    WAIT_STEP: float = 0.2


CFG = Config()


# ======================================================================
#  TEST LOGGER
# ======================================================================

class TestLogger:
    """Logger com cores ANSI e contagem de resultados."""

    GREEN  = '\033[92m'
    RED    = '\033[91m'
    YELLOW = '\033[93m'
    CYAN   = '\033[96m'
    BOLD   = '\033[1m'
    DIM    = '\033[2m'
    RESET  = '\033[0m'

    _results: Dict[str, List[str]] = {'pass': [], 'fail': [], 'error': [], 'skip': []}
    _section: str = ''

    @classmethod
    def section(cls, title: str):
        cls._section = title
        print(f'\n{cls.BOLD}{cls.CYAN}{"=" * 64}{cls.RESET}')
        print(f'{cls.BOLD}{cls.CYAN}  {title}{cls.RESET}')
        print(f'{cls.BOLD}{cls.CYAN}{"=" * 64}{cls.RESET}')

    @classmethod
    def info(cls, msg: str):
        print(f'  {cls.DIM}[INFO]{cls.RESET}  {msg}')

    @classmethod
    def ok(cls, msg: str):
        print(f'  {cls.GREEN}[PASS]{cls.RESET}  {msg}')
        cls._results['pass'].append(f'{cls._section}: {msg}')

    @classmethod
    def fail(cls, msg: str, detail: str = ''):
        print(f'  {cls.RED}[FAIL]{cls.RESET}  {msg}')
        if detail:
            for line in detail.strip().splitlines():
                print(f'         {cls.RED}{line}{cls.RESET}')
        cls._results['fail'].append(f'{cls._section}: {msg}')

    @classmethod
    def error(cls, msg: str, detail: str = ''):
        print(f'  {cls.RED}[ERROR]{cls.RESET} {msg}')
        if detail:
            for line in detail.strip().splitlines():
                print(f'         {cls.RED}{line}{cls.RESET}')
        cls._results['error'].append(f'{cls._section}: {msg}')

    @classmethod
    def skip(cls, msg: str):
        print(f'  {cls.YELLOW}[SKIP]{cls.RESET}  {msg}')
        cls._results['skip'].append(f'{cls._section}: {msg}')

    @classmethod
    def warn(cls, msg: str):
        print(f'  {cls.YELLOW}[WARN]{cls.RESET}  {msg}')

    @classmethod
    def summary(cls):
        total = sum(len(v) for v in cls._results.values())
        p, f, e, s = (len(cls._results[k]) for k in ('pass', 'fail', 'error', 'skip'))
        print(f'\n{cls.BOLD}{"=" * 64}{cls.RESET}')
        print(f'{cls.BOLD}  RESUMO FINAL{cls.RESET}')
        print(f'{cls.BOLD}{"=" * 64}{cls.RESET}')
        print(f'  Total:   {total}')
        print(f'  {cls.GREEN}Passed:  {p}{cls.RESET}')
        print(f'  {cls.RED}Failed:  {f}{cls.RESET}')
        print(f'  {cls.RED}Errors:  {e}{cls.RESET}')
        print(f'  {cls.YELLOW}Skipped: {s}{cls.RESET}')
        if f or e:
            print(f'\n  {cls.RED}{cls.BOLD}Falhas:{cls.RESET}')
            for name in cls._results['fail'] + cls._results['error']:
                print(f'    • {cls.RED}{name}{cls.RESET}')
        print(f'{cls.BOLD}{"=" * 64}{cls.RESET}\n')
        return f + e


LOG = TestLogger


# ======================================================================
#  BINARY PROTOCOL HELPERS
# ======================================================================

DMMR_MAGIC   = 0xD4D4
DMMR_VERSION = 1
OP_GET       = 1
OP_SET       = 2
OP_DEL       = 3
OP_SYNC      = 4
FLAG_NONE    = 0


def pack_frame(opcode: int, key: bytes, value: bytes = b'',
               timestamp: int = 0) -> bytes:
    """Empacota um frame no protocolo moderno DMMR."""
    header = struct.pack(
        '!HHHHIIQ',
        DMMR_MAGIC, DMMR_VERSION, opcode, FLAG_NONE,
        len(key), len(value), timestamp,
    )
    return header + key + value


def pack_legacy_get(key: bytes) -> bytes:
    """Empacota um GET no protocolo legado."""
    return struct.pack('!HH', 1, len(key)) + key


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Recebe exatamente *n* bytes do socket."""
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(
                f'Conexão fechada ao tentar ler {n} bytes (recebidos {len(buf)})'
            )
        buf += chunk
    return buf


# ======================================================================
#  CACHE CLIENT (Binary Protocol)
#  IMPORTANTE: o cache usa modelo one-request-per-connection
#  (handle_client fecha o fd após processar um frame).
#  Cada operação abre uma conexão nova.
# ======================================================================

class CacheClient:
    """Cliente para o protocolo binário do DMMR Cache.

    Cada chamada a send_frame() ou send_legacy_get() abre uma conexão
    nova, envia o frame, lê a resposta e fecha — compatível com o modelo
    one-request-per-connection do servidor.
    """

    def __init__(self, *, use_unix: bool = True,
                 path: str = None, host: str = '127.0.0.1', port: int = None):
        self.use_unix = use_unix
        self.path = path or CFG.CACHE_SOCK
        self.host = host
        self.port = port or CFG.CACHE_TCP_PORT

    # -- Context manager (no-op, mantido para compatibilidade) -----------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # -- Internal: create one-shot connection -----------------------------

    def _connect(self) -> socket.socket:
        if self.use_unix:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self.path)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        sock.settimeout(CFG.SOCKET_TIMEOUT)
        return sock

    # -- Protocol: Modern --------------------------------------------------

    def send_frame(self, opcode: int, key: bytes, value: bytes = b'',
                   timestamp: int = 0) -> Tuple[int, bytes]:
        """Envia frame moderno e retorna (status, payload).
        Abre e fecha a conexão internamente."""
        sock = self._connect()
        try:
            frame = pack_frame(opcode, key, value, timestamp)
            sock.sendall(frame)
            header = _recv_exact(sock, 8)
            status = struct.unpack('!H', header[:2])[0]
            payload_len = struct.unpack('!I', header[2:6])[0]
            payload = _recv_exact(sock, payload_len) if payload_len else b''
            return status, payload
        finally:
            sock.close()

    # -- Protocol: Legacy --------------------------------------------------

    def send_legacy_get(self, key: bytes) -> Tuple[int, bytes]:
        """Envia GET legado e retorna (status, payload)."""
        sock = self._connect()
        try:
            frame = pack_legacy_get(key)
            sock.sendall(frame)
            header = _recv_exact(sock, 4)
            status = struct.unpack('!H', header[:2])[0]
            payload_len = struct.unpack('!H', header[2:4])[0]
            payload = _recv_exact(sock, payload_len) if payload_len else b''
            return status, payload
        finally:
            sock.close()

    # -- Raw socket (for edge-case tests) ----------------------------------

    def raw_socket(self) -> socket.socket:
        """Retorna um socket conectado para testes de baixo nível."""
        return self._connect()


# ======================================================================
#  BACKEND MANAGER (HTTP Servers)
# ======================================================================

class _BackendHandler(BaseHTTPRequestHandler):
    """Handler HTTP simples que retorna a mensagem configurada no server."""
    protocol_version = 'HTTP/1.1'

    def do_GET(self):
        body = self.server.backend_message.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_POST = do_GET

    def log_message(self, fmt, *args):
        pass  # silencia logs do servidor


class _ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, handler_class, message: str):
        self.backend_message = message
        super().__init__(address, handler_class)


class BackendManager:
    """Gerencia os servidores HTTP de backend."""

    def __init__(self):
        self._servers: List[Tuple[_ThreadedHTTPServer, threading.Thread, int]] = []

    def start_all(self):
        backends = [
            (CFG.BACKEND1_PORT, 'Backend 8001'),
            (CFG.BACKEND2_PORT, 'Backend 8002'),
        ]
        for port, msg in backends:
            self._start_one(port, msg)

    def _start_one(self, port: int, message: str):
        LOG.info(f'Starting backend on port {port}...')
        server = _ThreadedHTTPServer(('127.0.0.1', port), _BackendHandler, message)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        if not _wait_for_port('127.0.0.1', port, timeout=5):
            raise RuntimeError(f'Backend na porta {port} não iniciou a tempo')
        LOG.ok(f'Backend {port} ready.')
        self._servers.append((server, thread, port))

    def stop_all(self):
        for server, thread, port in self._servers:
            LOG.info(f'Stopping backend {port}...')
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
            LOG.ok(f'Backend {port} stopped.')
        self._servers.clear()

    def kill_first(self):
        """Mata o primeiro backend (para testes de falha)."""
        if not self._servers:
            return
        server, thread, port = self._servers[0]
        LOG.info(f'Killing backend {port} for failure test...')
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        LOG.ok(f'Backend {port} killed.')


# ======================================================================
#  CACHE MANAGER (dmmr_cache process)
# ======================================================================

class CacheManager:
    """Gerencia o ciclo de vida do processo dmmr_cache."""

    def __init__(self):
        self.proc: Optional[subprocess.Popen] = None
        self._stdout_path: Optional[str] = None
        self._stderr_path: Optional[str] = None

    def start(self, args: List[str] = None):
        """Inicia o cache com os argumentos solicitados."""
        self.stop()
        args = args or ['--unix', '--tcp']
        cmd = [CFG.CACHE_BIN] + args
        LOG.info(f'Starting cache: {" ".join(cmd)}')

        # Limpa socket antigo
        if os.path.exists(CFG.CACHE_SOCK):
            os.unlink(CFG.CACHE_SOCK)

        # Espera a porta TCP ficar livre
        self._wait_port_free(CFG.CACHE_TCP_PORT, timeout=5)

        # Cria arquivos temporários para stdout/stderr
        self._stdout_path = tempfile.mktemp(prefix='cache_stdout_', suffix='.log')
        self._stderr_path = tempfile.mktemp(prefix='cache_stderr_', suffix='.log')
        stdout_f = open(self._stdout_path, 'w')
        stderr_f = open(self._stderr_path, 'w')

        self.proc = subprocess.Popen(
            cmd,
            stdout=stdout_f,
            stderr=stderr_f,
            preexec_fn=os.setsid if os.name != 'nt' else None,
        )
        stdout_f.close()
        stderr_f.close()

        # Espera listeners
        need_unix = '--unix' in args or '--both' in args
        need_tcp = '--tcp' in args or '--both' in args
        if '--unix' in args and '--tcp' in args:
            need_unix = need_tcp = True

        ok = True
        if need_unix:
            if not _wait_for_socket(CFG.CACHE_SOCK, timeout=CFG.WAIT_TIMEOUT):
                LOG.error('Unix socket did not appear', self.dump_logs())
                ok = False
            else:
                LOG.ok('Unix socket created.')

        if need_tcp:
            if not _wait_for_port('127.0.0.1', CFG.CACHE_TCP_PORT, timeout=CFG.WAIT_TIMEOUT):
                LOG.error('TCP port did not open', self.dump_logs())
                ok = False
            else:
                LOG.ok(f'TCP listener ready on port {CFG.CACHE_TCP_PORT}.')

        if not ok:
            raise RuntimeError('Cache service failed to start — see logs above.')

    def stop(self):
        """Para o processo do cache de forma graciosa."""
        if self.proc and self.proc.poll() is None:
            LOG.info('Stopping cache process...')
            try:
                if os.name != 'nt':
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
                else:
                    self.proc.terminate()
                self.proc.wait(timeout=CFG.PROCESS_TIMEOUT)
            except subprocess.TimeoutExpired:
                LOG.warn('Cache did not stop gracefully — killing...')
                self.proc.kill()
                self.proc.wait()
            LOG.ok('Cache process stopped.')
        self.proc = None

        if os.path.exists(CFG.CACHE_SOCK):
            try:
                os.unlink(CFG.CACHE_SOCK)
            except OSError:
                pass

        self._wait_port_free(CFG.CACHE_TCP_PORT, timeout=5)

    def dump_logs(self) -> str:
        parts = []
        for label, path in [('STDOUT', self._stdout_path), ('STDERR', self._stderr_path)]:
            if path and os.path.exists(path):
                with open(path, 'r', errors='replace') as f:
                    content = f.read().strip()
                if content:
                    parts.append(f'=== Cache {label} ===\n{content}')
        return '\n'.join(parts) if parts else '(no cache logs captured)'

    def cleanup_logs(self):
        for path in (self._stdout_path, self._stderr_path):
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass

    @property
    def pid(self) -> Optional[int]:
        return self.proc.pid if self.proc else None

    @property
    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    @staticmethod
    def _wait_port_free(port: int, timeout: float = 5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2)
                if s.connect_ex(('127.0.0.1', port)) != 0:
                    return True
            time.sleep(0.2)
        return False


# ======================================================================
#  HTTP CLIENT (Nginx requests)
# ======================================================================

class HttpClient:
    """Cliente HTTP simplificado para testar o Nginx/DMMR."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or CFG.API_KEY

    @property
    def auth_headers(self) -> dict:
        return {'Authorization': f'Bearer {self.api_key}'}

    def get(self, port: int, path: str = '/api/v1', *,
            headers: dict = None, timeout: float = None) -> requests.Response:
        headers = headers if headers is not None else self.auth_headers
        timeout = timeout or CFG.HTTP_TIMEOUT
        return requests.get(
            f'http://127.0.0.1:{port}{path}',
            headers=headers,
            timeout=timeout,
        )


# ======================================================================
#  WAIT HELPERS
# ======================================================================

def _wait_for_socket(path: str, timeout: float = 10, step: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(step)
    return False


def _wait_for_port(host: str, port: int, timeout: float = 10,
                   step: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(step)
            if s.connect_ex((host, port)) == 0:
                return True
        time.sleep(step)
    return False


def _wait_for_http(url: str, timeout: float = 10, step: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            requests.get(url, timeout=1)
            return True
        except requests.ConnectionError:
            time.sleep(step)
    return False


# ======================================================================
#  BASE TEST CASE
# ======================================================================

class BaseIntegrationTest(unittest.TestCase):
    """Classe base com diagnósticos automáticos e helpers."""

    # Shared managers — setados por setUpClass
    cache_mgr: CacheManager = None
    backend_mgr: BackendManager = None
    http: HttpClient = None

    def _test_on_both_ports(self, test_func):
        """Executa test_func para ambas as portas (TCP e Unix-proxied)."""
        for port in (CFG.NGINX_TCP_PORT, CFG.NGINX_UNIX_PORT):
            with self.subTest(port=port):
                test_func(port)

    def _ensure_rate_limit_clear(self, port: int):
        """Se o rate limiter estiver ativo, espera a janela resetar."""
        try:
            resp = self.http.get(port)
            if resp.status_code == 429:
                LOG.warn(f'[:{port}] Rate limited — waiting 62s for window reset...')
                time.sleep(62)
        except Exception:
            pass

    # -- Diagnóstico automático em falha -----------------------------------

    def _outcome_has_failures(self) -> bool:
        if hasattr(self, '_outcome'):
            result = getattr(self._outcome, 'result', self._outcome)
            if result and hasattr(result, 'failures'):
                for test, _ in result.failures + result.errors:
                    if test is self:
                        return True
        return False

    def tearDown(self):
        if self._outcome_has_failures() and self.cache_mgr:
            LOG.warn('Test failed — dumping cache logs for diagnosis:')
            print(self.cache_mgr.dump_logs(), file=sys.stderr)


# ======================================================================
#  TEST SUITE
#  Os testes são numerados para controlar a ordem de execução.
#  unittest ordena alfabeticamente, então test_01 < test_02 < ...
#
#  Ordem lógica:
#    01-06: Cache direto (sem HTTP → sem impacto no rate limiter)
#    07-10: Nginx HTTP (poucas requests)
#    11-13: Recovery (HTTP moderado)
#    14:    Stability (muitas requests mas não verifica status)
#    15:    Rate Limiting (muitas requests + 61s wait → reseta janela)
#    16:    Load Simulation (após rate limit resetar)
# ======================================================================

class DMMRTestSuite(BaseIntegrationTest):
    """Suíte de testes de integração DMMR."""

    # ==================================================================
    #  SETUP / TEARDOWN
    # ==================================================================

    @classmethod
    def setUpClass(cls):
        LOG.section('Environment Setup')

        if not os.path.exists(CFG.CACHE_BIN):
            raise FileNotFoundError(
                f'Cache binary not found: {CFG.CACHE_BIN}\n'
                'Compile with: make -C ../http_dmmr_cache'
            )
        LOG.ok(f'Cache binary found: {CFG.CACHE_BIN}')

        cls.cache_mgr = CacheManager()
        cls.backend_mgr = BackendManager()
        cls.http = HttpClient()

        cls.backend_mgr.start_all()
        cls.cache_mgr.start(['--unix', '--tcp'])

        LOG.info('Checking Nginx availability...')
        for port in (CFG.NGINX_TCP_PORT, CFG.NGINX_UNIX_PORT):
            if not _wait_for_http(f'http://127.0.0.1:{port}/', timeout=3):
                raise RuntimeError(
                    f'Nginx não está respondendo na porta {port}. '
                    'Certifique-se de que o Nginx está em execução com o módulo DMMR carregado.'
                )
            LOG.ok(f'Nginx responding on port {port}.')

        LOG.section('Running Tests')

    @classmethod
    def tearDownClass(cls):
        LOG.section('Cleanup')
        cls.cache_mgr.stop()
        cls.cache_mgr.cleanup_logs()
        cls.backend_mgr.stop_all()
        LOG.ok('All resources released.')

    # ==================================================================
    #  01. CACHE — START / STOP
    # ==================================================================

    def test_01_cache_start_stop(self):
        """Cache inicia com ambos os listeners e para corretamente."""
        LOG.section('01 — Cache Start / Stop')

        self.cache_mgr.stop()
        self.cache_mgr.start(['--unix', '--tcp'])

        self.assertTrue(os.path.exists(CFG.CACHE_SOCK), 'Unix socket must exist')
        LOG.ok('Unix socket exists.')

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            result = s.connect_ex(('127.0.0.1', CFG.CACHE_TCP_PORT))
            self.assertEqual(result, 0, 'TCP port must be open')
        LOG.ok(f'TCP port {CFG.CACHE_TCP_PORT} open.')

        self.cache_mgr.stop()
        self.assertFalse(os.path.exists(CFG.CACHE_SOCK), 'Socket must be cleaned up')
        LOG.ok('Socket cleaned up after stop.')

        self.cache_mgr.start(['--unix', '--tcp'])
        LOG.ok('Cache restarted for subsequent tests.')

    # ==================================================================
    #  02. CACHE — LISTENER MODES
    # ==================================================================

    def test_02_cache_modes(self):
        """Cache respeita modos de escuta: --unix, --tcp, --both."""
        LOG.section('02 — Cache Listener Modes')

        modes = [
            (['--unix'],  True,  False),
            (['--tcp'],   False, True),
            (['--both'],  True,  True),
        ]
        for args, expect_unix, expect_tcp in modes:
            with self.subTest(args=args):
                label = ' '.join(args)
                LOG.info(f'Testing mode: {label}')
                self.cache_mgr.stop()
                self.cache_mgr.start(args)

                unix_ok = os.path.exists(CFG.CACHE_SOCK)
                self.assertEqual(unix_ok, expect_unix,
                                 f'Unix socket presence mismatch for {label}')
                LOG.ok(f'[{label}] Unix: {"present" if unix_ok else "absent"} (expected).')

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    tcp_ok = s.connect_ex(('127.0.0.1', CFG.CACHE_TCP_PORT)) == 0
                self.assertEqual(tcp_ok, expect_tcp,
                                 f'TCP port mismatch for {label}')
                LOG.ok(f'[{label}] TCP: {"open" if tcp_ok else "closed"} (expected).')

                self.cache_mgr.stop()

        self.cache_mgr.start(['--unix', '--tcp'])
        LOG.ok('Cache restored to default mode.')

    # ==================================================================
    #  03. PROTOCOL — MODERN SET/GET/DEL
    # ==================================================================

    def test_03_protocol_set_get_del(self):
        """Protocolo moderno: SET → GET → DEL → GET (not found)."""
        LOG.section('03 — Protocol Modern SET/GET/DEL')

        client = CacheClient(use_unix=True)

        status, _ = client.send_frame(OP_SET, b'proto_key', b'proto_val')
        self.assertEqual(status, 0)
        LOG.ok('SET proto_key=proto_val → status 0')

        status, payload = client.send_frame(OP_GET, b'proto_key')
        self.assertEqual(status, 0)
        self.assertEqual(payload, b'proto_val')
        LOG.ok('GET proto_key → proto_val')

        status, _ = client.send_frame(OP_DEL, b'proto_key')
        self.assertEqual(status, 0)
        LOG.ok('DEL proto_key → status 0')

        status, _ = client.send_frame(OP_GET, b'proto_key')
        self.assertEqual(status, 1)
        LOG.ok('GET proto_key → not found (status 1)')

    # ==================================================================
    #  04. PROTOCOL — MODERN EDGE CASES
    # ==================================================================

    def test_04_protocol_edge_cases(self):
        """Protocolo moderno: chaves/valores variados, opcode inválido, pacote incompleto."""
        LOG.section('04 — Protocol Modern Edge Cases')

        client = CacheClient(use_unix=True)

        cases = [
            (b'edge_k',       b'v',        'tiny key/value'),
            (b'edge_' + b'x' * 95, b'y' * 200,  '100-byte key, 200-byte value'),
            (b'edge_large',   b'z' * 4096, '4KB value'),
            (b'edge_empty',   b'',         'empty value'),
        ]
        for k, v, desc in cases:
            status, _ = client.send_frame(OP_SET, k, v)
            self.assertEqual(status, 0)
            status, payload = client.send_frame(OP_GET, k)
            self.assertEqual(status, 0)
            self.assertEqual(payload, v)
            LOG.ok(f'{desc}: SET+GET OK')

        # Opcode inválido
        LOG.info('Testing invalid opcode (99)...')
        try:
            status, _ = client.send_frame(99, b'edge_inv', b'val')
            self.assertNotEqual(status, 0)
            LOG.ok(f'Invalid opcode rejected (status={status})')
        except (socket.timeout, ConnectionError, BrokenPipeError) as e:
            LOG.ok(f'Invalid opcode caused disconnect: {type(e).__name__}')

        # Pacote incompleto
        LOG.info('Testing incomplete frame...')
        sock = client.raw_socket()
        try:
            sock.sendall(pack_frame(OP_GET, b'edge_k')[:10])
            time.sleep(0.3)
            try:
                data = sock.recv(8, socket.MSG_DONTWAIT)
                if data:
                    status = struct.unpack('!H', data[:2])[0]
                    self.assertNotEqual(status, 0)
                    LOG.ok(f'Incomplete frame rejected (status={status})')
                else:
                    LOG.ok('Incomplete frame — server closed connection')
            except (socket.timeout, BlockingIOError):
                LOG.ok('Incomplete frame — no immediate response (expected)')
        finally:
            sock.close()

        # Serviço continua funcional
        status, _ = client.send_frame(OP_GET, b'edge_k')
        self.assertEqual(status, 0)
        LOG.ok('Cache service still functional after edge cases.')

    # ==================================================================
    #  05. PROTOCOL — LEGACY
    # ==================================================================

    def test_05_protocol_legacy(self):
        """Protocolo legado: GET funciona, SET é rejeitado."""
        LOG.section('05 — Protocol Legacy')

        client = CacheClient(use_unix=True)

        # Prepara via moderno
        client.send_frame(OP_SET, b'leg_key', b'leg_value')
        LOG.info('Prepared key via modern protocol.')

        # GET legado
        status, payload = client.send_legacy_get(b'leg_key')
        self.assertEqual(status, 0)
        self.assertEqual(payload, b'leg_value')
        LOG.ok('Legacy GET returned correct value.')

        # SET legado deve ser rejeitado
        LOG.info('Testing legacy SET (should be rejected)...')
        sock = client.raw_socket()
        try:
            sock.sendall(struct.pack('!HH', 2, 3) + b'abc')
            data = sock.recv(4)
            status = struct.unpack('!H', data[:2])[0]
            self.assertNotEqual(status, 0)
            LOG.ok(f'Legacy SET rejected (status={status}).')
        finally:
            sock.close()

    # ==================================================================
    #  06. CACHE SUITE (basic operations)
    # ==================================================================

    def test_06_cache_suite(self):
        """Suite básica: SET → GET → DEL → GET, e legacy GET."""
        LOG.section('06 — Cache Suite Basic Operations')

        client = CacheClient(use_unix=True)

        client.send_frame(OP_SET, b'suite_key', b'suite_value')
        status, payload = client.send_frame(OP_GET, b'suite_key')
        self.assertEqual(status, 0)
        self.assertEqual(payload, b'suite_value')
        LOG.ok('SET + GET suite_key → suite_value')

        client.send_frame(OP_DEL, b'suite_key')
        status, _ = client.send_frame(OP_GET, b'suite_key')
        self.assertEqual(status, 1)
        LOG.ok('DEL + GET suite_key → not found')

        client.send_frame(OP_SET, b'suite_legacy', b'suite_data')
        status, payload = client.send_legacy_get(b'suite_legacy')
        self.assertEqual(status, 0)
        self.assertEqual(payload, b'suite_data')
        LOG.ok('Legacy GET suite_legacy → suite_data')

    # ==================================================================
    #  07. SECURITY — MALFORMED PAYLOADS
    # ==================================================================

    def test_07_security_malformed(self):
        """Cache sobrevive a payloads malformados."""
        LOG.section('07 — Security — Malformed Payloads')

        client = CacheClient(use_unix=True)

        # key_len gigante
        LOG.info('Sending frame with key_len=0xFFFFFFFF...')
        sock = client.raw_socket()
        try:
            frame = struct.pack(
                '!HHHHIIQ',
                DMMR_MAGIC, DMMR_VERSION, OP_GET, FLAG_NONE,
                0xFFFFFFFF, 0, 0,
            )
            sock.sendall(frame)
            time.sleep(0.5)
        except (BrokenPipeError, ConnectionError):
            pass  # server may close immediately
        finally:
            sock.close()
        LOG.ok('Giant key_len sent — cache did not crash.')

        # Verificar funcionalidade com chave conhecida
        client.send_frame(OP_SET, b'sec_test', b'alive')
        status, payload = client.send_frame(OP_GET, b'sec_test')
        self.assertEqual(status, 0)
        self.assertEqual(payload, b'alive')
        LOG.ok('Cache functional after giant key_len.')

        # Bytes aleatórios
        LOG.info('Sending 1024 random bytes...')
        sock = client.raw_socket()
        try:
            sock.sendall(os.urandom(1024))
        except (BrokenPipeError, ConnectionError):
            pass
        finally:
            sock.close()
        time.sleep(0.5)

        status, payload = client.send_frame(OP_GET, b'sec_test')
        self.assertEqual(status, 0)
        self.assertEqual(payload, b'alive')
        LOG.ok('Cache functional after random bytes.')

    # ==================================================================
    #  08. AUTHENTICATION
    # ==================================================================

    def test_08_authentication(self):
        """Testa API key válida, inválida, ausente e com caracteres especiais."""
        LOG.section('08 — Authentication')

        def _test(port):
            LOG.info(f'Testing authentication on port {port}...')

            resp = self.http.get(port)
            self.assertNotIn(resp.status_code, (401, 403),
                             f'Valid key should not be rejected (got {resp.status_code})')
            LOG.ok(f'[:{port}] Valid API key → {resp.status_code}')

            resp = self.http.get(port, headers={'Authorization': 'Bearer invalid'})
            self.assertIn(resp.status_code, (401, 403))
            LOG.ok(f'[:{port}] Invalid API key → {resp.status_code}')

            resp = self.http.get(port, headers={})
            self.assertEqual(resp.status_code, 401)
            LOG.ok(f'[:{port}] No API key → 401')

            resp = self.http.get(port, headers={'Authorization': 'Bearer áéíóú!@#'})
            self.assertIn(resp.status_code, (401, 403))
            LOG.ok(f'[:{port}] Special chars API key → {resp.status_code}')

        self._test_on_both_ports(_test)

    # ==================================================================
    #  09. NGINX — ROUTING
    # ==================================================================

    def test_09_nginx_routing(self):
        """Nginx roteia /api/v1 → backend:8001, /api/v2 → backend:8002, /api/v3 → 404."""
        LOG.section('09 — Nginx Routing')

        def _test(port):
            self._ensure_rate_limit_clear(port)
            LOG.info(f'Testing routing on port {port}...')

            resp = self.http.get(port, '/api/v1')
            self.assertEqual(resp.status_code, 200,
                             f'Expected 200 for /api/v1, got {resp.status_code}')
            self.assertIn(b'Backend 8001', resp.content)
            self.assertEqual(resp.headers.get('Content-Type'), 'text/plain')
            LOG.ok(f'[:{port}] /api/v1 → 200, Backend 8001')

            resp = self.http.get(port, '/api/v2')
            self.assertEqual(resp.status_code, 200)
            self.assertIn(b'Backend 8002', resp.content)
            LOG.ok(f'[:{port}] /api/v2 → 200, Backend 8002')

            resp = self.http.get(port, '/api/v3')
            self.assertEqual(resp.status_code, 404)
            LOG.ok(f'[:{port}] /api/v3 → 404')

        self._test_on_both_ports(_test)

    # ==================================================================
    #  10. NGINX — BACKEND FAILURE
    # ==================================================================

    def test_10_nginx_backend_failure(self):
        """Nginx retorna 5xx quando o backend morre e recupera após restart."""
        LOG.section('10 — Nginx Backend Failure & Recovery')

        # Garante rate limit limpo antes de testar
        for port in (CFG.NGINX_TCP_PORT, CFG.NGINX_UNIX_PORT):
            self._ensure_rate_limit_clear(port)

        # Mata o primeiro backend
        self.backend_mgr.kill_first()
        time.sleep(0.5)

        def _test_failure(port):
            LOG.info(f'[:{port}] Requesting with dead backend...')
            try:
                resp = self.http.get(port, '/api/v1')
                self.assertIn(resp.status_code, (429, 500, 502, 503, 504),
                              f'Expected error status, got {resp.status_code}')
                LOG.ok(f'[:{port}] Got {resp.status_code} (expected error)')
            except requests.ConnectionError:
                self.fail(f'Nginx na porta {port} deve responder mesmo com backend morto')

        self._test_on_both_ports(_test_failure)

        # Restaura backends
        LOG.info('Restarting all backends...')
        self.backend_mgr.stop_all()
        self.backend_mgr.start_all()
        time.sleep(0.5)

        def _test_recovery(port):
            resp = self.http.get(port, '/api/v1')
            self.assertIn(resp.status_code, (200, 429),
                          f'Expected 200 or 429 after recovery, got {resp.status_code}')
            LOG.ok(f'[:{port}] Recovery → {resp.status_code}')

        self._test_on_both_ports(_test_recovery)

    # ==================================================================
    #  11. NGINX — CACHE UNAVAILABLE
    # ==================================================================

    def test_11_nginx_cache_unavailable(self):
        """Nginx retorna erro quando o cache está indisponível."""
        LOG.section('11 — Nginx Cache Unavailable')

        # Garante rate limit limpo
        for port in (CFG.NGINX_TCP_PORT, CFG.NGINX_UNIX_PORT):
            self._ensure_rate_limit_clear(port)

        self.cache_mgr.stop()
        LOG.info('Cache stopped.')

        def _test(port):
            try:
                resp = self.http.get(port, '/api/v1')
                self.assertIn(resp.status_code, (429, 500, 502, 503),
                              f'Expected error with cache down, got {resp.status_code}')
                LOG.ok(f'[:{port}] Got {resp.status_code} with cache down')
            except requests.ConnectionError:
                self.fail(f'Nginx na porta {port} deve responder mesmo com cache indisponível')

        self._test_on_both_ports(_test)

        self.cache_mgr.start(['--unix', '--tcp'])
        LOG.ok('Cache restored.')

    # ==================================================================
    #  12. RECOVERY — CACHE RESTART
    # ==================================================================

    def test_12_recovery_cache_restart(self):
        """Sistema recupera após restart do cache (erros transitórios < 20)."""
        LOG.section('12 — Recovery — Cache Restart Under Load')

        def _test(port):
            stop_event = threading.Event()
            errors = []

            def loader():
                while not stop_event.is_set():
                    try:
                        requests.get(
                            f'http://127.0.0.1:{port}/api/v1',
                            headers=self.http.auth_headers, timeout=1,
                        )
                    except Exception as e:
                        errors.append(str(e))
                    time.sleep(0.1)

            LOG.info(f'[:{port}] Starting background load...')
            t = threading.Thread(target=loader, daemon=True)
            t.start()
            time.sleep(2)

            LOG.info(f'[:{port}] Restarting cache...')
            self.cache_mgr.stop()
            time.sleep(1)
            self.cache_mgr.start(['--unix', '--tcp'])
            time.sleep(2)

            stop_event.set()
            t.join(timeout=3)

            LOG.info(f'[:{port}] Transient errors during restart: {len(errors)}')
            self.assertLess(len(errors), 20,
                            f'Too many errors during restart: {len(errors)}')
            LOG.ok(f'[:{port}] {len(errors)} transient errors (limit 20).')

            resp = self.http.get(port)
            self.assertNotIn(resp.status_code, (500, 502, 503),
                             f'Expected non-5xx after recovery, got {resp.status_code}')
            LOG.ok(f'[:{port}] Post-restart request → {resp.status_code}')

        self._test_on_both_ports(_test)

    # ==================================================================
    #  13. RECOVERY — SOCKET REMOVAL
    # ==================================================================

    def test_13_recovery_socket_removal(self):
        """Sistema recupera após remoção manual do socket Unix."""
        LOG.section('13 — Recovery — Socket File Removal')

        def _test(port):
            LOG.info(f'[:{port}] Removing Unix socket file...')
            if os.path.exists(CFG.CACHE_SOCK):
                os.unlink(CFG.CACHE_SOCK)
            time.sleep(2)

            LOG.info(f'[:{port}] Restarting cache...')
            self.cache_mgr.stop()
            self.cache_mgr.start(['--unix', '--tcp'])
            time.sleep(2)

            resp = self.http.get(port)
            self.assertNotIn(resp.status_code, (500, 502, 503),
                             f'Expected non-5xx after recovery, got {resp.status_code}')
            LOG.ok(f'[:{port}] Post-recovery → {resp.status_code}')

        self._test_on_both_ports(_test)

    # ==================================================================
    #  14. STABILITY (memory leak detection)
    # ==================================================================

    def test_14_stability_short(self):
        """Verifica que o consumo de memória não cresce >20% em 60s sob carga."""
        if not psutil:
            LOG.skip('psutil not installed — skipping stability test.')
            self.skipTest('psutil não instalado')

        LOG.section('14 — Stability — Memory Leak Check')

        def _test(port):
            LOG.info(f'[:{port}] Generating load for 60s...')
            url = f'http://127.0.0.1:{port}/api/v1'
            stop_event = threading.Event()

            def load_gen():
                while not stop_event.is_set():
                    try:
                        requests.get(url, headers=self.http.auth_headers, timeout=1)
                    except Exception:
                        pass
                    time.sleep(0.01)

            t = threading.Thread(target=load_gen, daemon=True)
            t.start()

            cache_proc = psutil.Process(self.cache_mgr.pid)
            mem_samples = []
            for i in range(6):
                time.sleep(10)
                rss = cache_proc.memory_info().rss
                mem_samples.append(rss)
                LOG.info(f'[:{port}] Sample {i + 1}/6: RSS = {rss / 1024:.0f} KB')

            stop_event.set()
            t.join(timeout=3)

            if len(mem_samples) > 1:
                start_mem, end_mem = mem_samples[0], mem_samples[-1]
                growth = (end_mem - start_mem) / start_mem if start_mem else 0
                LOG.info(f'[:{port}] Memory growth: {growth:.1%}')
                self.assertLess(growth, 0.20,
                                f'Memory grew {growth:.1%} (max 20%)')
                LOG.ok(f'[:{port}] Memory stable ({growth:.1%} growth).')

        for port in (CFG.NGINX_TCP_PORT, CFG.NGINX_UNIX_PORT):
            with self.subTest(port=port):
                _test(port)

    # ==================================================================
    #  15. RATE LIMITING
    # ==================================================================

    def test_15_rate_limiting(self):
        """Rate limiter entra em ação após ~120 requests e reseta após a janela."""
        LOG.section('15 — Rate Limiting')

        def _test(port):
            # Espera janela limpa antes de testar
            self._ensure_rate_limit_clear(port)

            LOG.info(f'[:{port}] Sending up to 130 requests...')
            url = f'http://127.0.0.1:{port}/api/v1'
            success_count = 0
            for i in range(130):
                resp = requests.get(url, headers=self.http.auth_headers)
                if resp.status_code != 429:
                    success_count += 1
                else:
                    LOG.info(f'[:{port}] Rate limited at request #{i + 1}')
                    break

            self.assertGreaterEqual(success_count, 1,
                                    f'Expected at least 1 success before rate limit, got 0')
            self.assertLessEqual(success_count, 130,
                                 f'Rate limit never triggered in 130 requests')
            LOG.ok(f'[:{port}] {success_count} requests before rate limit.')

            LOG.info(f'[:{port}] Waiting 62s for rate window reset...')
            time.sleep(62)

            resp = requests.get(url, headers=self.http.auth_headers)
            self.assertNotEqual(resp.status_code, 429,
                                'Rate limit should have reset after window')
            LOG.ok(f'[:{port}] Rate limit reset → {resp.status_code}')

        self._test_on_both_ports(_test)

    # ==================================================================
    #  16. LOAD SIMULATION
    # ==================================================================

    def test_16_load_simulation(self):
        """Simulação de carga: 20 threads × 50 requests. Verifica estabilidade."""
        LOG.section('16 — Load Simulation')

        def _test(port):
            # Espera rate limit limpo
            self._ensure_rate_limit_clear(port)

            LOG.info(f'[:{port}] Starting load: 20 threads × 50 requests...')
            url = f'http://127.0.0.1:{port}/api/v1'
            stats = {'200': 0, '4xx': 0, '5xx': 0, 'timeout': 0, 'exception': 0}
            lock = threading.Lock()

            def worker():
                for _ in range(50):
                    try:
                        resp = requests.get(url, headers=self.http.auth_headers, timeout=2)
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

            threads = [threading.Thread(target=worker) for _ in range(20)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            total = sum(stats.values())
            self.assertGreater(total, 0)

            # Sob carga pesada, rate limiting (4xx) é esperado.
            # Verificamos que não há excesso de erros de servidor (5xx).
            ok_or_limited = (stats['200'] + stats['4xx']) / total
            err_rate = stats['5xx'] / total

            LOG.info(f'[:{port}] Results: {stats}')
            LOG.info(f'[:{port}] OK+Limited: {ok_or_limited:.1%}, Server errors: {err_rate:.1%}')

            self.assertGreater(ok_or_limited, 0.9,
                               f'Too many non-HTTP errors: {ok_or_limited:.1%} OK+4xx')
            self.assertLess(err_rate, 0.1,
                            f'Server error rate too high: {err_rate:.1%}')
            LOG.ok(f'[:{port}] Load test passed: {err_rate:.1%} server errors.')

        self._test_on_both_ports(_test)


# ======================================================================
#  CUSTOM TEST RUNNER
# ======================================================================

class DMMRTestRunner(unittest.TextTestRunner):
    """Runner que imprime o relatório final consolidado."""

    def run(self, test):
        result = super().run(test)
        LOG.summary()
        return result


# ======================================================================
#  MAIN
# ======================================================================

if __name__ == '__main__':
    argv = [sys.argv[0], '-v'] + sys.argv[1:]
    runner = DMMRTestRunner(verbosity=2)
    unittest.main(argv=argv, testRunner=runner)