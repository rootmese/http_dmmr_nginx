#!/usr/bin/env python3
import socket
import struct
import sys
import os

SOCK_PATH = "/tmp/dmmr_cache.sock"
# Descomente e ajuste para testar via TCP
# SOCK_PATH = ("127.0.0.1", 9080)

# Cores para o output no terminal
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

def connect():
    if isinstance(SOCK_PATH, tuple):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(SOCK_PATH)
    else:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(SOCK_PATH)
    return sock

# -----------------
# MODERN PROTOCOL
# -----------------
def send_modern_request(sock, opcode, key, value=b''):
    magic = 0xD4D4
    version = 1
    flags = 0
    key_len = len(key)
    value_len = len(value)
    timestamp = 0
    
    fmt = '!HHHHIIQ'
    header = struct.pack(fmt, magic, version, opcode, flags, key_len, value_len, timestamp)
    payload = key.encode('utf-8') + value
    
    sock.sendall(header + payload)
    
    # Ler o cabeçalho de resposta (8 bytes)
    resp_header = b''
    while len(resp_header) < 8:
        chunk = sock.recv(8 - len(resp_header))
        if not chunk:
            raise RuntimeError("Conexão fechada prematuramente pelo servidor ao ler o cabeçalho")
        resp_header += chunk
        
    status, resp_len = struct.unpack('!H I', resp_header[:6])
    
    # Ler o payload se houver
    payload_resp = b''
    if resp_len > 0:
        while len(payload_resp) < resp_len:
            chunk = sock.recv(resp_len - len(payload_resp))
            if not chunk:
                raise RuntimeError("Conexão fechada prematuramente pelo servidor ao ler o payload")
            payload_resp += chunk
            
    return status, payload_resp

# -----------------
# LEGACY PROTOCOL
# -----------------
def send_legacy_request(sock, key):
    opcode = 1 # Apenas OP_GET é suportado no modo legado
    key_len = len(key)
    
    fmt = '!HH'
    header = struct.pack(fmt, opcode, key_len)
    payload = key.encode('utf-8')
    
    sock.sendall(header + payload)
    
    # Ler cabeçalho de resposta do legado (4 bytes)
    # status (2 bytes) + payload_len (2 bytes)
    resp_header = b''
    while len(resp_header) < 4:
        chunk = sock.recv(4 - len(resp_header))
        if not chunk:
            raise RuntimeError("Conexão fechada prematuramente pelo servidor ao ler cabeçalho legado")
        resp_header += chunk
        
    status, resp_len = struct.unpack('!HH', resp_header)
    
    payload_resp = b''
    if resp_len > 0:
        while len(payload_resp) < resp_len:
            chunk = sock.recv(resp_len - len(payload_resp))
            if not chunk:
                raise RuntimeError("Conexão fechada prematuramente pelo servidor ao ler payload legado")
            payload_resp += chunk
            
    return status, payload_resp

# -----------------
# TESTS
# -----------------
def run_test(name, func):
    print(f"Executando {name}...", end="", flush=True)
    try:
        func()
        print(f" {GREEN}[OK]{RESET}")
        return True
    except Exception as e:
        print(f" {RED}[FALHOU]{RESET}")
        print(f"  Erro: {e}")
        return False

def test_set_get():
    # 1. SET
    sock = connect()
    status, _ = send_modern_request(sock, 2, "teste_chave", b"teste_valor")
    sock.close()
    assert status == 0, f"Esperava status 0 (OK), obteve {status}"
    
    # 2. GET
    sock = connect()
    status, val = send_modern_request(sock, 1, "teste_chave")
    sock.close()
    assert status == 0, f"Esperava status 0 (OK), obteve {status}"
    assert val == b"teste_valor", f"Esperava b'teste_valor', obteve {val}"

def test_delete():
    # 1. DEL
    sock = connect()
    status, _ = send_modern_request(sock, 3, "teste_chave")
    sock.close()
    assert status == 0, f"Esperava status 0 (OK), obteve {status}"
    
    # 2. GET (deve ser not found)
    sock = connect()
    status, _ = send_modern_request(sock, 1, "teste_chave")
    sock.close()
    assert status == 1, f"Esperava status 1 (NOT FOUND), obteve {status}"

def test_legacy_get():
    # 1. SET via protocolo moderno primeiro
    sock = connect()
    send_modern_request(sock, 2, "chave_legada", b"valor_legado")
    sock.close()
    
    # 2. GET via protocolo legado
    sock = connect()
    status, val = send_legacy_request(sock, "chave_legada")
    sock.close()
    assert status == 0, f"Esperava status 0 (OK), obteve {status}"
    assert val == b"valor_legado", f"Esperava b'valor_legado', obteve {val}"

def main():
    if not isinstance(SOCK_PATH, tuple) and not os.path.exists(SOCK_PATH):
        print(f"{RED}Erro: O socket Unix '{SOCK_PATH}' nao existe. Certifique-se de que o servidor DMMR Cache esta rodando.{RESET}")
        sys.exit(1)
        
    tests = [
        ("Teste SET & GET (Protocolo Moderno)", test_set_get),
        ("Teste DEL & GET (Protocolo Moderno)", test_delete),
        ("Teste GET (Protocolo Legado)", test_legacy_get),
    ]
    
    success = True
    for name, func in tests:
        if not run_test(name, func):
            success = False
            
    if success:
        print(f"\n{GREEN}Todos os testes passaram com sucesso!{RESET}")
    else:
        print(f"\n{RED}Alguns testes falharam.{RESET}")
        sys.exit(1)

if __name__ == "__main__":
    main()
