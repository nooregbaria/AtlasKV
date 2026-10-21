#!/usr/bin/env python3
"""AtlasKV partial replica catch-up integration test (macOS/local ports 8081, 8082)."""
import argparse
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import zlib

HOST = '127.0.0.1'
PRIMARY_PORT, REPLICA_PORT = 8081, 8082
# WAL uses host-native integer byte order, as in wal.cpp.
U64 = '=Q'
U32 = '=I'


def record(lsn, operation, key, value=b''):
    key = key.encode()
    value = value.encode() if isinstance(value, str) else value
    if operation == 'put':
        raw = struct.pack(U64, lsn) + b'1' + struct.pack('=II', len(key), len(value)) + key + value
    else:
        raw = struct.pack(U64, lsn) + b'2' + struct.pack(U32, len(key)) + key
    return raw + struct.pack(U32, zlib.crc32(raw) & 0xffffffff)


def read_exact(sock, n):
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise RuntimeError('Connection closed while reading response')
        data += chunk
    return data


def request(port, payload):
    with socket.create_connection((HOST, port), timeout=2) as sock:
        sock.settimeout(3)
        sock.sendall(struct.pack('!I', len(payload)) + payload)
        length = struct.unpack('!I', read_exact(sock, 4))[0]
        if length > 1024 * 1024:
            raise RuntimeError('Unexpected response length')
        return read_exact(sock, length)


def wait_ready(proc, port, timeout=6):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            raise RuntimeError(f'Server exited early ({proc.returncode})')
        try:
            with socket.create_connection((HOST, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.08)
    raise RuntimeError(f'Server did not listen on {port}')


def ensure_ports_free():
    for port in (PRIMARY_PORT, REPLICA_PORT):
        try:
            with socket.create_connection((HOST, port), timeout=0.3):
                raise RuntimeError(f'Port {port} is already in use. Stop your AtlasKV servers first.')
        except (ConnectionRefusedError, TimeoutError, OSError):
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('server', type=Path, help='Path to your compiled atlas_server executable')
    args = parser.parse_args()
    server = args.server.expanduser().resolve()
    if not server.is_file() or not os.access(server, os.X_OK):
        parser.error(f'Not an executable: {server}')
    ensure_ports_free()

    entries = [
        record(1, 'put', 'apple', 'red'),
        record(2, 'put', 'banana', 'yellow'),
        record(3, 'remove', 'apple'),
    ]
    processes = []
    with tempfile.TemporaryDirectory(prefix='atlaskv-catchup-') as tmp:
        directory = Path(tmp)
        (directory / 'log.txt').write_bytes(b''.join(entries))
        (directory / 'replica_log.txt').write_bytes(b''.join(entries[:2]))
        logs = []
        try:
            for role, port in [('replica', REPLICA_PORT), ('primary', PRIMARY_PORT)]:
                log = open(directory / f'{role}.output', 'w+')
                logs.append(log)
                proc = subprocess.Popen([str(server), role], cwd=directory, stdout=log, stderr=subprocess.STDOUT)
                processes.append(proc)
                wait_ready(proc, port)
                print(f'{role.capitalize()} started')

            # A successful startup/catch-up must persist exactly one missing REMOVE record.
            expected = b''.join(entries)
            actual = (directory / 'replica_log.txt').read_bytes()
            if actual != expected:
                raise AssertionError('Replica WAL does not match primary WAL after catch-up')
            print('PASS: Replica WAL matches primary WAL (LSNs 1, 2, 3)')

            lsn = request(REPLICA_PORT, b'GET_LSN')
            assert lsn == b'LSN 3', f'Expected LSN 3, got {lsn!r}'
            print('PASS: Replica reports LSN 3')

            # This GET should see the remove applied during catch-up.
            assert request(PRIMARY_PORT, b'get apple') == b'NOT_FOUND'
            assert request(PRIMARY_PORT, b'get banana') == b'yellow'
            print('PASS: Primary recovered expected values')

            # Restart replica independently, verifying WAL replay and LSN recovery.
            replica = processes[0]
            replica.terminate()
            replica.wait(timeout=3)
            logs[0].close()
            log = open(directory / 'replica-restart.output', 'w+')
            logs[0] = log
            replica = subprocess.Popen([str(server), 'replica'], cwd=directory, stdout=log, stderr=subprocess.STDOUT)
            processes[0] = replica
            wait_ready(replica, REPLICA_PORT)
            assert request(REPLICA_PORT, b'GET_LSN') == b'LSN 3'
            print('PASS: Replica independently restarts and recovers LSN 3')
            print('\nALL TESTS PASSED')
        except Exception as exc:
            print(f'\nFAIL: {exc}', file=sys.stderr)
            for log in logs:
                if not log.closed:
                    log.flush()
            for role in ('replica', 'primary', 'replica-restart'):
                path = directory / f'{role}.output'
                if path.exists():
                    print(f'\n--- {role} output ---\n{path.read_text(errors="replace")}', file=sys.stderr)
            return 1
        finally:
            for proc in processes:
                if proc.poll() is None:
                    proc.terminate()
            for proc in processes:
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            for log in logs:
                if not log.closed:
                    log.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
