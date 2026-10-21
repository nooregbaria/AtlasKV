#!/usr/bin/env python3
"""AtlasKV Phase 6: binary replication + restart recovery integration test (macOS).

Usage: python3 tests/test_binary_replication.py ./build/bin/atlas_server
Stops/starts test servers on ports 8081/8082. Uses a temporary working directory.
"""
import argparse
import os
import pathlib
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import zlib

HOST = '127.0.0.1'
PRIMARY_PORT = 8081
REPLICA_PORT = 8082
CASES = [
    (b'spaces', b'hello   world'),
    (b'newlines', b'hello\nworld'),
    (b'nul', b'hello\x00world'),
    (b'ordinary', b'hello world'),
    (b'binary', bytes([0, 1, 2, 127, 128, 255])),
]


def wal_record(lsn, op, key, value=b''):
    # WAL currently stores native-endian uint64/uint32 fields on macOS.
    payload = struct.pack('=Q', lsn) + bytes([ord(op)]) + struct.pack('=I', len(key))
    if op == '1':
        payload += struct.pack('=I', len(value))
    payload += key + (value if op == '1' else b'')
    return payload + struct.pack('=I', zlib.crc32(payload))


def connect(port, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return socket.create_connection((HOST, port), timeout=1)
        except OSError:
            time.sleep(0.04)
    raise RuntimeError(f'Port {port} did not become available')


def frame_send(sock, data):
    sock.sendall(struct.pack('!I', len(data)) + data)


def read_exact(sock, n):
    chunks = []
    while n:
        block = sock.recv(n)
        if not block:
            raise RuntimeError('Connection closed during framed response')
        chunks.append(block)
        n -= len(block)
    return b''.join(chunks)


def frame_recv(sock):
    length = struct.unpack('!I', read_exact(sock, 4))[0]
    if length > 11 * 1024 * 1024:
        raise RuntimeError('Unexpectedly large response')
    return read_exact(sock, length)


def get_lsn(port):
    with connect(port) as sock:
        frame_send(sock, b'GET_LSN')
        result = frame_recv(sock)
    if not result.startswith(b'LSN '):
        raise RuntimeError(f'Unexpected LSN response: {result!r}')
    return int(result[4:])


def stop(proc):
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=3)


def start(binary, role, cwd, logs):
    proc = subprocess.Popen([str(binary), role], cwd=cwd,
                            stdout=logs, stderr=subprocess.STDOUT)
    port = PRIMARY_PORT if role == 'primary' else REPLICA_PORT
    try:
        with connect(port):
            pass
        if proc.poll() is not None:
            raise RuntimeError(f'{role} exited early with code {proc.returncode}')
        return proc
    except Exception:
        stop(proc)
        raise


def wait_for_lsn(port, expected, proc, timeout=12):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'Server exited unexpectedly ({proc.returncode})')
        try:
            last = get_lsn(port)
            if last == expected:
                return
        except (OSError, RuntimeError, ValueError):
            pass
        time.sleep(0.05)
    raise RuntimeError(f'Expected LSN {expected}, last observed {last}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('server', nargs='?', default=str(pathlib.Path(__file__).resolve().parents[1] / 'build/bin/atlas_server'))
    args = parser.parse_args()
    binary = pathlib.Path(args.server).expanduser().resolve()
    if not binary.is_file():
        parser.error(f'Server executable not found: {binary}')

    # Ensure no existing server is listening: this test must never touch a user's running DB.
    for port in (PRIMARY_PORT, REPLICA_PORT):
        try:
            with socket.create_connection((HOST, port), timeout=0.3):
                raise RuntimeError(f'Port {port} is in use. Stop your AtlasKV servers first.')
        except (ConnectionRefusedError, TimeoutError, socket.timeout):
            pass

    with tempfile.TemporaryDirectory(prefix='atlaskv_binary_') as directory:
        root = pathlib.Path(directory)
        primary_wal = root / 'log.txt'
        replica_wal = root / 'replica_log.txt'
        records = [wal_record(i, '1', key, value)
                   for i, (key, value) in enumerate(CASES, start=1)]
        # Include a REMOVE to verify both operation types in the binary format.
        records.append(wal_record(len(records) + 1, '2', b'ordinary'))
        expected = b''.join(records)
        primary_wal.write_bytes(expected)
        print(f'Created primary WAL: {len(records)} operations, {len(expected)} bytes')

        replica = primary = None
        log_path = root / 'servers.log'
        with log_path.open('wb') as logs:
            try:
                replica = start(binary, 'replica', root, logs)
                print('Replica started at LSN 0')
                primary = start(binary, 'primary', root, logs)
                print('Primary started with prebuilt binary WAL')
                wait_for_lsn(REPLICA_PORT, len(records), replica)
                print(f'PASS: Replica caught up to LSN {len(records)}')

                actual = replica_wal.read_bytes()
                if actual != expected:
                    raise AssertionError('Replica WAL differs from primary WAL (binary data not preserved)')
                print('PASS: Exact WAL byte match, including spaces, newlines, NUL and non-UTF8 bytes')

                stop(primary)
                primary = None
                stop(replica)
                replica = None
                replica = start(binary, 'replica', root, logs)
                wait_for_lsn(REPLICA_PORT, len(records), replica)
                print('PASS: Replica independently restarted and recovered all binary WAL entries')
                if replica_wal.read_bytes() != expected:
                    raise AssertionError('Replica WAL changed after restart')
                print('PASS: WAL remains byte-identical after recovery')
                print('\nALL TESTS PASSED')
            except Exception as exc:
                print(f'\nFAIL: {exc}', file=sys.stderr)
                print(f'Server output:\n{log_path.read_text(errors="replace")}', file=sys.stderr)
                return 1
            finally:
                stop(primary)
                stop(replica)
    return 0


if __name__ == '__main__':
    sys.exit(main())
