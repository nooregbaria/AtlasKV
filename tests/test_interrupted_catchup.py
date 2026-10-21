#!/usr/bin/env python3
"""Test interrupted AtlasKV catch-up and subsequent recovery (macOS/POSIX)."""
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
PRIMARY_PORT = 8081
REPLICA_PORT = 8082
COUNT = 1200  # Many fsync'd entries provide time to interrupt catch-up.


def record(lsn, key, value):
    k, v = key.encode(), value.encode()
    data = struct.pack('=Q', lsn) + b'1' + struct.pack('=II', len(k), len(v)) + k + v
    return data + struct.pack('=I', zlib.crc32(data) & 0xffffffff)


def recv_exact(sock, n):
    result = b''
    while len(result) < n:
        chunk = sock.recv(n - len(result))
        if not chunk:
            raise RuntimeError('Socket closed early')
        result += chunk
    return result


def request(port, payload):
    with socket.create_connection((HOST, port), timeout=2) as sock:
        sock.settimeout(3)
        sock.sendall(struct.pack('!I', len(payload)) + payload)
        length = struct.unpack('!I', recv_exact(sock, 4))[0]
        if length > 1024 * 1024:
            raise RuntimeError('Unexpected response size')
        return recv_exact(sock, length)


def wait_for_port(proc, port, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            raise RuntimeError(f'Process exited prematurely: {proc.returncode}')
        try:
            with socket.create_connection((HOST, port), timeout=.15):
                return
        except OSError:
            time.sleep(.03)
    raise RuntimeError(f'Port {port} did not become available')


def check_ports():
    for port in (PRIMARY_PORT, REPLICA_PORT):
        try:
            sock = socket.create_connection((HOST, port), timeout=.2)
        except OSError:
            continue
        else:
            sock.close()
            raise RuntimeError(f'Port {port} is in use. Stop AtlasKV servers before running.')


def stop(proc):
    if proc is None:
        return
    if proc.poll() is None:
        proc.kill()
    try:
        proc.wait(timeout=4)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('server', type=Path, help='Path to atlas_server executable')
    args = parser.parse_args()
    server = args.server.expanduser().resolve()
    if not server.is_file() or not os.access(server, os.X_OK):
        parser.error(f'Executable not found: {server}')
    check_ports()

    entries = [record(i, f'key{i}', f'value{i}') for i in range(1, COUNT + 1)]
    expected = b''.join(entries)
    initial = entries[0]
    replica = primary = None
    logs = []
    with tempfile.TemporaryDirectory(prefix='atlaskv-interrupt-') as tmp:
        directory = Path(tmp)
        replica_path = directory / 'replica_log.txt'
        (directory / 'log.txt').write_bytes(expected)
        replica_path.write_bytes(initial)

        def launch(role, label):
            log = open(directory / f'{label}.output', 'w+')
            logs.append(log)
            proc = subprocess.Popen([str(server), role], cwd=directory, stdout=log, stderr=subprocess.STDOUT)
            wait_for_port(proc, REPLICA_PORT if role == 'replica' else PRIMARY_PORT)
            return proc

        try:
            replica = launch('replica', 'replica-first')
            primary = launch('primary', 'primary-first')
            print('Replica started at LSN 1; primary started at LSN', COUNT, flush=True)

            # Detect some, but not all, records persisted on replica, then freeze it.
            deadline = time.monotonic() + 12
            intermediate = None
            while time.monotonic() < deadline:
                size = replica_path.stat().st_size
                if len(initial) < size < len(expected):
                    os.kill(replica.pid, signal.SIGSTOP)
                    intermediate = replica_path.read_bytes()
                    break
                if size == len(expected):
                    raise RuntimeError('Catch-up completed before interruption. Retry test.')
                if primary.poll() is not None:
                    raise RuntimeError('Primary exited unexpectedly during catch-up')
                time.sleep(.001)
            if intermediate is None:
                raise RuntimeError('Timed out waiting for partial catch-up')

            # Freeze prevents any further replica writes; crash primary while in catch-up.
            stop(primary)
            primary = None
            os.kill(replica.pid, signal.SIGCONT)
            stop(replica)
            replica = None

            partial_size = len(intermediate)
            if not expected.startswith(intermediate):
                raise AssertionError('Replica WAL diverged from primary WAL')
            if not (len(initial) < partial_size < len(expected)):
                raise AssertionError('Interruption did not leave a partial WAL')
            print(f'PASS: Interrupted catch-up after {partial_size:,} of {len(expected):,} WAL bytes')

            # Replica may have an incomplete tail if killed mid-write; WAL recovery must repair it.
            replica = launch('replica', 'replica-restarted')
            lsn_response = request(REPLICA_PORT, b'GET_LSN')
            recovered = int(lsn_response.removeprefix(b'LSN ')) if lsn_response.startswith(b'LSN ') else -1
            if not (1 < recovered < COUNT):
                raise AssertionError(f'Expected partial recovered LSN, got {lsn_response!r}')
            print(f'PASS: Replica independently recovered to LSN {recovered}')

            primary = launch('primary', 'primary-restarted')
            deadline = time.monotonic() + 25
            while time.monotonic() < deadline:
                if replica_path.stat().st_size == len(expected):
                    break
                if primary.poll() is not None:
                    raise RuntimeError('Primary exited during resumed catch-up')
                time.sleep(.02)
            else:
                raise RuntimeError('Timed out waiting for resumed catch-up')
            if replica_path.read_bytes() != expected:
                raise AssertionError('Replica WAL differs from primary WAL')
            assert request(REPLICA_PORT, b'GET_LSN') == f'LSN {COUNT}'.encode()
            assert request(PRIMARY_PORT, f'get key{COUNT}'.encode()) == f'value{COUNT}'.encode()
            print(f'PASS: Resumed catch-up to LSN {COUNT}, exact WAL match')
            print('PASS: Primary recovered expected data')
            print('\nALL TESTS PASSED')
            return 0
        except Exception as exc:
            print(f'\nFAIL: {exc}', file=sys.stderr)
            for log in logs:
                log.flush()
            for path in sorted(directory.glob('*.output')):
                print(f'\n--- {path.name} ---\n{path.read_text(errors="replace")}', file=sys.stderr)
            return 1
        finally:
            # Resume a frozen replica before shutdown, if an exception occurred.
            if replica is not None and replica.poll() is None:
                try:
                    os.kill(replica.pid, signal.SIGCONT)
                except ProcessLookupError:
                    pass
            stop(primary)
            stop(replica)
            for log in logs:
                log.close()


if __name__ == '__main__':
    sys.exit(main())
