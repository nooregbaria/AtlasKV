#!/usr/bin/env python3
"""AtlasKV Raft three-node integration test.

Run: python3 tests/test_atlaskv_raft.py --binary ./build/bin/raft_server
Uses fixed localhost RPC ports 9001..9003 and client ports 9101..9103.
All node storage is created in a temporary directory, never in your project.
"""
import argparse
import concurrent.futures
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time

HOST = '127.0.0.1'
NODES = (1, 2, 3)

class TestFailure(Exception):
    pass


def request(node, command, timeout=7.0):
    payload = command.encode('utf-8')
    with socket.create_connection((HOST, 9100 + node), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack('!I', len(payload)) + payload)
        header = read_exact(sock, 4)
        length = struct.unpack('!I', header)[0]
        if length > 11 * 1024 * 1024:
            raise TestFailure(f'Node {node}: invalid response length {length}')
        return read_exact(sock, length).decode('utf-8')


def read_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError('Connection closed before complete response')
        data.extend(chunk)
    return bytes(data)


class Cluster:
    def __init__(self, binary, workdir):
        self.binary = str(Path(binary).resolve())
        self.workdir = workdir
        self.processes = {}
        self.logs = {}

    def start(self, node):
        if node in self.processes and self.processes[node].poll() is None:
            raise TestFailure(f'Node {node} already running')
        log_path = Path(self.workdir) / f'node{node}.stdout.log'
        log = open(log_path, 'ab', buffering=0)
        proc = subprocess.Popen([self.binary, str(node)], cwd=self.workdir,
                                stdout=log, stderr=subprocess.STDOUT)
        self.processes[node] = proc
        self.logs[node] = log
        print(f'    started node {node} (PID {proc.pid})')

    def stop(self, node):
        proc = self.processes.get(node)
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
        print(f'    stopped node {node}')

    def running(self):
        return [n for n in NODES if n in self.processes and self.processes[n].poll() is None]

    def leader(self, candidates=None, timeout=12):
        candidates = list(candidates if candidates is not None else self.running())
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leaders = []
            for n in candidates:
                if n not in self.running():
                    continue
                try:
                    answer = request(n, 'get __raft_test_probe_nonexistent__', timeout=1.5)
                    if answer != 'NOT_LEADER':
                        leaders.append(n)
                except (OSError, TimeoutError, ConnectionError):
                    pass
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.2)
        raise TestFailure(f'No unique responsive leader among {candidates} within {timeout}s')

    def close(self):
        for n in list(self.processes):
            self.stop(n)
        for log in self.logs.values():
            log.close()


def expect(actual, expected, description):
    if actual != expected:
        raise TestFailure(f'{description}: expected {expected!r}, got {actual!r}')


def retry_expect(fn, expected, description, timeout=10):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = fn()
            if last == expected:
                return
        except (OSError, ConnectionError, TimeoutError) as exc:
            last = repr(exc)
        time.sleep(0.15)
    raise TestFailure(f'{description}: expected {expected!r}, last observed {last!r}')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--binary', default=str(Path(__file__).resolve().parents[1] / 'build/bin/raft_server'), help='Path to compiled Raft server executable')
    ap.add_argument('--keep-data', action='store_true', help='Keep temporary storage and logs after test')
    args = ap.parse_args()
    binary = Path(args.binary).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        print(f'ERROR: executable not found: {binary}\nCompile your raft_server first.', file=sys.stderr)
        return 2

    tmp = tempfile.mkdtemp(prefix='atlaskv-raft-test-')
    cluster = Cluster(binary, tmp)
    passed = 0
    failed = 0

    def check(name, func):
        nonlocal passed, failed
        print(f'\nTEST: {name}', flush=True)
        try:
            func()
        except Exception as exc:
            failed += 1
            print(f'  FAIL: {exc}', flush=True)
            raise
        else:
            passed += 1
            print('  PASS', flush=True)

    try:
        check('Start all three nodes and elect a leader', lambda: [cluster.start(n) for n in NODES])
        leader = cluster.leader()
        print(f'    elected leader: node {leader}')

        def command_results():
            expect(request(leader, 'put apple red'), 'OK', 'initial PUT')
            expect(request(leader, 'put apple green'), 'KEY_ALREADY_EXISTS', 'duplicate PUT')
            expect(request(leader, 'get apple'), 'red', 'GET original value')
            expect(request(leader, 'remove apple'), 'OK', 'REMOVE existing')
            expect(request(leader, 'remove apple'), 'NOT_FOUND', 'REMOVE missing')
            expect(request(leader, 'get apple'), 'NOT_FOUND', 'GET missing')
            expect(request(leader, 'put phrase hello world from raft'), 'OK', 'multiword PUT')
            expect(request(leader, 'get phrase'), 'hello world from raft', 'multiword GET')
        check('PUT / duplicate PUT / GET / REMOVE / missing key / multiword values', command_results)

        def malformed():
            for cmd in ('', 'put', 'put onlykey', 'remove', 'remove a b', 'unknown xyz', 'get', 'get a b'):
                response = request(leader, cmd)
                if not response.startswith('ERROR'):
                    raise TestFailure(f'{cmd!r}: expected ERROR response, got {response!r}')
        check('Malformed and unsupported commands are rejected', malformed)

        def follower_rejects():
            for n in NODES:
                if n != leader:
                    expect(request(n, 'put follower_write forbidden'), 'NOT_LEADER', f'node {n} rejects PUT')
                    expect(request(n, 'get phrase'), 'NOT_LEADER', f'node {n} rejects GET')
        check('Followers reject client reads and writes', follower_rejects)

        def ordered_writes():
            for i in range(12):
                expect(request(leader, f'put ordered_{i} value_{i}'), 'OK', f'ordered PUT {i}')
            for i in range(12):
                expect(request(leader, f'get ordered_{i}'), f'value_{i}', f'ordered GET {i}')
        check('Multiple ordered committed writes', ordered_writes)

        def concurrent_clients():
            # The transport currently handles connections serially; this still checks
            # that simultaneous callers do not receive incorrect application results.
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                futures = [pool.submit(request, leader, 'put collision same_value', 15.0) for _ in range(8)]
                answers = [f.result(timeout=20) for f in futures]
            if sorted(answers) != sorted(['OK'] + ['KEY_ALREADY_EXISTS'] * 7):
                raise TestFailure(f'Expected 1 OK and 7 KEY_ALREADY_EXISTS, got {answers!r}')
        check('Concurrent clients writing the same key', concurrent_clients)

        def leader_failure():
            nonlocal leader
            old = leader
            cluster.stop(old)
            leader = cluster.leader(timeout=15)
            if leader == old:
                raise TestFailure('Stopped node was still reported as leader')
            print(f'    new leader: node {leader}')
            retry_expect(lambda: request(leader, 'get phrase'), 'hello world from raft',
                         'committed value survived leader crash', timeout=8)
            for i in range(12):
                expect(request(leader, f'get ordered_{i}'), f'value_{i}', f'ordered_{i} after failover')
            expect(request(leader, 'put after_failover yes'), 'OK', 'write after failover')
        check('Leader crash, new election, committed data preserved, new writes work', leader_failure)

        def restart():
            stopped = next(n for n in NODES if n not in cluster.running())
            cluster.start(stopped)
            retry_expect(lambda: request(stopped, 'get phrase'), 'NOT_LEADER',
                         'restarted node becomes reachable follower', timeout=12)
            # We cannot GET from followers through the current client API.
            # Later promotion will verify that a former follower retained data.
            expect(request(leader, 'get after_failover'), 'yes', 'cluster works after restart')
        check('Restart crashed node from persisted state', restart)

        def quorum_loss():
            nonlocal leader
            # Isolate the currently elected leader by stopping both followers.
            followers = [n for n in NODES if n != leader]
            for n in followers:
                cluster.stop(n)
            response = request(leader, 'put no_quorum should_not_ack', timeout=9)
            if response == 'OK':
                raise TestFailure('Leader acknowledged a write without a majority!')
            if response not in ('TIMEOUT UNKNOWN_OUTCOME', 'NOT_LEADER', 'ERROR LEADERSHIP_CHANGED'):
                raise TestFailure(f'Unexpected response without quorum: {response!r}')
            print(f'    no-quorum response: {response}')
            # Restore followers, then allow any legitimate election/commit to finish.
            for n in followers:
                cluster.start(n)
            leader = cluster.leader(timeout=18)
            expect(request(leader, 'put after_quorum restored'), 'OK', 'write after quorum restored')
        check('No majority => no successful write acknowledgment; recovery after quorum returns', quorum_loss)

        def full_restart():
            nonlocal leader
            for n in NODES:
                cluster.stop(n)
            for n in NODES:
                cluster.start(n)
            leader = cluster.leader(timeout=18)
            retry_expect(lambda: request(leader, 'get phrase'), 'hello world from raft',
                         'data after full restart', timeout=12)
            expect(request(leader, 'get after_quorum'), 'restored', 'post-quorum data after restart')
            expect(request(leader, 'put phrase changed'), 'KEY_ALREADY_EXISTS',
                   'duplicate check survives restart')
        check('Full-cluster restart preserves data and logical command outcomes', full_restart)

    except Exception:
        pass
    finally:
        cluster.close()
        print(f'\nRESULT: {passed} PASS, {failed} FAIL')
        if args.keep_data or failed:
            print(f'Storage and per-node logs retained: {tmp}')
            print('Inspect node1.stdout.log, node2.stdout.log, node3.stdout.log for diagnostics.')
        else:
            shutil.rmtree(tmp, ignore_errors=True)

    return 1 if failed else 0

if __name__ == '__main__':
    sys.exit(main())
