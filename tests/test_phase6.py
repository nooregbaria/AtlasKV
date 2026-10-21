#!/usr/bin/env python3
"""AtlasKV Phase 6 integration test. Run in project directory: python3 test_phase6.py"""
import os
import socket
import struct
import subprocess
import time
import zlib
from pathlib import Path

ROOT = Path.cwd()
SERVER = Path(__file__).resolve().parents[1] / 'build/bin/atlas_server'
PRIMARY_WAL = ROOT / 'log.txt'
REPLICA_WAL = ROOT / 'replica_log.txt'
BACKUPS = {}
PROCS = []
passed = 0


def check(label, actual, expected):
    global passed
    if actual != expected:
        raise AssertionError(f'{label}: expected {expected!r}, got {actual!r}')
    passed += 1
    print(f'PASS {label}: {actual!r}', flush=True)


def exchange(port, command):
    with socket.create_connection(('127.0.0.1', port), timeout=3) as s:
        s.settimeout(3)
        payload = command.encode() if isinstance(command, str) else command
        s.sendall(struct.pack('!I', len(payload)) + payload)
        size = struct.unpack('!I', read_exact(s, 4))[0]
        return read_exact(s, size).decode()


def replicate(lsn, key=b'X', value=b'bad', operation=1):
    return struct.pack('!4sBQII', b'RPL1', operation, lsn, len(key), len(value)) + key + value


def check_catch_up_output(expected_lsn):
    # Call after a primary response so the completion acknowledgement has arrived.
    prefix = 'Replica catch-up complete. LSN: '
    replica_lines = (ROOT / 'phase6_test_replica.log').read_text().splitlines()
    check(f'replica prints catch-up LSN {expected_lsn}',
          [line for line in replica_lines if line.startswith(prefix)],
          [prefix + str(expected_lsn)])
    check('primary does not print replica catch-up',
          prefix in (ROOT / 'phase6_test_primary.log').read_text(), False)


def wal_record(lsn, key, value=b'', operation=1):
    # The on-disk WAL uses native byte order without struct padding.
    record = struct.pack('=QcI', lsn, b'1' if operation == 1 else b'2', len(key))
    if operation == 1:
        record += struct.pack('=I', len(value))
    record += key + value
    return record + struct.pack('=I', zlib.crc32(record))


def read_exact(s, count):
    result = bytearray()
    while len(result) < count:
        chunk = s.recv(count - len(result))
        if not chunk:
            raise ConnectionError('Server disconnected before response')
        result.extend(chunk)
    return bytes(result)


def start(role, port):
    log = open(ROOT / f'phase6_test_{role}.log', 'w')
    p = subprocess.Popen([str(SERVER), role], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    log.close()
    PROCS.append(p)
    for _ in range(60):
        if p.poll() is not None:
            raise RuntimeError(f'{role} exited early; see phase6_test_{role}.log')
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=.15):
                return
        except OSError:
            time.sleep(.1)
    raise RuntimeError(f'{role} did not listen on port {port}')


def stop_all():
    for p in reversed(PROCS):
        if p.poll() is None:
            p.terminate()
    for p in reversed(PROCS):
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
    PROCS.clear()


def lsn(path):
    # First eight bytes are LSN=1, stored in host byte order.
    if not path.exists() or path.stat().st_size < 8:
        raise AssertionError(f'WAL missing/empty: {path}')
    import sys
    return int.from_bytes(path.read_bytes()[:8], sys.byteorder)


def run():
    if not SERVER.is_file():
        raise FileNotFoundError('Build build/bin/atlas_server first, then run from a disposable working directory')
    for path in (PRIMARY_WAL, REPLICA_WAL):
        if path.exists():
            backup = path.with_name(path.name + '.phase6_backup')
            if backup.exists():
                raise FileExistsError(f'Backup already exists: {backup}. Move it before running.')
            path.rename(backup)
            BACKUPS[path] = backup
    try:
        start('replica', 8082)
        start('primary', 8081)
        check('PUT A', exchange(8081, 'put A hello'), 'OK')
        check_catch_up_output(0)
        check('PUT B', exchange(8081, 'put B world'), 'OK')
        replica_prefix = REPLICA_WAL.read_bytes()
        check('duplicate PUT', exchange(8081, 'put A again'), 'KEY_ALREADY_EXISTS')
        check('REMOVE A', exchange(8081, 'remove A'), 'OK')
        check('GET A', exchange(8081, 'get A'), 'NOT_FOUND')
        check('GET B', exchange(8081, 'get B'), 'world')
        check('replica rejects duplicate LSN', exchange(8082, replicate(3)), 'LSN_MISMATCH')
        check('replica rejects skipped LSN', exchange(8082, replicate(5)), 'LSN_MISMATCH')
        check('replica rejects zero LSN', exchange(8082, replicate(0)), 'LSN_MISMATCH')
        before = REPLICA_WAL.read_bytes()
        malformed = {
            'old text format': b'REPLICATE 4 put X bad',
            'short header': replicate(4)[:20],
            'truncated body': replicate(4)[:-1],
            'trailing bytes': replicate(4) + b'x',
            'unknown operation': replicate(4, operation=3),
            'unknown version': b'RPL2' + replicate(4)[4:],
            'remove with a value': replicate(4, operation=2),
            'oversized key': replicate(4, key=b'k' * 1025),
            'oversized value length': struct.pack('!4sBQII', b'RPL1', 1, 4, 1, 10 * 1024 * 1024 + 1),
            'overflowing lengths': struct.pack('!4sBQII', b'RPL1', 1, 4, 0xffffffff, 0xffffffff),
        }
        for label, message in malformed.items():
            check(f'replica rejects {label}', exchange(8082, message), 'INVALID_REPLICATION')
        check('invalid messages leave WAL unchanged', REPLICA_WAL.read_bytes() == before, True)
        check('replica rejects direct PUT', exchange(8082, 'put X bad'), 'INVALID_REPLICATION')
        stop_all()
        start('replica', 8082)
        start('primary', 8081)
        check('GET B after restart', exchange(8081, 'get B'), 'world')
        check_catch_up_output(3)
        check('GET A after restart', exchange(8081, 'get A'), 'NOT_FOUND')
        check('PUT C after restart', exchange(8081, 'put  C\t test  '), 'OK')
        stop_all()
        for label, contents, expected_lsn in (('partial', replica_prefix, 4), ('empty', b'', 6)):
            REPLICA_WAL.write_bytes(contents)
            start('replica', 8082)
            start('primary', 8081)
            check(f'{label} catch-up preserves B', exchange(8081, 'get B'), 'world')
            check_catch_up_output(expected_lsn)
            check(f'{label} catch-up preserves removal', exchange(8081, 'get A'), 'NOT_FOUND')
            check(f'{label} catch-up LSN', exchange(8082, 'GET_LSN'), f'LSN {expected_lsn}')
            # Removing C proves catch-up rebuilt the replica's in-memory state.
            check(f'{label} catch-up allows remove', exchange(8081, 'remove C'), 'OK')
            check(f'{label} catch-up allows put', exchange(8081, 'put C test'), 'OK')
            stop_all()
            check(f'{label} catch-up WALs match', PRIMARY_WAL.read_bytes() == REPLICA_WAL.read_bytes(), True)
        # Count/validate final WAL records by using restart success + same WAL bytes.
        check('primary and replica WAL identical', PRIMARY_WAL.read_bytes(), REPLICA_WAL.read_bytes())
        check('first WAL LSN', lsn(PRIMARY_WAL), 1)

        # Seed WAL bytes that cannot be represented by the text client grammar.
        key = b' key\x00\xff\t\n '
        value = bytes(range(256)) + b'  repeated   spaces\t\n\r\x00'
        removed_key = b'remove me\x00\t'
        history = (wal_record(1, key, value)
                   + wal_record(2, removed_key, b'temporary')
                   + wal_record(3, removed_key, operation=2)
                   + wal_record(4, b'', b''))
        PRIMARY_WAL.write_bytes(history)
        REPLICA_WAL.write_bytes(b'')
        start('replica', 8082)
        start('primary', 8081)
        # A primary response confirms that startup catch-up has finished.
        check('binary catch-up startup completes', exchange(8081, 'get missing'), 'NOT_FOUND')
        check('binary catch-up preserves every WAL byte', REPLICA_WAL.read_bytes() == history, True)
        check('live replication accepts binary bytes', exchange(8081, b'put live\x00\xff value\x00\xff'), 'OK')
        history += wal_record(5, b'live\x00\xff', b'value\x00\xff')
        check('live primary WAL bytes', PRIMARY_WAL.read_bytes() == history, True)
        check('live replica WAL bytes', REPLICA_WAL.read_bytes() == history, True)
        stop_all()
        start('replica', 8082)
        start('primary', 8081)
        check('binary recovery startup completes', exchange(8081, 'get missing'), 'NOT_FOUND')
        check('binary key restored in replica', exchange(8082, replicate(6, key, b'', 2)), 'OK')
        check('empty key restored in replica', exchange(8082, replicate(7, b'', b'', 2)), 'OK')
        check('binary removed key stays absent', exchange(8082, replicate(8, removed_key, b'', 2)), 'NOT_FOUND')
        check('failed remove preserves LSN', exchange(8082, 'GET_LSN'), 'LSN 7')
        stop_all()
        print(f'\nALL {passed} TESTS PASSED. WAL files are preserved as phase6_test_*.wal.')
    finally:
        stop_all()
        for path in (PRIMARY_WAL, REPLICA_WAL):
            if path.exists():
                path.rename(ROOT / ('phase6_test_primary.wal' if path == PRIMARY_WAL else 'phase6_test_replica.wal'))
            if path in BACKUPS:
                BACKUPS[path].rename(path)


if __name__ == '__main__':
    try:
        run()
    except Exception as exc:
        print(f'\nFAIL: {exc}', flush=True)
        print('Check phase6_test_primary.log and phase6_test_replica.log for details.')
        raise SystemExit(1)
