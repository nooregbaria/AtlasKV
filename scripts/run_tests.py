#!/usr/bin/env python3
"""Run the existing tests sequentially, with disposable storage and per-test logs."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / 'build/bin'
TEST_BIN = ROOT / 'build/tests'


def stop(proc):
    # Each child owns a session, so its servers are also cleaned up on timeout.
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def wait_for_port(proc, port):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'Companion server exited with {proc.returncode}')
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f'Companion server did not open port {port}')


def run_case(name, command, directory, companions=(), expected_summary=None):
    directory.mkdir()
    children = []
    result = {'test': name, 'command': [str(arg) for arg in command]}
    started = time.monotonic()
    with (directory / 'output.log').open('w') as output:
        try:
            for args, port in companions:
                proc = subprocess.Popen(args, cwd=directory, stdout=output,
                                        stderr=subprocess.STDOUT, start_new_session=True)
                children.append(proc)
                wait_for_port(proc, port)
            proc = subprocess.Popen(command, cwd=directory, stdout=output,
                                    stderr=subprocess.STDOUT, start_new_session=True)
            children.append(proc)
            code = proc.wait(timeout=180)
            result['exit_code'] = code
            result['status'] = 'PASS' if code == 0 else 'FAIL'
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
            result['status'] = 'FAIL'
            result['error'] = str(exc)
        finally:
            for proc in reversed(children):
                stop(proc)
    # The original Raft scripts can swallow an early exception and return zero.
    # Require their complete summary; do not change their assertions or control flow.
    if expected_summary and result['status'] == 'PASS':
        if expected_summary not in (directory / 'output.log').read_text():
            result['status'] = 'FAIL'
            result['error'] = f'Missing complete suite summary: {expected_summary}'
    result['seconds'] = round(time.monotonic() - started, 2)
    result['log'] = str(directory / 'output.log')
    print(f"{result['status']}: {name} ({result['seconds']}s)", flush=True)
    if result['status'] != 'PASS':
        print(f"  {result.get('error', 'See ' + result['log'])}", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--only', nargs='+', help='Test names without file extensions')
    args = parser.parse_args()
    cases = []
    for source in sorted((ROOT / 'tests').glob('*.cpp')):
        name = source.stem.replace(' ', '_')
        if name == 'test_raft_network':
            continue  # The listening server is exercised by test_raft_client below.
        companions = []
        if name == 'test_raft_client':
            companions = [([str(TEST_BIN / 'test_raft_network')], 9001)]
        elif name == 'concurrency_test':
            companions = [([str(BIN / 'atlas_server'), 'replica'], 8082),
                          ([str(BIN / 'atlas_server'), 'primary'], 8081)]
        cases.append((name, [str(TEST_BIN / name)], companions, None))
    for source in sorted((ROOT / 'tests').glob('test_*.py')):
        command = [sys.executable, str(source)]
        summary = None
        if source.stem in ('test_atlaskv_final', 'test_atlaskv_raft'):
            command += ['--binary', str(BIN / 'raft_server')]
            count = 11 if source.stem == 'test_atlaskv_final' else 10
            summary = f'RESULT: {count} PASS, 0 FAIL'
        elif source.stem != 'test_phase6':
            command += [str(BIN / 'atlas_server')]
        cases.append((source.stem, command, [], summary))
    if args.only:
        unknown = set(args.only) - {case[0] for case in cases}
        if unknown:
            parser.error('Unknown tests: ' + ', '.join(sorted(unknown)))
        cases = [case for case in cases if case[0] in args.only]
    # Tests use fixed ports and must not attach to an unrelated local server.
    for port in (8081, 8082, 9001, 9002, 9003, 9101, 9102, 9103):
        with socket.socket() as probe:
            if probe.connect_ex(('127.0.0.1', port)) == 0:
                parser.error(f'Port {port} is occupied; stop your server before running tests')
    results_parent = ROOT / 'build/test-results'
    results_parent.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix='run-', dir=results_parent))
    print(f'Test logs and disposable storage: {run_dir}', flush=True)
    results = []
    for name, command, companions, summary in cases:
        results.append(run_case(name, command, run_dir / name, companions, summary))
        (run_dir / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
    failed = sum(result['status'] != 'PASS' for result in results)
    print(f'\nRESULT: {len(results) - failed} PASS, {failed} FAIL')
    return int(failed != 0)


if __name__ == '__main__':
    sys.exit(main())
