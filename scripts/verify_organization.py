#!/usr/bin/env python3
"""Compare a pre-organization snapshot with this tree, ignoring comments/include paths.

Usage: python3 scripts/verify_organization.py /path/to/baseline
The baseline contains the original files and baseline-manifest.json (SHA-256).
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
# Preserve quoted literals before recognizing comments. C++ raw strings are handled too.
TOKEN = re.compile(
    r'(?P<raw>(?:u8|u|U|L)?R"(?P<delimiter>[^ ()\\\t\n]{0,16})\(.*?\)(?P=delimiter)")'
    r'|(?P<string>(?:u8|u|U|L)?"(?:\\.|[^"\\])*")'
    r"|(?P<char>(?:u8|u|U|L)?'(?:\\.|[^'\\])*')"
    r'|(?P<comment>//[^\n]*|/\*.*?\*/)'
    r'|(?P<space>\s+)'
    r'|(?P<word>[a-zA-Z_][a-zA-Z_0-9]*|[0-9]+(?:\.[0-9]+)?)'
    r'|(?P<operator><<=|>>=|->\*|\.\.\.|::|->|\+\+|--|<<|>>|<=|>=|==|!=|&&|\|\||\+=|-=|\*=|/=|%=|&=|\|=|\^=|##|.)',
    re.DOTALL,
)


def cpp_tokens(text):
    text = re.sub(r'(#include[ \t]+")(?:(?:\.\./)|(?:\./))([^"/]+)(")',
                  r'\1\2\3', text)
    return [m.group() for m in TOKEN.finditer(text)
            if m.lastgroup not in ('comment', 'space')]


def expected_python(text, name):
    # Only executable-location expressions are allowed to differ in existing tests.
    if name in ('test_atlaskv_final.py', 'test_atlaskv_raft.py'):
        text = text.replace("default='./raft_server'",
                            "default=str(Path(__file__).resolve().parents[1] / 'build/bin/raft_server')")
    if name == 'test_binary_replication.py':
        text = text.replace("default='./atlas_server'",
                            "default=str(pathlib.Path(__file__).resolve().parents[1] / 'build/bin/atlas_server')")
    if name == 'test_phase6.py':
        text = text.replace("SERVER = ROOT / 'atlas_server'",
                            "SERVER = Path(__file__).resolve().parents[1] / 'build/bin/atlas_server'")
        text = text.replace('Build ./atlas_server first, then run from its project directory',
                            'Build build/bin/atlas_server first, then run from a disposable working directory')
    return text


def python_ast(text):
    tree = ast.parse(text)
    if (tree.body and isinstance(tree.body[0], ast.Expr)
            and isinstance(tree.body[0].value, ast.Constant)
            and isinstance(tree.body[0].value.value, str)):
        tree.body.pop(0)  # Usage documentation may refer to the new locations.
    return ast.dump(tree, include_attributes=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('baseline', type=Path)
    args = parser.parse_args()
    moves = json.loads((ROOT / 'docs/file-moves.json').read_text())
    manifest = json.loads((args.baseline / 'baseline-manifest.json').read_text())
    counts = {'cpp': 0, 'python': 0, 'unchanged_files': 0}
    for old, expected_hash in manifest.items():
        new = old
        for before, after in moves.items():
            if old == before or old.startswith(before + '/'):
                new = after + old[len(before):]
                break
        original = args.baseline / old
        current = ROOT / new
        if not current.is_file():
            raise AssertionError(f'Missing original file: {old} -> {new}')
        if original.suffix in ('.cpp', '.h'):
            if cpp_tokens(original.read_text()) != cpp_tokens(current.read_text()):
                raise AssertionError(f'C++ tokens changed: {old} -> {new}')
            counts['cpp'] += 1
        elif original.suffix == '.py':
            expected = expected_python(original.read_text(), original.name)
            if python_ast(expected) != python_ast(current.read_text()):
                raise AssertionError(f'Python logic changed: {old} -> {new}')
            counts['python'] += 1
        elif original.suffix != '.md' and not old.startswith('.vscode/'):
            if hashlib.sha256(current.read_bytes()).hexdigest() != expected_hash:
                raise AssertionError(f'Original data/artifact changed: {old} -> {new}')
            counts['unchanged_files'] += 1
    print(json.dumps(counts, indent=2))
    print('PASS: Original files retained; C++ tokens unchanged except include paths; '
          'Python test logic unchanged except executable paths; data hashes match.')


if __name__ == '__main__':
    main()
