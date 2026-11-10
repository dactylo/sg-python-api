from __future__ import annotations

import ast
import re
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from tests._live_safety import LiveBudget
from tests._live_vocabulary import SESSION_NAMES
from tests.live._session import session_nodes
from tests.live.conftest import new_run_namespace
from tests.tooling._session_process import ROOT, environment

if TYPE_CHECKING:
    from pathlib import Path


# Record variables below come from the named functions' case.create calls and
# tuple loops. Keep this finite data-flow audit explicit; unknown expressions fail.
_RECORD_TYPES = {
    ('lifecycle_rows', "record['type']"): ('Shot', 'Asset', 'Sequence'),
    ('paired_assets_and_shots', "source['type']"): ('Shot', 'Asset'),
    ('inverse_holders_hide_retired_members', "member['type']"): (
        'Shot',
        'Task',
        'Note',
        'PublishedFile',
        'CutItem',
        'Playlist',
    ),
    ('activity_silent_field_changes', "record['type']"): ('Shot', 'Task'),
}


def _loop_expressions(name: str, function: ast.AST) -> tuple[ast.expr, ...]:
    for node in ast.walk(function):
        if not isinstance(node, (ast.For, ast.comprehension)) or not isinstance(
            node.iter, (ast.Tuple, ast.List)
        ):
            continue
        if isinstance(node.target, ast.Name) and node.target.id == name:
            return tuple(node.iter.elts)
        if isinstance(node.target, ast.Tuple):
            for index, target in enumerate(node.target.elts):
                if isinstance(target, ast.Name) and target.id == name:
                    values: list[ast.expr] = []
                    for row in node.iter.elts:
                        assert isinstance(row, ast.Tuple), ast.unparse(row)
                        values.append(row.elts[index])
                    return tuple(values)
    return ()


def _static_values(
    expression: ast.expr,
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
) -> tuple[str, ...]:
    if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
        return (expression.value,)
    if isinstance(expression, ast.Name):
        expressions = _loop_expressions(expression.id, function)
        if expressions:
            return tuple(
                value
                for item in expressions
                for value in _static_values(item, function, functions)
            )
        parameters = [argument.arg for argument in function.args.args]
        if expression.id in parameters:
            index = parameters.index(expression.id)
            values: set[str] = set()
            for caller in functions.values():
                for node in ast.walk(caller):
                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id == function.name
                    ):
                        values.update(_static_values(node.args[index], caller, functions))
            assert values, (function.name, expression.id)
            return tuple(values)
    key = function.name, ast.unparse(expression)
    assert key in _RECORD_TYPES, f'Uncovered parity call expression: {key}'
    return _RECORD_TYPES[key]


def test_live_namespace_is_short_and_unique() -> None:
    first, second = new_run_namespace(), new_run_namespace()
    assert re.fullmatch(r'sg-python-api-live-[0-9a-f]{12}', first)
    assert len(first) == 31
    assert first != second


def test_no_settings_live_run_skips_every_case(tmp_path: Path) -> None:

    result = subprocess.run(
        [
            sys.executable,
            '-m',
            'pytest',
            'tests/live',
            '-q',
            '-o',
            'addopts=',
            '-p',
            'anyio.pytest_plugin',
            '-p',
            'tests.tooling._live_standin',
        ],
        cwd=ROOT,
        env=environment(tmp_path),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout
    assert 'skipped' in result.stdout
    assert ' passed' not in result.stdout
    assert ' failed' not in result.stdout


@pytest.mark.parametrize('session', [name for name in SESSION_NAMES if session_nodes(name)])
def test_scenario_and_helper_calls_are_authorized(session: str) -> None:
    tree = ast.parse((ROOT / 'tests/_parity.py').read_text())
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    pending = [node.split('[', 1)[1].split('-', 1)[1][:-1] for node in session_nodes(session)]
    visited: set[str] = set()
    budget = LiveBudget(session=session)
    local = {'set_session_uuid', 'add_user_agent', 'reset_user_agent', 'close'}
    while pending:
        name = pending.pop()
        if name in visited:
            continue
        visited.add(name)
        function = functions[name]
        for node in ast.walk(function):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name) and node.func.id in functions:
                pending.append(node.func.id)
            if not (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == 'case'
                and node.func.attr in {'call', 'sudo_call', 'create'}
            ):
                continue
            methods = (
                ('create',)
                if node.func.attr == 'create'
                else _static_values(node.args[0], function, functions)
            )
            for method in methods:
                if method in local:
                    continue
                index = 0 if node.func.attr == 'create' else 1
                entities = (
                    _static_values(node.args[index], function, functions)
                    if method in {'create', 'update', 'delete', 'revive'}
                    else ('',)
                )
                for entity in entities:
                    budget.authorize(method, entity)


@pytest.mark.parametrize(
    ('session', 'count'),
    [
        ('C', 3),
        ('L1', 43),
        ('L2', 47),
        ('L2b', 13),
        ('L3', 7),
        ('L4', 8),
        ('R1', 16),
        ('R2', 4),
        ('R3', 1),
        ('R4', 1),
    ],
)
def test_session_registration_order_and_counts(session: str, count: int) -> None:
    nodes = session_nodes(session)
    assert len(nodes) == count
    assert len(set(nodes)) == count
    assert all(('async-' if session in {'L3', 'R2'} else 'sync-') in node for node in nodes)


@pytest.mark.parametrize('name', [name for name in SESSION_NAMES if session_nodes(name)])
def test_real_collection_matches_session_table(tmp_path: Path, name: str) -> None:

    expected = session_nodes(name)
    args = [
        sys.executable,
        '-m',
        'pytest',
        '--collect-only',
        '-q',
        '-o',
        'addopts=',
        '-p',
        'anyio.pytest_plugin',
        '-p',
        'tests.tooling._live_standin',
        *expected,
    ]
    if name in {'L4', 'R3', 'R4'}:
        args.append('--live-extended')
    result = subprocess.run(
        args, cwd=ROOT, env=environment(tmp_path), capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout
    assert [
        line for line in result.stdout.splitlines() if line.startswith('tests/live/')
    ] == expected
