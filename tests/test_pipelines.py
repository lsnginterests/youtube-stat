import ast
from pathlib import Path

import pytest


PIPELINES_DIR = Path(__file__).resolve().parents[1] / 'dataplatform' / 'pipelines'
PIPELINE_FILES = sorted(PIPELINES_DIR.rglob('load_*.py'))


def parse_run(path: Path) -> tuple[ast.Module, ast.FunctionDef]:
    tree = ast.parse(path.read_text(encoding='utf-8'))
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
    assert [function.name for function in functions] == ['run']
    return tree, functions[0]


def calls_slicer(node: ast.AST, method: str) -> bool:
    return any(
        isinstance(call.func, ast.Attribute)
        and call.func.attr == method
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == 'slicer'
        for call in ast.walk(node)
        if isinstance(call, ast.Call)
    )


def commit_statements(node: ast.AST) -> list[ast.Expr]:
    return [
        statement for statement in ast.walk(node)
        if isinstance(statement, ast.Expr)
        and isinstance(statement.value, ast.Call)
        and isinstance(statement.value.func, ast.Attribute)
        and statement.value.func.attr == 'commit'
        and isinstance(statement.value.func.value, ast.Name)
        and statement.value.func.value.id == 'slicer'
    ]


def test_every_pipeline_is_collected():
    assert len(PIPELINE_FILES) == 17


@pytest.mark.parametrize('path', PIPELINE_FILES, ids=lambda path: path.stem)
def test_module_holds_nothing_but_run(path: Path):
    tree, _ = parse_run(path)
    stray = [
        type(node).__name__ for node in tree.body
        if not isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef))
    ]
    assert not stray, f'module level holds more than imports and run: {stray}'


@pytest.mark.parametrize('path', PIPELINE_FILES, ids=lambda path: path.stem)
def test_slicer_run_implies_commit(path: Path):
    _, run = parse_run(path)
    if calls_slicer(run, 'run'):
        assert commit_statements(run), 'slice is read but slicer.commit() is never called'


@pytest.mark.parametrize('path', PIPELINE_FILES, ids=lambda path: path.stem)
def test_commit_is_not_nested(path: Path):
    _, run = parse_run(path)
    for statement in commit_statements(run):
        assert any(statement is child for child in run.body), (
            'slicer.commit() is nested in a condition: the slice is read, '
            'but the watermark stays put when there is nothing to load'
        )


@pytest.mark.parametrize('path', PIPELINE_FILES, ids=lambda path: path.stem)
def test_commit_closes_the_pipeline(path: Path):
    _, run = parse_run(path)
    commits = commit_statements(run)
    if commits:
        assert run.body[-1] is commits[-1], 'slicer.commit() must be the last statement of run()'
