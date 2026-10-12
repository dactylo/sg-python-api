import tomllib
from pathlib import Path


def test_project_metadata_separates_dev_group_from_public_metadata() -> None:
    pyproject_path = Path(__file__).parents[1] / 'pyproject.toml'
    with pyproject_path.open('rb') as pyproject_file:
        pyproject = tomllib.load(pyproject_file)

    project = pyproject['project']
    assert project['optional-dependencies'] == {'async': ['httpx2>=2,<3']}
    groups = pyproject['dependency-groups']
    assert {'include-group': 'test'} in groups['dev']
    assert 'pytest' in groups['test']


def test_sdist_uses_public_file_allowlist() -> None:
    pyproject_path = Path(__file__).parents[1] / 'pyproject.toml'
    with pyproject_path.open('rb') as pyproject_file:
        pyproject = tomllib.load(pyproject_file)

    sdist = pyproject['tool']['hatch']['build']['targets']['sdist']

    assert 'exclude' not in sdist
    assert sdist['include'] == [
        '/README.md',
        '/COMPATIBILITY.md',
        '/docs/*.md',
        '/examples/**',
        '/Makefile',
        '/scripts/**',
        '/src/**',
        '/tests/**',
    ]
