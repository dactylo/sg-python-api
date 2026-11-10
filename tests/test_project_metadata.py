import re
import tomllib
from pathlib import Path


def test_changelog_heading_matches_version() -> None:
    repository = Path(__file__).parents[1]
    with (repository / 'pyproject.toml').open('rb') as pyproject_file:
        version = tomllib.load(pyproject_file)['project']['version']

    changelog = (repository / 'CHANGELOG.md').read_text(encoding='utf-8')
    newest_heading = re.search(
        r'^## (\d+\.\d+\.\d+) \(\d{4}-\d{2}-\d{2}\)$', changelog, re.MULTILINE
    )
    assert newest_heading is not None
    assert newest_heading.group(1) == version


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
        '/src',
    ]
