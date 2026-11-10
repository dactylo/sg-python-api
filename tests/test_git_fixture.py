import os
import subprocess
from pathlib import Path

import pytest

from tests._git import commit_fixture, git_env


def test_fixture_commit_ignores_poisoned_global_git_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / 'home'
    home.mkdir()
    hooks = tmp_path / 'hooks'
    hooks.mkdir()
    called = tmp_path / 'hook-called'
    hook = hooks / 'pre-commit'
    hook.write_text(f'#!/bin/sh\ntouch "{called}"\nexit 1\n')
    hook.chmod(0o755)
    (home / '.gitconfig').write_text(
        '[user]\n\tname = poisoned\n\temail = poisoned@example.invalid\n'
        f'[core]\n\thooksPath = {hooks}\n'
        '[commit]\n\tgpgSign = true\n'
    )
    monkeypatch.setenv('HOME', str(home))
    monkeypatch.setenv('GIT_CONFIG_GLOBAL', str(home / '.gitconfig'))
    monkeypatch.setenv('GIT_CONFIG_NOSYSTEM', '0')
    repository = tmp_path / 'repository'
    repository.mkdir()
    environment = git_env()
    assert environment['GIT_CONFIG_GLOBAL'] == os.devnull
    assert environment['GIT_CONFIG_NOSYSTEM'] == '1'
    subprocess.run(['git', 'init', '--quiet', str(repository)], env=environment, check=True)
    subprocess.run(
        ['git', '-C', str(repository), 'config', 'fixture.value', 'local'],
        env=environment,
        check=True,
    )
    (repository / 'item').write_text('fixture\n')
    subprocess.run(['git', '-C', str(repository), 'add', 'item'], env=environment, check=True)

    commit_fixture(repository, 'fixture', identity=('Fixture', 'fixture@example.invalid'))

    author = subprocess.check_output(
        ['git', '-C', str(repository), 'log', '-1', '--format=%an <%ae>'],
        env=environment,
        text=True,
    ).strip()
    local_value = subprocess.check_output(
        ['git', '-C', str(repository), 'config', '--local', 'fixture.value'],
        env=environment,
        text=True,
    ).strip()
    assert author == 'Fixture <fixture@example.invalid>'
    assert local_value == 'local'
    assert not called.exists()
