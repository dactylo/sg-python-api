#!/usr/bin/env python
"""Validate preserved artifact evidence for maintainer workflows."""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import sys
import zipfile
from email.parser import BytesParser
from email.policy import default
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

_ARGUMENT_COUNT = 2
# Keep the public input scope aligned with validate_artifacts.sh.
_ARTIFACT_INPUTS = (
    ':(top,glob)*',
    ':(top,glob).github/**',
    ':(top,glob)docs/*.md',
    ':(top,glob)examples/**',
    ':(top,glob)scripts/**',
    ':(top,glob)src/**',
    ':(top,glob)tests/**',
)
_EVIDENCE_PATH = 'dist/artifact-results.md'


class _EvidenceError(Exception):
    """Reject evidence with a sanitized diagnostic."""


def _artifact_path(
    repo_root: Path,
    source_name: str,
    preserved_name: str,
    expected_hash: str,
) -> tuple[Path, str]:
    """Resolve the trusted wheel path and prove its digest."""
    relative = PurePosixPath(preserved_name)
    if relative.is_absolute() or '..' in relative.parts:
        raise _EvidenceError('Unsafe preserved source wheel path in artifact validation evidence')
    if relative.name != source_name:
        raise _EvidenceError('Preserved source wheel does not match artifact validation evidence')
    trusted_root = (repo_root / 'dist' / 'validated-artifacts' / 'source').resolve()
    try:
        wheel_path = (repo_root / Path(*relative.parts)).resolve(strict=True)
    except OSError as error:
        raise _EvidenceError('Validated source wheel is unavailable') from error
    if not wheel_path.is_relative_to(trusted_root):
        raise _EvidenceError('Preserved source wheel is outside the validated source directory')
    try:
        digest = hashlib.sha256(wheel_path.read_bytes()).hexdigest()
    except OSError:
        raise _EvidenceError('Cannot read validated source wheel') from None
    if digest != expected_hash:
        raise _EvidenceError('Artifact SHA-256 does not match validation evidence')
    return wheel_path, digest


def _artifact_version(wheel_path: Path) -> str:
    """Return the validated project version from one wheel."""
    try:
        with zipfile.ZipFile(wheel_path) as archive:
            metadata_names = [
                name for name in archive.namelist() if name.endswith('.dist-info/METADATA')
            ]
            if len(metadata_names) != 1:
                raise _EvidenceError('Validated source wheel has invalid metadata contents')
            metadata = BytesParser(policy=default).parsebytes(archive.read(metadata_names[0]))
    except (OSError, KeyError, zipfile.BadZipFile) as error:
        raise _EvidenceError('Cannot inspect validated source wheel metadata') from error
    if metadata['Name'] != 'sg-python-api':
        raise _EvidenceError('Validated source wheel has the wrong project name')
    version = metadata['Version']
    if version is None or re.fullmatch(r'[^|\s]+', version) is None:
        raise _EvidenceError('Validated source wheel has an invalid version')
    return version


def _field(text: str, label: str) -> str:
    """Extract one exact backtick-delimited evidence field."""
    matches = re.findall(rf'^- {re.escape(label)}: `([^`\r\n]+)`$', text, re.MULTILINE)
    if len(matches) != 1:
        raise _EvidenceError(f'Invalid artifact validation evidence field: {label}')
    return matches[0]


def _artifact_fields(repo_root: Path) -> tuple[str, str, str, str]:
    """Read and validate scalar artifact evidence fields."""
    evidence_path = repo_root / _EVIDENCE_PATH
    try:
        text = evidence_path.read_text(encoding='ascii')
    except (OSError, UnicodeError) as error:
        raise _EvidenceError('Cannot read artifact validation evidence') from error
    if not re.search(r'^- Status: PASS$', text, re.MULTILINE):
        raise _EvidenceError('Artifact validation evidence does not have PASS status')
    candidate_commit = _field(text, 'Candidate commit')
    expected_hash = _field(text, 'Source wheel SHA256')
    if re.fullmatch(r'[0-9a-f]{40}', candidate_commit) is None:
        raise _EvidenceError('Invalid candidate commit in artifact validation evidence')
    if re.fullmatch(r'[0-9a-f]{64}', expected_hash) is None:
        raise _EvidenceError('Invalid source wheel SHA-256 in artifact validation evidence')
    return (
        candidate_commit,
        _field(text, 'Source wheel'),
        expected_hash,
        _field(text, 'Preserved source wheel'),
    )


def _git_status(
    repo_root: Path,
    *arguments: str,
    run: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
) -> int:
    """Run one Git check without exposing repository diagnostics."""
    git = shutil.which('git')
    if git is None:
        raise _EvidenceError('Could not run artifact validation Git check')
    try:
        return run(
            [str(Path(git).resolve()), '-C', str(repo_root), *arguments],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
    except OSError:
        raise _EvidenceError('Could not run artifact validation Git check') from None


def _verify_freshness(repo_root: Path, candidate_commit: str) -> None:
    """Require an available ancestor candidate with unchanged tracked public inputs."""
    checks = (
        (
            ('rev-parse', '--verify', f'{candidate_commit}^{{commit}}'),
            'Candidate commit is unavailable in the repository',
        ),
        (
            ('merge-base', '--is-ancestor', candidate_commit, 'HEAD'),
            'Candidate commit is not an ancestor of the current checkout',
        ),
    )
    for arguments, message in checks:
        if _git_status(repo_root, *arguments) != 0:
            raise _EvidenceError(message)
    source_statuses = (
        _git_status(
            repo_root,
            'diff',
            '--quiet',
            '--exit-code',
            candidate_commit,
            '--',
            *_ARTIFACT_INPUTS,
        ),
        _git_status(
            repo_root,
            'diff',
            '--cached',
            '--quiet',
            '--exit-code',
            candidate_commit,
            '--',
            *_ARTIFACT_INPUTS,
        ),
    )
    if any(status not in (0, 1) for status in source_statuses):
        raise _EvidenceError('Could not verify artifact validation freshness')
    if any(source_statuses):
        raise _EvidenceError('Artifact validation evidence is stale')


def _load_evidence(repo_root: Path) -> tuple[str, str, str, str]:
    """Return the commit, version, absolute wheel path, and verified digest."""
    repo_root = repo_root.resolve()
    commit, source_name, expected_hash, preserved_name = _artifact_fields(repo_root)
    wheel_path, digest = _artifact_path(repo_root, source_name, preserved_name, expected_hash)
    version = _artifact_version(wheel_path)
    _verify_freshness(repo_root, commit)
    return commit, version, str(wheel_path), digest


def _main() -> int:
    """Print the existing four-field evidence tuple or a sanitized error."""
    if len(sys.argv) != _ARGUMENT_COUNT:
        print('Usage: _artifact_evidence.py REPO_ROOT', file=sys.stderr)
        return 2
    try:
        fields = _load_evidence(Path(sys.argv[1]))
    except _EvidenceError as error:
        print(str(error), file=sys.stderr)
        return 1
    print('|'.join(fields))
    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
