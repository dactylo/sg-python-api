from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from tests._live_vocabulary import PROBE_KEYS, SESSION_NAMES

if TYPE_CHECKING:
    from collections.abc import Collection

_BASELINES = {'equal', 'restored', 'UNVERIFIED', 'different'}
_COUNTERS = {
    'requests',
    'upload_bytes',
    'human_authentications',
    'subscription_creates',
    'schema_creates',
    'uploads',
    'project_creates',
    'schedule_writes',
    'follows',
    'unfollows',
    'sudo_creates',
    'last_accessed_updates',
    'waits',
}
_EXCEPTIONS = {
    'Fault',
    'AuthenticationFault',
    'ShotgunError',
    'ShotgunFileDownloadError',
    'ShotgunThumbnailNotReady',
    'UserCredentialsNotAllowedForOxygenAuthenticationFault',
    'UserCredentialsNotAllowedForSSOAuthenticationFault',
    'ProtocolError',
    'MissingTwoFactorAuthenticationFault',
    'MockgunError',
    'AssertionError',
    'Failed',
    'LiveStop',
    'LiveStopError',
    'RuntimeError',
    'OSError',
    'TimeoutError',
    'KeyboardInterrupt',
    'SystemExit',
    'OtherException',
}
_PHASES = {'setup', 'call', 'teardown'}
_SCHEMA_WORDS = """
error_type template failure_locations failures passed failed skipped
started_utc finished_utc tests counts total_counts requests_by_test stop_reason
cleanup probe session collected selected deselected executed stop_template baselines
prefix_before prefix_after privacy_hits cleanup_verified error
schedule-overlap F S W P keys types unexpected-key str int float bool dict list null other
status reads equal streams sudo_metadata Settled Unsettled type_value type_length
name_equal id_equal content_type_equal link_type_equal host path_has_id url name content_type
link_type type id Attachment attachment Upload upload File file Note none site storage
creator type_equal status_equal status_null image_null human_image_null status_value meta
sudo sudo_type_equal sudo_id_equal attempt image entity_type entity_id sudo_actual_user
valid uuid act dis clean rpc entity upload follow archive schedule sudo collection baseline
reserve true false
""" + ' '.join((*SESSION_NAMES, *PROBE_KEYS))
_STOPS = (
    {
        '',
        'live interrupted',
        'live stop after HTTP 429',
        'live stop after HTTP 503',
        'live cleanup or restoration failed',
        'live cleanup budget exhausted',
    }
    | {f'live budget exhausted: {key}' for key in (*_COUNTERS, 'reserve')}
    | {
        f'refused: {key}'
        for key in (
            'rpc',
            'entity',
            'upload',
            'follow',
            'archive',
            'schedule',
            'sudo',
            'collection',
            'baseline',
        )
    }
)
_TEMPLATES = {
    'assert',
    'match-mismatch',
    'did-not-raise',
    'ready-timeout',
    'fault',
    'auth-fault',
    'shotgun-error',
    'download-error',
    'protocol-error',
    'refused',
    'cleanup-failed',
    'unmatched',
    'budget',
    'http-429',
    'http-503',
    'interrupted',
    'collection',
    'privacy',
    'baseline',
    '',
}


def _boolean(value: object) -> None:
    if type(value) is not bool:
        raise ValueError('invalid evidence boolean')


def _count(value: object) -> None:
    if type(value) is not int or value < 0:
        raise ValueError('invalid evidence count')


def _label(value: object, labels: Collection[str]) -> None:
    if not isinstance(value, str) or value not in labels:
        raise ValueError('invalid evidence label')


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not all(
        isinstance(key, str) for key in cast('dict[object, object]', value)
    ):
        raise ValueError('invalid evidence mapping')
    return cast('dict[str, object]', value)


def _counts(value: object, keys: Collection[str]) -> None:
    for key, count in _mapping(value).items():
        if key not in keys:
            raise ValueError('invalid evidence counter')
        _count(count)


def _keys(value: object, keys: set[str]) -> dict[str, object]:
    result = _mapping(value)
    if set(result) != keys:
        raise ValueError('invalid evidence fields')
    return result


def _failure(value: object) -> None:
    row = _keys(value, {'error_type', 'template', 'failure_locations'})
    _label(row['error_type'], _EXCEPTIONS)
    _label(row['template'], _TEMPLATES)
    locations = row['failure_locations']
    if not isinstance(locations, list):
        raise TypeError('invalid evidence locations')
    for entry in cast('list[object]', locations):
        if not isinstance(entry, str) or not re.fullmatch(
            r'tests/[A-Za-z0-9_/]+\.py:[0-9]+', entry
        ):
            raise ValueError('invalid evidence location')


def _nodes(value: object, collected: Collection[str]) -> None:
    if not isinstance(value, list):
        raise TypeError('invalid evidence nodes')
    for node in cast('list[object]', value):
        _label(node, collected)


def _outcomes(value: object, collected: Collection[str]) -> None:
    for node, phases in _mapping(value).items():
        _label(node, collected)
        for phase, outcome in _mapping(phases).items():
            if phase == 'failures':
                for when, detail in _mapping(outcome).items():
                    _label(when, _PHASES)
                    _failure(detail)
            else:
                _label(phase, _PHASES)
                _label(outcome, {'passed', 'failed', 'skipped'})


def _shape(value: object, allowed: set[str]) -> None:
    row = _keys(value, {'keys', 'types'})
    _nodes(row['keys'], allowed | {'unexpected-key'})
    _nodes(row['types'], {'str', 'int', 'float', 'bool', 'dict', 'list', 'null', 'other'})


def _probe(value: object) -> None:
    first_key, second_key = PROBE_KEYS
    root = _keys(value, set(PROBE_KEYS))
    first = _keys(root[first_key], {'status', 'error', 'reads', 'equal'})
    second = _keys(root[second_key], {'status', 'error', 'streams', 'sudo_metadata'})
    for row in (first, second):
        _label(row['status'], {'Settled', 'Unsettled'})
        _label(row['error'], _EXCEPTIONS | {''})
    _boolean(first['equal'])
    _boolean(second['sudo_metadata'])
    if not isinstance(first['reads'], list) or not isinstance(second['streams'], list):
        raise TypeError('invalid evidence probe observations')
    for item in cast('list[object]', first['reads']):
        observation = _keys(
            item,
            {
                'keys',
                'types',
                'type_value',
                'type_length',
                'name_equal',
                'id_equal',
                'content_type_equal',
                'link_type_equal',
                'host',
                'path_has_id',
            },
        )
        _shape(
            {key: observation[key] for key in ('keys', 'types')},
            {'url', 'name', 'content_type', 'link_type', 'type', 'id'},
        )
        _label(
            observation['type_value'],
            {'Attachment', 'attachment', 'Upload', 'upload', 'File', 'file', 'Note', '', 'none'},
        )
        _label(observation['host'], {'site', 'storage', 'other'})
        _count(observation['type_length'])
        for key in (
            'name_equal',
            'id_equal',
            'content_type_equal',
            'link_type_equal',
            'path_has_id',
        ):
            _boolean(observation[key])
    for item in cast('list[object]', second['streams']):
        observation = _keys(
            item,
            {
                'creator',
                'type_equal',
                'id_equal',
                'name_equal',
                'status_equal',
                'status_null',
                'image_null',
                'human_image_null',
                'status_value',
                'meta',
                'sudo',
                'sudo_type_equal',
                'sudo_id_equal',
                'attempt',
            },
        )
        _shape(observation['creator'], {'type', 'id', 'name', 'status', 'image'})
        _shape(observation['meta'], {'type', 'entity_type', 'entity_id', 'sudo_actual_user'})
        _shape(observation['sudo'], {'type', 'id', 'name', 'valid', 'uuid'})
        _label(observation['status_value'], {'act', 'dis', 'other'})
        _count(observation['attempt'])
        for key in (
            'type_equal',
            'id_equal',
            'name_equal',
            'status_equal',
            'status_null',
            'image_null',
            'human_image_null',
            'sudo_type_equal',
            'sudo_id_equal',
        ):
            _boolean(observation[key])


def _stop(value: object) -> None:
    _label(value, _STOPS)


def _timestamp(value: object) -> None:
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T[0-9:.]+\+00:00', value):
        raise ValueError('invalid evidence timestamp')
    datetime.fromisoformat(value)


def _session(value: object, collected: Collection[str]) -> None:
    row = _keys(
        value,
        {
            'session',
            'started_utc',
            'finished_utc',
            'collected',
            'selected',
            'deselected',
            'executed',
            'tests',
            'counts',
            'requests_by_test',
            'stop_template',
            'baselines',
            'prefix_before',
            'prefix_after',
            'privacy_hits',
            'cleanup_verified',
            'error',
        },
    )
    _label(row['session'], set(SESSION_NAMES))
    _timestamp(row['started_utc'])
    _timestamp(row['finished_utc'])
    for key in ('collected', 'selected', 'executed'):
        _nodes(row[key], collected)
    for node, reason in _mapping(row['deselected']).items():
        _label(node, collected)
        _label(reason, {'schedule-overlap'})
    _outcomes(row['tests'], collected)
    _counts(row['counts'], _COUNTERS)
    _counts(row['requests_by_test'], {*collected, 'setup', 'cleanup', 'probe'})
    _label(row['stop_template'], _TEMPLATES)
    for value_label in _keys(row['baselines'], {'F', 'S', 'W', 'P'}).values():
        _label(value_label, _BASELINES)
    for key in ('prefix_before', 'prefix_after', 'privacy_hits'):
        _count(row[key])
    if type(row['cleanup_verified']) is not bool:
        raise ValueError('invalid evidence boolean')
    _label(row['error'], _EXCEPTIONS | {''})


def _standard(value: object, collected: Collection[str]) -> None:
    row = _keys(
        value,
        {
            'started_utc',
            'finished_utc',
            'tests',
            'counts',
            'total_counts',
            'requests_by_test',
            'stop_reason',
        },
    )
    _timestamp(row['started_utc'])
    _timestamp(row['finished_utc'])
    _outcomes(row['tests'], collected)
    _counts(row['counts'], _COUNTERS)
    _counts(row['total_counts'], _COUNTERS)
    _counts(row['requests_by_test'], {*collected, 'setup', 'cleanup', 'probe'})
    _stop(row['stop_reason'])


def _validate_artifact(name: str, value: object, collected: Collection[str]) -> None:
    if name == 'budget.json':
        _counts(value, _COUNTERS)
    elif name in {'default.json', 'extended.json'}:
        _standard(value, collected)
    elif name == 'probe.json':
        _probe(value)
    elif name == 'session.json':
        _session(value, collected)
    else:
        raise ValueError('invalid evidence artifact')


def digest_bytes(value: object) -> bytes:
    row = _keys(value, {'session', 'clean', 'safe', 'mismatches', 'requests'})
    _label(row['session'], set(SESSION_NAMES))
    _boolean(row['clean'])
    _boolean(row['safe'])
    _count(row['mismatches'])
    _count(row['requests'])
    return (json.dumps(row) + '\n').encode('ascii')


def exception_label(error: BaseException) -> str:
    name = type(error).__name__
    return name if name in _EXCEPTIONS else 'OtherException'


def location(path: Path, line: int) -> str | None:
    root = Path(__file__).resolve().parents[2]
    try:
        relative = path.resolve().relative_to(root).as_posix()
    except ValueError:
        return None
    if re.fullmatch(r'tests/[A-Za-z0-9_/]+\.py', relative):
        return f'{relative}:{line}'
    return None


def privacy_hits(directory: Path, secrets: Collection[str], collected: Collection[str]) -> int:
    reference = '\n'.join(
        (
            *collected,
            *_BASELINES,
            *_COUNTERS,
            *_EXCEPTIONS,
            *_PHASES,
            *_STOPS,
            *_TEMPLATES,
            _SCHEMA_WORDS,
        )
    ).casefold()
    needles = [
        value.casefold()
        for value in secrets
        if any(character.isalpha() for character in value) and value.casefold() not in reference
    ]
    needles.extend(('@', '://', 'cookie', 'token'))
    hits = 0
    for name in ('default.json', 'extended.json', 'budget.json', 'session.json', 'probe.json'):
        path = directory / name
        if path.exists():
            content = path.read_text(encoding='ascii').casefold()
            hits += sum(content.count(needle) for needle in needles)
    return hits


def template(error: BaseException) -> str:
    message = str(error)
    for prefix, label in (
        ('live interrupted', 'interrupted'),
        ('refused:', 'refused'),
        ('live budget exhausted:', 'budget'),
        ('live cleanup', 'cleanup-failed'),
        ('live stop after HTTP 429', 'http-429'),
        ('live stop after HTTP 503', 'http-503'),
        ('DID NOT RAISE', 'did-not-raise'),
        ('Regex pattern did not match', 'match-mismatch'),
    ):
        if message.startswith(prefix):
            return label
    return {
        'AssertionError': 'assert',
        'AuthenticationFault': 'auth-fault',
        'Fault': 'fault',
        'ShotgunError': 'shotgun-error',
        'ShotgunFileDownloadError': 'download-error',
        'ProtocolError': 'protocol-error',
        'KeyboardInterrupt': 'interrupted',
    }.get(exception_label(error), 'unmatched')


def failure(error: BaseException, locations: list[str]) -> dict[str, object]:
    return {
        'error_type': exception_label(error),
        'template': template(error),
        'failure_locations': locations,
    }


def write_artifact(path: Path, value: object, collected: Collection[str]) -> None:
    _validate_artifact(path.name, value, collected)
    payload = (json.dumps(value, indent=2, ensure_ascii=True) + '\n').encode('ascii')
    descriptor, temporary = tempfile.mkstemp(prefix='.evidence-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        Path(temporary).replace(path)
    finally:
        Path(temporary).unlink(missing_ok=True)
