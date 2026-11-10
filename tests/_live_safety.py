from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, cast

from tests._live_vocabulary import SESSION_NAMES

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping

_COUNTERS = (
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
)
_FILE_RPCS = frozenset(
    {
        'upload',
        'upload_thumbnail',
        'upload_filmstrip_thumbnail',
        'share_thumbnail',
        'get_upload_url',
        'upload_file',
    }
)
_LIMITS = {
    'requests': 2500,
    'upload_bytes': 42_100_000,
    'human_authentications': 12,
    'subscription_creates': 2,
    'schema_creates': 2,
}
_READS = {
    'find',
    'find_one',
    'download_attachment',
    'get_attachment_download_url',
    'nav_expand',
    'info',
    'read',
    'summarize',
    'text_search',
    'schema_read',
    'schema_entity_read',
    'schema_field_read',
    'preferences_read',
    'following',
    'followers',
    'user_subscriptions_read',
    'work_schedule_read',
    'activity_stream_read',
    'note_thread_read',
    'nav_search_entity',
    'nav_search_string',
    'activity_stream',
    'note_thread_contents',
    'nav_search',
    'query_display_name_cache',
}
_RESERVES = {
    name: reserve
    for name, reserve in zip(
        SESSION_NAMES, (20, 100, 150, 150, 60, 80, 0, 80, 60, 20, 30, 0), strict=True
    )
    if reserve
}
_SESSION_RPCS: dict[str, set[str]] = dict(
    zip(
        SESSION_NAMES,
        (
            set(),
            set(),
            {'get_session_token', *_FILE_RPCS},
            {'get_session_token', *_FILE_RPCS},
            {'get_session_token', *_FILE_RPCS},
            {
                'follow',
                'unfollow',
                'work_schedule_update',
                'user_subscriptions_create',
                'update_project_last_accessed',
            },
            {'unfollow', *_FILE_RPCS},
            {'get_session_token', *_FILE_RPCS},
            {'get_session_token', *_FILE_RPCS},
            {'unfollow'},
            set(),
            set(),
        ),
        strict=True,
    )
)
_WRITABLE = {
    'Attachment',
    'Reply',
    'Task',
    'Version',
    'Asset',
    'Shot',
    'Sequence',
    'Cut',
    'CutItem',
    'PublishedFile',
    'Playlist',
    'Note',
    'Tag',
}


class LiveStop(BaseException):
    pass


# Raised in place of LiveStop inside a test: anyio's test runner cannot survive a
# BaseException, which would skip the fixture's recorded teardown.
class LiveStopError(LiveStop, Exception):
    pass


class LiveBudget:
    def __init__(
        self,
        counts: dict[str, int] | None = None,
        *,
        limits: Mapping[str, int] | None = None,
        cleanup_requests: int = 0,
        session: str = '',
    ) -> None:
        self._limits = dict(_LIMITS)
        for key, value in (limits or {}).items():
            if key not in _COUNTERS or value < 0 or value > _LIMITS.get(key, value):
                raise ValueError('limits must only lower the shipped profile')
            self._limits[key] = value
        self.counts: dict[str, int] = dict.fromkeys(_COUNTERS, 0)
        self.counts.update(counts or {})
        self.stopped = ''
        self._lock = threading.Lock()
        self.current_test = 'setup'
        self.requests_by_test: dict[str, int] = {}
        self.session = session
        self._cleanup_requests = cleanup_requests
        self.cleanup_count = 0
        self._cleaning = False
        self.cleanup_stopped = False
        self.retry_after = 0

    def _charge(self, increments: dict[str, int]) -> None:
        with self._lock:
            if self._cleaning and self.stopped:
                if (
                    self.cleanup_stopped
                    or self.cleanup_count + increments.get('requests', 0) > self._cleanup_requests
                ):
                    self.cleanup_stopped = True
                    raise LiveStop('live cleanup budget exhausted')
            elif self.stopped:
                raise LiveStop(self.stopped)
            else:
                for key, increment in increments.items():
                    if self.counts[key] + increment > self._limits.get(
                        key, self.counts[key] + increment
                    ):
                        self.stopped = f'live budget exhausted: {key}'
                        raise LiveStop(self.stopped)
            for key, increment in increments.items():
                self.counts[key] += increment
            requests = increments.get('requests', 0)
            if self._cleaning and self.stopped:
                self.cleanup_count += requests
            self.requests_by_test[self.current_test] = (
                self.requests_by_test.get(self.current_test, 0) + requests
            )

    def refuse(self, operation: str) -> None:
        self.stopped = f'refused: {operation}'
        raise LiveStop(self.stopped)

    def authorize(self, method: str, entity_type: str = '') -> None:
        if not self.session or method in _READS:
            return
        allowed = {'create', 'update', 'delete', 'revive', 'batch'}
        allowed.update(_SESSION_RPCS.get(self.session, ()))
        if self._cleaning:
            allowed.add('unfollow')
        if method not in allowed:
            self.refuse('rpc')
        if entity_type == 'WorkDayRule' and method != 'delete':
            self.refuse('entity')
        if method in {'create', 'update', 'delete', 'revive'}:
            writable = set(_WRITABLE)
            if self.session == 'L4' or self._cleaning:
                writable.update({'Project', 'WorkDayRule'})
            if self.session == 'R4' or self._cleaning:
                writable.update({'Department', 'Group', 'CustomEntity03', 'Ticket'})
            if entity_type not in writable:
                self.refuse('entity')

    def before(self, *, body: object = None, upload_bytes: int = 0) -> None:
        rpc = _rpc(body)
        method = str(rpc.get('method_name', ''))
        if method == 'update_project_last_accessed_by_current_user':
            method = 'update_project_last_accessed'
        operations = _operations(rpc)
        if method == 'batch':
            for operation in operations:
                self.authorize(
                    str(operation.get('request_type', '')), str(operation.get('type', ''))
                )
        if method:
            if operations and method in {'create', 'update', 'delete', 'revive'}:
                for operation in operations:
                    self.authorize(method, str(operation.get('type', '')))
            else:
                self.authorize(method)
        increments = {
            'requests': 1,
            'upload_bytes': upload_bytes,
            'uploads': int(upload_bytes > 0),
        }
        for operation, counter in (
            ('user_subscriptions_create', 'subscription_creates'),
            ('schema_field_create', 'schema_creates'),
            ('work_schedule_update', 'schedule_writes'),
            ('follow', 'follows'),
            ('unfollow', 'unfollows'),
            ('update_project_last_accessed', 'last_accessed_updates'),
        ):
            increments[counter] = int(method == operation)
        increments['project_creates'] = sum(
            (method == 'create' or part.get('request_type') == 'create')
            and part.get('type') == 'Project'
            for part in operations
        )
        params = rpc.get('params', [])
        if isinstance(params, list) and params and isinstance(params[0], dict):
            auth = cast('dict[str, object]', params[0])
            increments['human_authentications'] = int('user_password' in auth)
            increments['sudo_creates'] = sum(
                (method == 'create' or part.get('request_type') == 'create')
                and 'sudo_as_login' in auth
                for part in operations
            )
        if (
            upload_bytes
            and self.session
            and not _SESSION_RPCS.get(self.session, set()) >= _FILE_RPCS
        ):
            self.refuse('upload')
        self._charge(increments)

    @contextmanager
    def cleanup(self) -> Generator[None]:
        previous = self._cleaning
        self._cleaning = True
        try:
            yield
        finally:
            self._cleaning = previous

    def response(self, status: int, retry_after: str = '') -> None:
        if status in {429, 503}:
            with self._lock:
                in_allowance = self._cleaning and bool(self.stopped)
                self.stopped = f'live stop after HTTP {status}'
                self.retry_after = min(120, int(retry_after)) if retry_after.isdigit() else 60
                if in_allowance:
                    self.cleanup_stopped = True
            raise LiveStop(self.stopped)

    def start_case(self) -> None:
        if self.stopped:
            raise LiveStop(self.stopped)
        if self._limits['requests'] - self.counts['requests'] < _RESERVES.get(self.session, 0):
            self.stopped = 'live budget exhausted: reserve'
            raise LiveStop(self.stopped)

    def wait(self, seconds: int) -> None:
        self._charge({'waits': seconds})


def _operations(rpc: dict[str, object]) -> list[dict[str, object]]:
    params = rpc.get('params', [])
    if not isinstance(params, list):
        return []
    parts: list[object] = []
    for part in cast('list[object]', params):
        if isinstance(part, list):
            parts.extend(cast('list[object]', part))
        else:
            parts.append(part)
    return [
        cast('dict[str, object]', part)
        for part in parts
        if isinstance(part, dict) and ('type' in part or 'request_type' in part)
    ]


def _rpc(body: object) -> dict[str, object]:
    if isinstance(body, bytes) and body.startswith(b'{'):
        payload: object = json.loads(body)
        if isinstance(payload, dict):
            return cast('dict[str, object]', payload)
    return {}
