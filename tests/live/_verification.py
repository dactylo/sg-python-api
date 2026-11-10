from __future__ import annotations

import inspect
import os
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal, cast

from anyio import sleep

import shotgun_api3
from tests._live_safety import LiveBudget, LiveStop, LiveStopError

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

_NAMESPACE_FIELDS = {
    'Project': ('name', False),
    'WorkDayRule': ('description', True),
    'Attachment': ('filename', True),
    'Reply': ('content', False),
    'Task': ('content', True),
    'Version': ('code', True),
    'Asset': ('code', True),
    'Shot': ('code', True),
    'Sequence': ('code', True),
    'Cut': ('code', True),
    'CutItem': ('code', True),
    'PublishedFile': ('code', True),
    'Playlist': ('code', True),
    'Note': ('subject', True),
    'Tag': ('name', False),
    'Department': ('code', False),
    'Group': ('code', False),
    'CustomEntity03': ('code', True),
    'Ticket': ('title', True),
}


@dataclass
class LiveTarget:
    factory: Callable[..., object]
    budget: LiveBudget
    human: dict[str, object] = field(default_factory=dict[str, object])
    human_login: str = ''
    site_host: str = ''
    run_date: date = field(default_factory=date.today)
    baselines: dict[str, object] = field(default_factory=dict[str, object])
    advance: Callable[[int], object] | None = None
    project_name: str = ''


class LiveCase:
    def __init__(
        self,
        kind: Literal['sync', 'async'],
        project: dict[str, object],
        namespace: str,
        *,
        script_name: str = '',
        target: LiveTarget | None = None,
    ) -> None:
        self.kind = kind
        self.project = project
        self.namespace = namespace
        self.script_name = script_name
        self.target = target
        self.run_date = target.run_date if target is not None else None
        self.flags: set[str] = set()
        self._cleaning = False
        self.discovered: set[tuple[str, int]] = set()
        self.client = self.make_client()
        self.human_login = (
            target.human_login if target else os.environ.get('SG_TEST_HUMAN_LOGIN', '')
        )
        self._sudo_client: shotgun_api3.Shotgun | shotgun_api3.AsyncShotgun | None = None
        self._resources: list[tuple[str, int]] = []
        self.scanned_types: set[str] = set()

    async def _advance(self, seconds: int) -> None:
        if self.target is not None:
            self.target.budget.wait(seconds)
            if self.target.advance is not None:
                result = self.target.advance(seconds)
                if inspect.isawaitable(result):
                    await result
                return
        await sleep(seconds)

    async def _cool_off(self) -> None:
        if self.target is None:
            return
        budget = self.target.budget
        if not budget.retry_after or budget.cleanup_stopped:
            return
        seconds = budget.retry_after
        budget.retry_after = 0
        if self.target.advance:
            result = self.target.advance(seconds)
            if inspect.isawaitable(result):
                await result
        else:
            await sleep(seconds)
        budget.counts['waits'] += seconds

    def _guard_batch(self, args: tuple[object, ...], *, sudo: bool) -> None:
        for operation in cast('list[dict[str, object]]', args[0]):
            operation_type = str(operation.get('request_type', ''))
            operation_args = (
                operation.get('entity_type'),
                operation.get('data')
                if operation_type == 'create'
                else operation.get('entity_id'),
                operation.get('data'),
            )
            self._guard(operation_type, operation_args, {}, sudo=sudo)

    def _guard_follow(self, method: str, args: tuple[object, ...]) -> None:
        assert self.target is not None
        owned = set(self._resources) | (
            self.discovered if self._cleaning and method == 'unfollow' else set[tuple[str, int]]()
        )
        if (
            len(args) < 2
            or _identity(args[0]) != _identity(self.target.human)
            or _identity(args[1]) not in owned
        ):
            self.target.budget.refuse('follow')

    def _guard_project(self, method: str, args: tuple[object, ...]) -> None:
        assert self.target is not None
        self.flags.add('archive')
        if method == 'create':
            data = cast('dict[str, object]', args[1])
            valid = str(data.get('name', '')).startswith(self.namespace)
        else:
            valid = ('Project', cast('int', args[1])) in (
                set(self._resources)
                | (self.discovered if self._cleaning else set[tuple[str, int]]())
            ) and args[1] != self.project.get('id')
        if not valid:
            self.target.budget.refuse('archive')

    def _guard_rule(self, method: str, args: tuple[object, ...]) -> None:
        assert self.target is not None
        self.flags.add('schedule')
        if method != 'delete' or ('WorkDayRule', cast('int', args[1])) not in self.discovered:
            self.target.budget.refuse('schedule')

    def _guard_schedule(self, args: tuple[object, ...], kwargs: dict[str, object]) -> None:
        assert self.target is not None
        self.flags.add('schedule')
        self.scanned_types.add('WorkDayRule')
        valid = len(args) >= 3 and kwargs.get('project') == self.project and not kwargs.get('user')
        try:
            day = date.fromisoformat(str(next(iter(args), '')))
        except (ValueError, IndexError):
            valid = False
            day = self.target.run_date
        valid = valid and str(args[2]).startswith(self.namespace) if len(args) >= 3 else False
        valid = valid and day >= self.target.run_date + timedelta(days=30)
        rules = cast('list[dict[str, object]]', self.target.baselines.get('W', []))
        if not valid or any(_inside_rule(day, rule) for rule in rules):
            self.target.budget.refuse('schedule')

    def _guard_sudo(self, args: tuple[object, ...]) -> None:
        assert self.target is not None
        data = cast('dict[str, object]', args[1])
        note = (
            args[0] == 'Note'
            and str(data.get('subject', '')).startswith(self.namespace)
            and data.get('project') == self.project
        )
        reply = args[0] == 'Reply' and _identity(data.get('entity')) in {
            resource for resource in self._resources if resource[0] == 'Note'
        }
        if self.target.budget.session == 'L5':
            reply = False
        if not note and not reply:
            self.target.budget.refuse('sudo')

    def _guard(
        self,
        method: str,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        sudo: bool = False,
    ) -> None:
        if self.target is None or method in {
            'set_session_uuid',
            'add_user_agent',
            'reset_user_agent',
            'close',
        }:
            return
        budget = self.target.budget
        if method in {'follow', 'unfollow'} or (sudo and method == 'create'):
            self.flags.add('follow')
        if method == 'user_subscriptions_create':
            self.flags.add('subscription')
        if method == 'work_schedule_update':
            self._guard_schedule(args, kwargs)
        if args and args[0] == 'Project' and method in {'create', 'update', 'delete', 'revive'}:
            self._guard_project(method, args)
        if method in {'follow', 'unfollow'}:
            self._guard_follow(method, args)
        if sudo and method == 'create':
            self._guard_sudo(args)
        if method == 'batch' and args:
            self._guard_batch(args, sudo=sudo)
        if (
            args
            and args[0] == 'WorkDayRule'
            and method in {'create', 'update', 'delete', 'revive'}
        ):
            self._guard_rule(method, args)
        budget.authorize(method, str(args[0]) if args else '')

    async def advance(self, seconds: int) -> None:
        try:
            await self._advance(seconds)
        except LiveStop as stop:
            raise _in_test(stop) from None

    def make_client(self, **kwargs: object) -> shotgun_api3.Shotgun | shotgun_api3.AsyncShotgun:
        if self.target is not None:
            return cast(
                'shotgun_api3.Shotgun | shotgun_api3.AsyncShotgun', self.target.factory(**kwargs)
            )
        options: dict[str, object] = {'connect': False}
        if not any(name in kwargs for name in ('login', 'password', 'session_token')):
            options.update(
                script_name=os.environ['SG_TEST_SCRIPT'], api_key=os.environ['SG_TEST_KEY']
            )
        options.update(kwargs)
        constructor = cast(
            'Callable[..., shotgun_api3.Shotgun | shotgun_api3.AsyncShotgun]',
            shotgun_api3.Shotgun if self.kind == 'sync' else shotgun_api3.AsyncShotgun,
        )
        return constructor(os.environ['SG_TEST_SITE'], **options)

    def track(self, entity_type: str, entity_id: object) -> None:
        if not isinstance(entity_id, int) or isinstance(entity_id, bool):
            raise TypeError('created resource has no integer identity')
        self._resources.append((entity_type, entity_id))
        self.scanned_types.add(entity_type)

    def _record_result(self, method: str, args: tuple[object, ...], result: object) -> None:
        if method == 'create' and isinstance(result, dict):
            identity = _identity(cast('dict[str, object]', result))
            if identity is not None and identity not in self._resources:
                self.track(*identity)
        if method not in {'find', 'find_one'} or len(args) < 2 or not isinstance(args[1], list):
            return
        entity_type = str(args[0])
        namespace_field = _NAMESPACE_FIELDS.get(entity_type)
        if namespace_field is None:
            return
        owned_query = _owned_query(
            cast('list[object]', args[1]), namespace_field[0], self.namespace
        )
        if owned_query:
            rows: list[object] = (
                cast('list[object]', result) if isinstance(result, list) else [result]
            )
            for value in rows:
                if isinstance(value, dict):
                    row = cast('dict[str, object]', value)
                    if isinstance(row.get('id'), int):
                        self.discovered.add((entity_type, cast('int', row['id'])))

    async def _call(self, method: str, *args: object, **kwargs: object) -> object:
        if self._cleaning:
            await self._cool_off()
        self._guard(method, args, kwargs)
        if method in {
            'upload',
            'upload_thumbnail',
            'upload_filmstrip_thumbnail',
            'share_thumbnail',
        } or (
            method in {'create', 'update'}
            and any(kwargs.get(key) for key in ('thumbnail_path', 'filmstrip_thumbnail_path'))
        ):
            self.scanned_types.add('Attachment')
        if method in {'create', 'batch'}:
            if method == 'create' and args:
                self.scanned_types.add(str(args[0]))
            elif args:
                for operation in cast('list[dict[str, object]]', args[0]):
                    if operation.get('request_type') == 'create':
                        self.scanned_types.add(str(operation['entity_type']))
        function = cast('Callable[..., object]', getattr(self.client, method))
        result = function(*args, **kwargs)
        result = await result if inspect.isawaitable(result) else result
        self._record_result(method, args, result)
        return result

    async def _sudo_call(self, method: str, *args: object, **kwargs: object) -> object:
        self._guard(method, args, kwargs, sudo=True)
        if self._sudo_client is None:
            self._sudo_client = self.make_client(sudo_as_login=self.human_login)
        if method == 'create' and args:
            self.scanned_types.add(str(args[0]))
        function = cast('Callable[..., object]', getattr(self._sudo_client, method))
        result = function(*args, **kwargs)
        result = await result if inspect.isawaitable(result) else result
        if method == 'create':
            record = cast('dict[str, object]', result)
            self.track(str(record['type']), record['id'])
        return result

    async def call(self, method: str, *args: object, **kwargs: object) -> object:
        try:
            return await self._call(method, *args, **kwargs)
        except LiveStop as stop:
            raise _in_test(stop) from None

    async def _namespace_resources(self, resources: list[tuple[str, int]]) -> int:
        failures = 0
        projects = [self.project]
        for entity_type, (field_name, project_scoped) in _NAMESPACE_FIELDS.items():
            if entity_type not in self.scanned_types:
                continue
            for project in projects if project_scoped else [None]:
                filters: list[list[object]] = [[field_name, 'starts_with', self.namespace]]
                if project is not None:
                    filters.insert(0, ['project', 'is', project])
                capture = _FailureCapture()
                with capture:
                    rows = cast(
                        'list[dict[str, object]]',
                        await self.call('find', entity_type, filters, ['id']),
                    )
                    resources.extend((entity_type, cast('int', row['id'])) for row in rows)
                    if entity_type == 'Project':
                        projects.extend({'type': 'Project', 'id': row['id']} for row in rows)
                failures += capture.failed
        self.discovered.update(resources)
        return failures

    async def _restore_follows(self) -> int:
        if self.target is None or 'follow' not in self.flags:
            return 0
        capture = _FailureCapture()
        failures = 0
        with capture:
            baseline = cast('list[dict[str, object]]', self.target.baselines['F'])
            current = cast(
                'list[dict[str, object]]', await self.call('following', self.target.human)
            )
            identities = {_identity(link) for link in baseline}
            for link in current:
                if _identity(link) not in identities:
                    if _identity(link) in self.discovered:
                        await self.call('unfollow', self.target.human, link)
                    else:
                        failures += 1
            if not baseline_equal('F', await self.call('following', self.target.human), baseline):
                failures += 1
        return failures + capture.failed

    async def _retire(self, resources: list[tuple[str, int]]) -> int:
        failures = 0
        for entity_type, entity_id in resources:
            capture = _FailureCapture()
            with capture:
                row = await self.call('find_one', entity_type, [['id', 'is', entity_id]])
                if row is not None:
                    await self.call('delete', entity_type, entity_id)
            failures += capture.failed
        return failures

    async def create(
        self, entity_type: str, data: dict[str, object], **kwargs: object
    ) -> dict[str, object]:
        entity = cast('dict[str, object]', await self.call('create', entity_type, data, **kwargs))
        entity_id = entity['id']
        assert isinstance(entity_id, int)
        if (entity_type, entity_id) not in self._resources:
            self.track(entity_type, entity_id)
        return entity

    async def prefix_count(self) -> int:
        namespace, scanned = self.namespace, self.scanned_types
        self.namespace = 'sg-python-api-live-'
        self.scanned_types = set(_NAMESPACE_FIELDS)
        resources: list[tuple[str, int]] = []
        try:
            failures = await self._namespace_resources(resources)
            if failures:
                raise RuntimeError('live prefix scan failed')
            return len(set(resources))
        finally:
            self.namespace, self.scanned_types = namespace, scanned

    async def read_baselines(self) -> dict[str, object]:
        assert self.target is not None
        return {
            'F': await self.call('following', self.target.human),
            'S': await self.call('user_subscriptions_read'),
            'W': await self.call(
                'find',
                'WorkDayRule',
                [['project', 'is', self.project]],
                ['start_date', 'end_date', 'working', 'description', 'project', 'user'],
            ),
            'P': await self.call(
                'find_one', 'Project', [['id', 'is', self.project['id']]], ['archived']
            ),
        }

    async def _compare_flagged(self) -> int:
        if self.target is None or not self.flags:
            return 0
        capture = _FailureCapture()
        failures = 0
        with capture:
            current = await self.read_baselines()
            for flag, label in (('schedule', 'W'), ('archive', 'P'), ('subscription', 'S')):
                if flag in self.flags and not baseline_equal(
                    label, current[label], self.target.baselines[label]
                ):
                    failures += 1
        return failures + capture.failed

    async def _cleanup_owned(self) -> int:
        resources = list(reversed(self._resources))
        failures = await self._namespace_resources(resources)
        resources = list(dict.fromkeys(resources))
        failures += await self._restore_follows()
        for entity_type, entity_id in resources:
            if entity_type == 'Project':
                capture = _FailureCapture()
                with capture:
                    row = cast(
                        'dict[str, object] | None',
                        await self.call(
                            'find_one', 'Project', [['id', 'is', entity_id]], ['archived']
                        ),
                    )
                    if row and row.get('archived'):
                        await self.call('update', 'Project', entity_id, {'archived': False})
                failures += capture.failed
        order = {'WorkDayRule': 0, 'Attachment': 1, 'Project': 3}
        resources.sort(key=lambda resource: order.get(resource[0], 2))
        failures += await self._retire(resources)
        for entity_type, entity_id in resources:
            capture = _FailureCapture()
            with capture:
                if await self.call('find_one', entity_type, [['id', 'is', entity_id]]) is not None:
                    failures += 1
            failures += capture.failed
        residuals: list[tuple[str, int]] = []
        failures += await self._namespace_resources(residuals)
        failures += len(residuals)
        failures += await self._compare_flagged()
        return failures

    async def cleanup(self) -> None:
        budget = self.target.budget if self.target else None
        failures = 0
        self._cleaning = True
        try:
            with budget.cleanup() if budget else nullcontext():
                failures = await self._cleanup_owned()
        finally:
            self._cleaning = False
            if self._sudo_client is not None:
                result = cast('Callable[[], object]', self._sudo_client.close)()
                if inspect.isawaitable(result):
                    await result
        if failures:
            if budget:
                budget.stopped = 'live cleanup or restoration failed'
            raise RuntimeError(f'live cleanup failed: {failures} unsuccessful checks')

    async def sudo_call(self, method: str, *args: object, **kwargs: object) -> object:
        try:
            return await self._sudo_call(method, *args, **kwargs)
        except LiveStop as stop:
            raise _in_test(stop) from None

    async def sweep(self) -> None:
        assert self.target is not None
        self.namespace = 'sg-python-api-live-'
        self.scanned_types = set(_NAMESPACE_FIELDS)
        resources: list[tuple[str, int]] = []
        if await self._namespace_resources(resources):
            raise RuntimeError('live cleanup failed during sweep discovery')
        self.target.baselines['F'] = [
            link
            for link in cast('list[dict[str, object]]', self.target.baselines['F'])
            if _identity(link) not in self.discovered
        ]
        self.target.baselines['W'] = [
            rule
            for rule in cast('list[dict[str, object]]', self.target.baselines['W'])
            if not str(rule.get('description', '')).startswith(self.namespace)
        ]
        self.flags.update({'follow', 'archive', 'schedule', 'subscription'})
        await self.cleanup()


class _FailureCapture:
    def __init__(self) -> None:
        self.failed = False

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exc_type, traceback
        self.failed = isinstance(exc, (Exception, LiveStop))
        return self.failed


def _canonical(value: object, *, unordered: bool = False) -> str:
    if isinstance(value, dict):
        return 'dict:' + repr(
            sorted(
                (key, _canonical(item, unordered=unordered))
                for key, item in cast('dict[str, object]', value).items()
            )
        )
    if isinstance(value, list):
        values = [_canonical(item, unordered=unordered) for item in cast('list[object]', value)]
        return 'list:' + repr(sorted(values) if unordered else values)
    return f'{type(value).__name__}:{value!r}'


def _identity(value: object) -> tuple[str, int] | None:
    if isinstance(value, dict):
        link = cast('dict[str, object]', value)
        if isinstance(link.get('type'), str) and isinstance(link.get('id'), int):
            return cast('str', link['type']), cast('int', link['id'])
    return None


def _in_test(stop: LiveStop) -> LiveStop:
    return stop if isinstance(stop, Exception) else LiveStopError(str(stop))


def _inside_rule(day: date, rule: dict[str, object]) -> bool:
    return str(rule['start_date']) <= day.isoformat() <= str(rule['end_date'])


def _owned_query(filters: object, field_name: str, namespace: str) -> bool:
    if not isinstance(filters, list):
        return False
    for value in cast('list[object]', filters):
        if not isinstance(value, list):
            continue
        condition = cast('list[object]', value)
        if (
            len(condition) == 3
            and condition[0] == field_name
            and condition[1] == 'starts_with'
            and isinstance(condition[2], str)
            and condition[2].startswith(namespace)
        ):
            return True
    return False


def baseline_equal(label: str, left: object, right: object) -> bool:
    if label in {'F', 'W'} and isinstance(left, list) and isinstance(right, list):
        return {(_identity(row), _canonical(row)) for row in cast('list[object]', left)} == {
            (_identity(row), _canonical(row)) for row in cast('list[object]', right)
        }
    if label == 'S':
        return _canonical(cast('object', left), unordered=True) == _canonical(
            right, unordered=True
        )
    return left == right
