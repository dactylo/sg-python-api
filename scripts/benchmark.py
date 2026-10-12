#!/usr/bin/env python
"""Benchmark the validated client artifact against a pinned official checkout.

Run this script with `SG_OFFICIAL_CLIENT`, `SG_TEST_SITE`, `SG_TEST_SCRIPT`,
`SG_TEST_KEY`, and `SG_TEST_PROJECT` configured. The checkout must have current
artifact evidence in `dist/artifact-results.md`, written by
`scripts/validate_artifacts.sh`, and the validated wheel it identifies.

`BENCH_N` selects an even number of paired rounds from 2 through 20 and
defaults to 2. `BENCH_SEED` selects the integer ordering seed and defaults
to 0.

The harness creates a disposable candidate installation, runs isolated workers
against the live site, and creates and cleans up benchmark resources.
Successful validation and cleanup precede writing the raw report to
`dist/benchmarks/benchmark-results.md`.
"""

# Same-file collaboration follows module-boundary privacy.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import argparse
import datetime
import hashlib
import importlib
import importlib.util
import json
import math
import os
import platform
import queue
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import uuid
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, TypedDict, TypeVar, cast
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from types import ModuleType
    from typing import Unpack

_ARTIFACT_FIELD_COUNT = 4
_BATCH_SIZE = 2
_CLIENTS = ('ours', 'official')
_COLD_WARM_INVOCATION_COUNT = 2
_DEFAULT_SAMPLE_COUNT = 2
_DIRECT_PAYLOAD_BYTES = 1_000_000
_DIRECT_ROUTE_OPERATIONS = frozenset({'upload_direct'})
_DOWNLOAD_ROUTE_OPERATIONS = frozenset({'download', 'rss_download'})
_ENTITY_COUNT = 30
_GENERIC_PAYLOAD_BYTES = 4096
_GENERIC_ROUTE_OPERATIONS = frozenset({'upload_generic'})
_LATENCY_OPERATIONS = (
    'auth_first_request',
    'batch',
    'create',
    'delete',
    'download',
    'find_medium',
    'find_one',
    'find_paged',
    'find_small',
    'parallel_read',
    'payload_decode',
    'payload_encode',
    'schema_entity_read',
    'schema_field_read',
    'session_first_request',
    'summarize',
    'update',
    'upload_direct',
    'upload_generic',
    'upload_multipart',
)
_MAX_ENTITY_CODE_LENGTH = 100
_MAX_RESOURCE_JOURNAL_ENTRIES = 64
_MAX_SAMPLE_COUNT = 20
_MAX_WORKER_DIAGNOSTIC_LENGTH = 512
_MIN_SAMPLE_COUNT = 2
_MULTIPART_CAPABILITY_OPERATIONS = frozenset({'upload_multipart', 'rss_upload'})
_MULTIPART_LIMIT = 20_000_000
_MULTIPART_ROUTE_OPERATIONS = frozenset({'upload_multipart'})
_OFFICIAL_COMMIT = 'db056df3fb663efa6a69a2c07d2d0b69db35c336'
_OFFICIAL_REF = 'v3.10.2'
_OFFICIAL_REPOSITORY = 'https://github.com/shotgunsoftware/python-api'
_OFFICIAL_VERSION = '3.10.2'
_OPERATION_CATEGORIES = {
    'auth_first_request': 'read',
    'batch': 'batch',
    'create': 'write',
    'delete': 'write',
    'download': 'download',
    'find_medium': 'read',
    'find_one': 'read',
    'find_paged': 'read',
    'find_small': 'read',
    'parallel_read': 'read',
    'payload_decode': 'read',
    'payload_encode': 'batch',
    'schema_entity_read': 'read',
    'schema_field_read': 'read',
    'session_first_request': 'session_authentication',
    'summarize': 'read',
    'update': 'write',
    'upload_direct': 'upload',
    'upload_generic': 'upload',
    'upload_multipart': 'upload',
}
_PARALLEL_CALLS = 4
_PAYLOAD_BATCH_SIZE = 20
_PAYLOAD_BLOCK_SIZE = 1 << 20
_PROCESS_RUN = subprocess.run
_REPORT_WRAP_WIDTH = 99
_REQUIRED_ENV = (
    'SG_OFFICIAL_CLIENT',
    'SG_TEST_KEY',
    'SG_TEST_PROJECT',
    'SG_TEST_SCRIPT',
    'SG_TEST_SITE',
)
_RESOURCE_ENTITY_TYPES = frozenset({'Attachment', 'Shot', 'Version'})
_RESOURCE_JOURNAL_NAME = 'resource-journal.json'
_RSS_OPERATIONS = ('rss_download', 'rss_upload')
_T = TypeVar('_T')
_TAIL_PERCENTILE = 0.95
_VERIFIER_OPERATION_KEYS = frozenset(_LATENCY_OPERATIONS)
_WORKER_BASE_FIELDS = frozenset(
    {
        'api_key',
        'client',
        'client_root',
        'expected_root',
        'expected_version',
        'mode',
        'namespace',
        'project',
        'round',
        'script_name',
        'site',
        'work_root',
    }
)
_WORKER_MODES = frozenset({'cleanup', 'latency', 'preflight', 'rss', 'rss_download_setup'})
_LARGE_PAYLOAD_BYTES = _MULTIPART_LIMIT + 1


class _FailureCapture:
    """Capture and suppress one exception for explicit boundary handling."""

    def __init__(self) -> None:
        """Initialize an empty exception capture."""
        self.error: BaseException | None = None

    def __enter__(self) -> None:
        """Enter the capture boundary."""

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        exception: BaseException | None,
        _traceback: object,
    ) -> bool:
        """Capture and suppress the active exception."""
        self.error = exception
        return exception is not None


class _FindOptions(TypedDict, total=False):
    """Describe optional live-client find arguments."""

    order: list[dict[str, str]] | None
    limit: int
    page: int


class _LiveClientConfig(Protocol):
    """Describe the live client configuration surface."""

    on_request: Callable[[object], None] | None


class _CleanupClient(Protocol):
    """Describe the live cleanup client surface."""

    def close(self) -> None:
        """Close the client."""
        ...

    def delete(self, entity_type: str, entity_id: int) -> object:
        """Delete one entity.

        Returns:
            The client-specific deletion result.
        """
        ...

    def find(
        self,
        entity_type: str,
        filters: list[list[object]],
        fields: list[str],
        **options: Unpack[_FindOptions],
    ) -> object:
        """Find matching entities.

        Args:
            entity_type: Type of entity to query.
            filters: Query filters.
            fields: Fields to return.
            **options: Optional order, limit, and page controls.

        Returns:
            The client-specific query result.
        """
        ...

    def find_one(
        self,
        entity_type: str,
        filters: list[list[object]],
        fields: list[str] | None = None,
    ) -> object:
        """Find the first matching entity.

        Returns:
            The client-specific first-match result.
        """
        ...


class _RouteCapture:
    """Collect sanitized route and operation labels from our public observer seam."""

    def __init__(self) -> None:
        """Initialize an empty request-event capture."""
        self.categories: set[str] = set()
        self.routes: set[str] = set()

    def __call__(self, event: object) -> None:
        """Capture only allowlisted string metadata from one request event."""
        category = getattr(event, 'operation', None)
        route = getattr(event, 'route', None)
        if isinstance(category, str) and category.isascii():
            self.categories.add(category)
        if isinstance(route, str) and route.isascii():
            self.routes.add(route)


class _SetupClient(_CleanupClient, Protocol):
    """Describe the live setup client surface."""

    def create(self, entity_type: str, data: dict[str, object]) -> object:
        """Create one entity.

        Returns:
            The client-specific creation result.
        """
        ...

    def get_session_token(self) -> str:
        """Return a session token."""
        ...

    def info(self) -> object:
        """Return server metadata."""
        ...

    def upload(
        self,
        entity_type: str,
        entity_id: int,
        path: str,
        field_name: str | None = None,
        display_name: str | None = None,
    ) -> object:
        """Upload one file.

        Args:
            entity_type: Type of entity receiving the upload.
            entity_id: Numeric identifier of the receiving entity.
            path: Local file path.
            field_name: Optional destination field.
            display_name: Optional attachment display name.

        Returns:
            The client-specific upload result.
        """
        ...


class _MeasuredClient(_SetupClient, Protocol):
    """Describe the measured live client surface."""

    config: _LiveClientConfig

    def batch(self, requests: list[dict[str, object]]) -> list[object]:
        """Execute batch requests.

        Args:
            requests: Ordered batch request payloads.

        Returns:
            Ordered results for the submitted requests.
        """
        ...

    def download_attachment(self, entity_id: int, *, file_path: str) -> object:
        """Download one attachment.

        Args:
            entity_id: Numeric attachment identifier.
            file_path: Destination file path.

        Returns:
            The client-specific download result.
        """
        ...

    def find(
        self,
        entity_type: str,
        filters: list[list[object]],
        fields: list[str],
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        """Find matching entities.

        Args:
            entity_type: Type of entity to query.
            filters: Query filters.
            fields: Fields to return.
            **options: Optional order, limit, and page controls.

        Returns:
            Matching entity dictionaries.
        """
        ...

    def schema_entity_read(self) -> dict[str, object]:
        """Return entity schema metadata.

        Returns:
            Entity schema metadata keyed by entity type.
        """
        ...

    def schema_field_read(self, entity_type: str) -> dict[str, object]:
        """Return field schema metadata.

        Returns:
            Field schema metadata keyed by field name.
        """
        ...

    def summarize(
        self,
        entity_type: str,
        filters: list[list[object]],
        summary_fields: list[dict[str, object]],
    ) -> dict[str, object]:
        """Summarize matching entities.

        Args:
            entity_type: Type of entity to summarize.
            filters: Query filters.
            summary_fields: Summary operations to compute.

        Returns:
            Computed summary values.
        """
        ...

    def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict[str, object],
    ) -> dict[str, object]:
        """Update one entity.

        Args:
            entity_type: Type of entity to update.
            entity_id: Numeric identifier of the entity.
            data: Replacement field values.

        Returns:
            Updated entity data.
        """
        ...


@dataclass(frozen=True)
class _ArtifactEvidence:
    """Describe the exact validated wheel accepted by the harness."""

    candidate_commit: str
    sha256: str
    version: str
    wheel_path: Path


class _BenchmarkError(RuntimeError):
    """Report one sanitized benchmark boundary failure."""


class _CleanupError(_BenchmarkError):
    """Report that benchmark cleanup did not complete."""


class _CorrectnessError(_BenchmarkError):
    """Report that an operation result failed validation."""


@dataclass(frozen=True)
class _AttachmentExpectation:
    """Describe one uploaded Attachment correctness proof."""

    field_name: str | None
    owner_id: int
    owner_type: str
    source: Path


@dataclass(frozen=True)
class _ClientLocation:
    """Describe one isolated client import boundary."""

    client: str
    client_root: Path
    expected_root: Path
    expected_version: str


class _LiveState:
    """Own one worker's unique live data, generated payloads, and cleanup ledger."""

    def __init__(self, module: ModuleType, config: Mapping[str, Any]) -> None:
        """Initialize live resources and client state."""
        self.config = config
        self.module = module
        self.namespace = cast('str', config['namespace'])
        self.resources: list[tuple[str, int]] = []
        self.verified_deleted_shot_ids: set[int] = set()
        self.work_root = Path(cast('str', config['work_root']))
        self.journal = _ResourceJournal._load(self.work_root)
        self.resources.extend(self.journal._resources)
        self.clients: list[_MeasuredClient] = []
        self.project: dict[str, Any] = {}
        self.entities: list[dict[str, Any]] = []
        self.expected_descriptions: dict[int, str | None] = {}
        self.version: dict[str, Any] = {}
        self.download_attachment_id = 0
        self.server_info: dict[str, Any] = {}
        self.generic_payload = self.work_root / f'{self.namespace}-generic.bin'
        self.direct_payload = self.work_root / f'{self.namespace}-direct.bin'
        self.multipart_payload = self.work_root / f'{self.namespace}-multipart.bin'

    def _close_clients(self) -> tuple[int, BaseException | None]:
        """Close every client and preserve the first process-control exception."""
        failures = 0
        process_control: BaseException | None = None
        for client in reversed(self.clients):
            capture = _FailureCapture()
            with capture:
                client.close()
            failures += isinstance(capture.error, Exception)
            if process_control is None:
                process_control = _process_control(capture.error)
        return failures, process_control

    def _cleanup_clients(self) -> None:
        """Close clients without claiming ownership of borrowed live resources."""
        failures, process_control = self._close_clients()
        if process_control is not None:
            raise process_control
        if failures:
            raise _CleanupError(f'benchmark client cleanup failed for {failures} client(s)')

    def _make_client(self, session_token: str | None = None) -> _MeasuredClient:
        """Create one disconnected client without tracking its ownership."""
        kwargs: dict[str, object] = {'connect': False}
        if session_token is None:
            kwargs.update(
                script_name=self.config['script_name'],
                api_key=self.config['api_key'],
            )
        else:
            kwargs['session_token'] = session_token
        return cast('_MeasuredClient', self.module.Shotgun(self.config['site'], **kwargs))

    def _client(self, *, session_token: str | None = None) -> _MeasuredClient:
        """Create and track one disconnected main-thread client."""
        client = self._make_client(session_token=session_token)
        self.clients.append(client)
        return client

    def _namespaced_resources(
        self,
        control: _CleanupClient,
        entity_type: str,
        field: str,
    ) -> tuple[list[tuple[str, int]], int, BaseException | None]:
        """Return valid namespaced identities plus sanitized query failures."""
        find_capture = _FailureCapture()
        residue: object = None
        with find_capture:
            residue = control.find(
                entity_type,
                [['project', 'is', self.project], [field, 'starts_with', self.namespace]],
                ['id'],
            )
        if find_capture.error is not None:
            return (
                [],
                int(isinstance(find_capture.error, Exception)),
                _process_control(find_capture.error),
            )
        resources: list[tuple[str, int]] = []
        failures = 0
        if isinstance(residue, list):
            for candidate in cast('list[object]', residue):
                if not isinstance(candidate, dict):
                    failures += 1
                    continue
                entity_id = cast('dict[str, object]', candidate).get('id')
                if not isinstance(entity_id, int) or isinstance(entity_id, bool) or entity_id <= 0:
                    failures += 1
                    continue
                resources.append((entity_type, entity_id))
        else:
            failures += 1
        return resources, failures, None

    def _cleanup_namespaced_resources(self, control: _CleanupClient) -> None:
        """Delete and verify namespaced attachments, Shots, and Versions."""
        failures = 0
        process_control: BaseException | None = None
        for entity_type, field in (
            ('Attachment', 'filename'),
            ('Shot', 'code'),
            ('Version', 'code'),
        ):
            resources, find_failures, find_process_control = self._namespaced_resources(
                control,
                entity_type,
                field,
            )
            failures += find_failures
            if process_control is None:
                process_control = find_process_control
            cleanup_capture = _FailureCapture()
            with cleanup_capture:
                _cleanup_resources(
                    resources,
                    control.delete,
                    lambda kind, identity: control.find_one(kind, [['id', 'is', identity]]),
                )
            failures += isinstance(cleanup_capture.error, Exception)
            if process_control is None:
                process_control = _process_control(cleanup_capture.error)
        if process_control is not None:
            raise process_control
        if failures:
            raise _CleanupError('benchmark namespaced cleanup failed')

    def _cleanup(self) -> None:
        """Delete tracked resources, verify no residue, and close operation clients."""
        failures = 0
        process_control: BaseException | None = None
        control = self.clients[0] if self.clients else None
        if control is not None:
            delete_capture = _FailureCapture()
            with delete_capture:
                _cleanup_resources(
                    self.resources,
                    control.delete,
                    lambda entity_type, entity_id: control.find_one(
                        entity_type,
                        [['id', 'is', entity_id]],
                    ),
                )
            failures += isinstance(delete_capture.error, Exception)
            process_control = _process_control(delete_capture.error)
            residue_capture = _FailureCapture()
            with residue_capture:
                self._cleanup_namespaced_resources(control)
            failures += isinstance(residue_capture.error, Exception)
            if process_control is None:
                process_control = _process_control(residue_capture.error)
        elif self.resources:
            failures += 1
        close_failures, close_process_control = self._close_clients()
        failures += close_failures
        if process_control is None:
            process_control = close_process_control
        if process_control is not None:
            raise process_control
        if failures:
            raise _CleanupError(f'benchmark cleanup failed for {failures} check(s)')
        clear_capture = _FailureCapture()
        with clear_capture:
            self.journal._clear()
        clear_process_control = _process_control(clear_capture.error)
        if clear_process_control is not None:
            raise clear_process_control
        if clear_capture.error is not None:
            raise _CleanupError('benchmark cleanup failed for 1 check(s)') from None
        self.resources.clear()

    def _record_resource(self, entity_type: str, entity_id: int) -> None:
        """Validate and persist one benchmark-owned resource identity."""
        if (
            entity_type not in _RESOURCE_ENTITY_TYPES
            or isinstance(entity_id, bool)
            or entity_id <= 0
        ):
            raise _CorrectnessError('benchmark resource identity is invalid')
        self.resources.append((entity_type, entity_id))
        self.journal._record(entity_type, entity_id)

    def _create(
        self,
        client: _SetupClient,
        entity_type: str,
        data: dict[str, object],
    ) -> dict[str, Any]:
        """Create and track one live entity after validating its identity."""
        entity = client.create(entity_type, data)
        if not isinstance(entity, dict):
            raise _CorrectnessError(f'benchmark {entity_type} creation failed')
        typed = cast('dict[str, Any]', entity)
        if not isinstance(typed.get('id'), int) or isinstance(typed.get('id'), bool):
            raise _CorrectnessError(f'benchmark {entity_type} creation failed')
        self._record_resource(entity_type, cast('int', typed['id']))
        return typed

    def _prepare_project_and_version(self, control: _SetupClient) -> None:
        """Resolve the project and create the minimal transfer owner."""
        project = control.find_one('Project', [['name', 'is', self.config['project']]], ['name'])
        if not isinstance(project, dict):
            raise _CorrectnessError('benchmark project lookup failed')
        typed_project = cast('dict[str, object]', project)
        if not isinstance(typed_project.get('id'), int):
            raise _CorrectnessError('benchmark project lookup failed')
        self.project = cast('dict[str, Any]', project)
        self.version = self._create(
            control,
            'Version',
            {'code': _entity_code(self.namespace, 'version'), 'project': self.project},
        )
        info = control.info()
        if not isinstance(info, dict):
            raise _CorrectnessError('benchmark site-info lookup failed')
        self.server_info = cast('dict[str, Any]', info)

    def _prepare(self) -> None:
        """Create generated payloads and uniquely named live fixtures."""
        _write_payload(self.generic_payload, _GENERIC_PAYLOAD_BYTES)
        _write_payload(self.direct_payload, _DIRECT_PAYLOAD_BYTES)
        _write_payload(self.multipart_payload, _LARGE_PAYLOAD_BYTES)
        control = self._client()
        self._prepare_project_and_version(control)
        for index in range(_ENTITY_COUNT):
            self.entities.append(
                self._create(
                    control,
                    'Shot',
                    {
                        'code': _entity_code(self.namespace, f'shot-{index:03d}'),
                        'project': self.project,
                    },
                )
            )
        self.expected_descriptions = {cast('int', entity['id']): None for entity in self.entities}
        attachment = control.upload(
            'Version',
            cast('int', self.version['id']),
            str(self.direct_payload),
            field_name='sg_uploaded_movie',
            display_name=f'{self.namespace}-download.bin',
        )
        if not isinstance(attachment, int) or isinstance(attachment, bool):
            raise _CorrectnessError('benchmark download fixture upload failed')
        self.download_attachment_id = attachment
        self._record_resource('Attachment', attachment)

    def _prepare_rss(self, name: str) -> None:
        """Create only fixtures required by one isolated transfer RSS sample."""
        _write_payload(self.multipart_payload, _LARGE_PAYLOAD_BYTES)
        control = self._client()
        self._prepare_project_and_version(control)
        if name != 'rss_download':
            return
        attachment = control.upload(
            'Version',
            cast('int', self.version['id']),
            str(self.multipart_payload),
            field_name='sg_uploaded_movie',
            display_name=f'{self.namespace}-rss-download-source.bin',
        )
        if not isinstance(attachment, int) or isinstance(attachment, bool):
            raise _CorrectnessError('benchmark RSS download fixture upload failed')
        self.download_attachment_id = attachment
        self._record_resource('Attachment', attachment)


class _ResourceJournal:
    """Persist one worker's allowlisted live-resource identities atomically."""

    def __init__(self, path: Path, resources: tuple[tuple[str, int], ...]) -> None:
        """Bind a journal path and validated resource snapshot."""
        self.path = path
        self._entries = resources

    @staticmethod
    def _journal_path(worker_root: Path, *, must_exist: bool) -> Path:
        """Return a contained journal path below one resolved worker root."""
        try:
            root = worker_root.resolve(strict=True)
        except OSError:
            raise _BenchmarkError('Benchmark resource journal path is invalid') from None
        if not root.is_dir():
            raise _BenchmarkError('Benchmark resource journal path is invalid')
        try:
            path = root / _RESOURCE_JOURNAL_NAME
            resolved = path.resolve(strict=must_exist)
        except OSError:
            raise _BenchmarkError('Benchmark resource journal path is invalid') from None
        if not resolved.is_relative_to(root):
            raise _BenchmarkError('Benchmark resource journal path is invalid')
        return path

    def _publish(self, resources: tuple[tuple[str, int], ...]) -> None:
        """Atomically publish one complete owner-only journal snapshot."""
        payload = {
            'resources': [
                {'entity_type': entity_type, 'id': entity_id}
                for entity_type, entity_id in resources
            ]
        }
        encoded = (json.dumps(payload, separators=(',', ':')) + '\n').encode('ascii')
        _write_atomic_snapshot(
            self.path,
            encoded,
            'Could not atomically write benchmark resource journal',
        )

    def _clear(self) -> None:
        """Atomically publish an empty resource snapshot."""
        self._publish(())
        self._entries = ()

    @staticmethod
    def _validate_resources(payload: object) -> tuple[tuple[str, int], ...]:
        """Return validated resource identities from the exact journal schema."""
        if not isinstance(payload, dict):
            raise _BenchmarkError('Benchmark resource journal schema is invalid')
        typed_payload = cast('dict[object, object]', payload)
        if set(typed_payload) != {'resources'}:
            raise _BenchmarkError('Benchmark resource journal schema is invalid')
        raw_resources = typed_payload['resources']
        if not isinstance(raw_resources, list):
            raise _BenchmarkError('Benchmark resource journal schema is invalid')
        typed_resources = cast('list[object]', raw_resources)
        if len(typed_resources) > _MAX_RESOURCE_JOURNAL_ENTRIES:
            raise _BenchmarkError('Benchmark resource journal schema is invalid')
        resources: list[tuple[str, int]] = []
        seen: set[tuple[str, int]] = set()
        for raw_resource in typed_resources:
            if not isinstance(raw_resource, dict):
                raise _BenchmarkError('Benchmark resource journal schema is invalid')
            resource = cast('dict[object, object]', raw_resource)
            if set(resource) != {'entity_type', 'id'}:
                raise _BenchmarkError('Benchmark resource journal schema is invalid')
            entity_type = resource['entity_type']
            entity_id = resource['id']
            if (
                not isinstance(entity_type, str)
                or entity_type not in _RESOURCE_ENTITY_TYPES
                or not isinstance(entity_id, int)
                or isinstance(entity_id, bool)
                or entity_id <= 0
            ):
                raise _BenchmarkError('Benchmark resource journal schema is invalid')
            identity = (entity_type, entity_id)
            if identity in seen:
                raise _BenchmarkError('Benchmark resource journal schema is invalid')
            resources.append(identity)
            seen.add(identity)
        return tuple(resources)

    @classmethod
    def _create(cls, worker_root: Path) -> _ResourceJournal:
        """Create and return an empty journal below one worker root."""
        journal = cls(cls._journal_path(worker_root, must_exist=False), ())
        journal._publish(())
        return journal

    @classmethod
    def _load(cls, worker_root: Path) -> _ResourceJournal:
        """Load and validate a journal below one worker root."""
        path = cls._journal_path(worker_root, must_exist=True)
        try:
            payload: object = json.loads(path.read_bytes().decode('ascii'))
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise _BenchmarkError('Benchmark resource journal is invalid') from None
        return cls(path, cls._validate_resources(payload))

    def _record(self, entity_type: str, entity_id: int) -> None:
        """Atomically append one validated resource identity."""
        resources = self._validate_resources(
            {
                'resources': [
                    *(
                        {'entity_type': recorded_type, 'id': recorded_id}
                        for recorded_type, recorded_id in self._entries
                    ),
                    {'entity_type': entity_type, 'id': entity_id},
                ]
            }
        )
        self._publish(resources)
        self._entries = resources

    @property
    def _resources(self) -> tuple[tuple[str, int], ...]:
        """Return the immutable ordered resource identities."""
        return self._entries


@dataclass
class _ParallelReadTask:
    """Hold one indexed parallel-read request and its captured outcome."""

    entity_id: int
    complete: threading.Event
    error: BaseException | None = None
    result: object = None


class _ParallelReadRunner:
    """Own four persistent workers for one latency operation."""

    def __init__(self, state: _LiveState, shared_client: _MeasuredClient | None) -> None:
        """Initialize an isolated parallel-read runner."""
        self.state = state
        self.shared_client: _MeasuredClient | None = shared_client
        self._closed = False
        self._close_errors: list[BaseException | None] = [None] * _PARALLEL_CALLS
        self._queues: list[queue.Queue[_ParallelReadTask | None]] = [
            queue.Queue() for _index in range(_PARALLEL_CALLS)
        ]
        self._ready = [threading.Event() for _index in range(_PARALLEL_CALLS)]
        self._setup_errors: list[BaseException | None] = [None] * _PARALLEL_CALLS
        self._started_indexes: list[int] = []
        self._threads = [
            threading.Thread(
                target=self._worker,
                args=(index,),
                name=f'benchmark-parallel-read-{index}',
            )
            for index in range(_PARALLEL_CALLS)
        ]
        start_error: BaseException | None = None
        for index, thread in enumerate(self._threads):
            start_capture = _FailureCapture()
            with start_capture:
                thread.start()
            if thread.ident is not None:
                self._started_indexes.append(index)
            if start_capture.error is not None:
                start_error = start_capture.error
                break
        if start_error is not None:
            self._close()
            raise start_error
        for ready in self._ready:
            ready.wait()
        setup_error = self._first_error(self._setup_errors)
        if setup_error is not None:
            self._close()
            raise setup_error

    @staticmethod
    def _first_error(errors: Sequence[BaseException | None]) -> BaseException | None:
        """Return the first process-control error or ordinary error."""
        for error in errors:
            process_control = _process_control(error)
            if process_control is not None:
                return process_control
        return next((error for error in errors if error is not None), None)

    def _close(self) -> None:
        """Stop every worker and finish thread-owned client cleanup."""
        if self._closed:
            return
        self._closed = True
        for index in self._started_indexes:
            self._queues[index].put(None)
        join_errors: list[BaseException | None] = []
        for index in self._started_indexes:
            capture = _FailureCapture()
            with capture:
                self._threads[index].join()
            join_errors.append(capture.error)
        error = self._first_error([*self._close_errors, *join_errors])
        if error is not None:
            raise error

    def _read(self, entity_ids: Sequence[int]) -> list[object]:
        """Read four identities concurrently and preserve their input order."""
        if len(entity_ids) != _PARALLEL_CALLS:
            raise _BenchmarkError('benchmark parallel read requires four entity IDs')
        tasks = [_ParallelReadTask(entity_id, threading.Event()) for entity_id in entity_ids]
        for work_queue, task in zip(self._queues, tasks, strict=True):
            work_queue.put(task)
        for task in tasks:
            task.complete.wait()
        error = self._first_error([task.error for task in tasks])
        if error is not None:
            raise error
        return [task.result for task in tasks]

    def _worker(self, index: int) -> None:
        """Run one persistent worker with its selected client ownership."""
        client = self.shared_client
        if client is None:
            setup_capture = _FailureCapture()
            with setup_capture:
                client = self.state._make_client()
            self._setup_errors[index] = setup_capture.error
        self._ready[index].set()
        if self._setup_errors[index] is not None:
            return
        client = cast('_MeasuredClient', client)
        try:
            while (task := self._queues[index].get()) is not None:
                request_capture = _FailureCapture()
                with request_capture:
                    task.result = client.find_one(
                        'Shot',
                        [['id', 'is', task.entity_id]],
                        ['code'],
                    )
                task.error = request_capture.error
                task.complete.set()
        finally:
            if self.shared_client is None:
                close_capture = _FailureCapture()
                with close_capture:
                    client.close()
                self._close_errors[index] = close_capture.error


class _Operations:
    """Execute and validate the complete live latency inventory."""

    def __init__(
        self,
        client: _MeasuredClient,
        state: _LiveState,
        route_capture: _RouteCapture | None = None,
    ) -> None:
        """Initialize benchmark operation dispatch tables."""
        self.client = client
        self.state = state
        self.route_capture = route_capture
        self._delete_ids: list[int] = []
        self._auth_client: _MeasuredClient | None = None
        self._parallel_runner: _ParallelReadRunner | None = None
        self._session_client: _MeasuredClient | None = None
        self._attach_route_capture(client)
        _validate_operation_verifiers(self)

    def _attach_route_capture(self, client: _MeasuredClient) -> None:
        """Attach the measured operation's observer to one request client."""
        if self.route_capture is not None:
            client.config.on_request = self.route_capture

    def _call(self, name: str, invocation: int) -> object:
        """Dispatch one complete public inventory operation."""
        return _Operations._callbacks()[name](self, invocation)

    def _close(self) -> None:
        """Close the prepared parallel-read runner when present."""
        runner = self._parallel_runner
        self._parallel_runner = None
        if runner is not None:
            runner._close()

    def _create(self, invocation: int) -> object:
        """Create and track one uniquely named entity."""
        expected = _entity_code(self.state.namespace, f'created-{invocation}')
        entity = self.state._create(
            self.client,
            'Shot',
            {
                'code': expected,
                'project': self.state.project,
            },
        )
        return entity, expected

    def _delete(self, invocation: int) -> object:
        """Delete one prepared unique entity and return its identity for verification."""
        entity_id = self._delete_ids[invocation]
        deleted = self.client.delete('Shot', entity_id)
        return deleted, entity_id

    def _download(self, invocation: int) -> object:
        """Download a known generated attachment to an atomic destination."""
        destination = self.state.work_root / f'download-{invocation}.bin'
        return self.client.download_attachment(
            self.state.download_attachment_id,
            file_path=str(destination),
        )

    def _entity_ids(self) -> list[int]:
        """Return prepared entity identifiers."""
        return [cast('int', entity['id']) for entity in self.state.entities]

    def _batch(self, invocation: int) -> object:
        """Update two entities in one batch."""
        expected = [
            f'{self.state.namespace}-batch-{invocation}-{index}' for index in range(_BATCH_SIZE)
        ]
        result = self.client.batch(
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': self._entity_ids()[index],
                    'data': {'description': expected[index]},
                }
                for index in range(_BATCH_SIZE)
            ]
        )
        for entity_id, value in zip(
            self._entity_ids()[:_BATCH_SIZE],
            expected,
            strict=True,
        ):
            self.state.expected_descriptions[entity_id] = value
        return result, expected

    def _filters(self) -> list[list[object]]:
        """Return filters restricted to this worker's unique namespace."""
        return [
            ['project', 'is', self.state.project],
            ['code', 'starts_with', self.state.namespace],
        ]

    def _find_known_on(self, client: _MeasuredClient) -> object:
        """Read one known prepared entity through a selected client."""
        return client.find_one('Shot', [['id', 'is', self._entity_ids()[0]]], ['code'])

    def _auth_first_request(self, invocation: int) -> object:
        """Construct a script-auth client cold, then distinguish its warm request."""
        if invocation == 0:
            self._auth_client = self.state._client()
            self._attach_route_capture(self._auth_client)
        return self._find_known_on(cast('_MeasuredClient', self._auth_client))

    def _find_known(self) -> object:
        """Read one known prepared entity."""
        return self._find_known_on(self.client)

    def _find_medium(self, _invocation: int) -> object:
        """Read the medium result set."""
        return self.client.find(
            'Shot',
            self._filters(),
            ['code'],
            order=[{'field_name': 'id', 'direction': 'asc'}],
            limit=20,
        )

    def _find_one(self, _invocation: int) -> object:
        """Read one known prepared entity."""
        return self._find_known()

    def _find_paged(self, _invocation: int) -> object:
        """Read two explicit result pages."""
        order = [{'field_name': 'id', 'direction': 'asc'}]
        first = self.client.find('Shot', self._filters(), ['code'], order=order, limit=15, page=1)
        second = self.client.find('Shot', self._filters(), ['code'], order=order, limit=15, page=2)
        return [*first, *second]

    def _find_small(self, _invocation: int) -> object:
        """Read the small result set."""
        return self.client.find(
            'Shot',
            self._filters(),
            ['code'],
            order=[{'field_name': 'id', 'direction': 'asc'}],
            limit=3,
        )

    def _matching_shot_count(self) -> int:
        """Return the tracked matching Shot count after measured deletions."""
        return sum(
            entity_type == 'Shot' and entity_id not in self.state.verified_deleted_shot_ids
            for entity_type, entity_id in self.state.resources
        )

    def _parallel_read(self, _invocation: int) -> object:
        """Read four known entities through the prepared persistent runner."""
        if self._parallel_runner is None:
            raise _BenchmarkError('benchmark parallel read was not prepared')
        return self._parallel_runner._read(self._entity_ids()[:_PARALLEL_CALLS])

    def _payload_decode(self, _invocation: int) -> object:
        """Exercise the large public response-decoding path."""
        return self.client.find(
            'Shot',
            self._filters(),
            ['code', 'description', 'created_at'],
            order=[{'field_name': 'id', 'direction': 'asc'}],
            limit=30,
        )

    def _payload_encode(self, invocation: int) -> object:
        """Exercise the large public request-encoding path."""
        expected = [
            f'{self.state.namespace}-payload-{invocation}-{index}'
            for index in range(_PAYLOAD_BATCH_SIZE)
        ]
        result = self.client.batch(
            [
                {
                    'request_type': 'update',
                    'entity_type': 'Shot',
                    'entity_id': self._entity_ids()[index],
                    'data': {'description': expected[index]},
                }
                for index in range(_PAYLOAD_BATCH_SIZE)
            ]
        )
        for entity_id, value in zip(
            self._entity_ids()[:_PAYLOAD_BATCH_SIZE],
            expected,
            strict=True,
        ):
            self.state.expected_descriptions[entity_id] = value
        return result, expected

    def _prepare(self, name: str) -> None:
        """Prepare untimed per-operation resources."""
        if name == 'parallel_read':
            shared_client = self.client if self.route_capture is not None else None
            self._parallel_runner = _ParallelReadRunner(self.state, shared_client)
            return
        if name != 'delete':
            return
        for invocation in range(_COLD_WARM_INVOCATION_COUNT):
            target = self.state._create(
                self.state.clients[0],
                'Shot',
                {
                    'code': _entity_code(self.state.namespace, f'delete-{invocation}'),
                    'project': self.state.project,
                },
            )
            self._delete_ids.append(cast('int', target['id']))

    def _schema_entity_read(self, _invocation: int) -> object:
        """Read the entity schema."""
        return self.client.schema_entity_read()

    def _schema_field_read(self, _invocation: int) -> object:
        """Read the Shot field schema."""
        return self.client.schema_field_read('Shot')

    def _session_first_request(self, invocation: int) -> object:
        """Acquire a session, configure a client, and make its first request."""
        if invocation == 0:
            control = self.state.clients[0]
            self._attach_route_capture(control)
            session_token = control.get_session_token()
            self._session_client = self.state._client(session_token=session_token)
            self._attach_route_capture(self._session_client)
        return cast('_MeasuredClient', self._session_client).find_one(
            'Shot',
            [['id', 'is', self._entity_ids()[0]]],
            ['code'],
        )

    def _summarize(self, _invocation: int) -> object:
        """Summarize the unique prepared result set."""
        return self.client.summarize(
            'Shot',
            self._filters(),
            [{'field': 'id', 'type': 'count'}],
        )

    def _track_upload(self, attachment: object) -> object:
        """Track one validated attachment identity."""
        if isinstance(attachment, int) and not isinstance(attachment, bool) and attachment > 0:
            self.state._record_resource('Attachment', attachment)
        return attachment

    def _update(self, invocation: int) -> object:
        """Update and return one entity plus its expected value."""
        expected = f'{self.state.namespace}-updated-{invocation}'
        result = self.client.update(
            'Shot',
            self._entity_ids()[0],
            {'description': expected},
        )
        self.state.expected_descriptions[self._entity_ids()[0]] = expected
        return result, expected

    def _upload_generic(self, invocation: int) -> object:
        """Upload a generic generated file."""
        return self._track_upload(
            self.client.upload(
                'Shot',
                self._entity_ids()[0],
                str(self.state.generic_payload),
                display_name=f'{self.state.namespace}-generic-{invocation}.bin',
            )
        )

    def _upload_version(self, path: Path, label: str, invocation: int) -> object:
        """Upload one generated Version movie and track its attachment."""
        return self._track_upload(
            self.client.upload(
                'Version',
                cast('int', self.state.version['id']),
                str(path),
                field_name='sg_uploaded_movie',
                display_name=f'{self.state.namespace}-{label}-{invocation}.bin',
            )
        )

    def _upload_direct(self, invocation: int) -> object:
        """Upload a direct-route generated payload."""
        return self._upload_version(self.state.direct_payload, 'direct', invocation)

    def _upload_multipart(self, invocation: int) -> object:
        """Upload a multipart-route generated payload."""
        return self._upload_version(self.state.multipart_payload, 'multipart', invocation)

    @staticmethod
    def _verify_batch_result(result: object, expected_size: int) -> bool:
        """Prove batch results preserve all exact update values."""
        if not isinstance(result, tuple):
            return False
        typed_result = cast('tuple[object, ...]', result)
        match len(typed_result):
            case 2:
                responses, expected = typed_result
            case _:
                return False
        if not isinstance(responses, list) or not isinstance(expected, list):
            return False
        typed_responses = cast('list[object]', responses)
        typed_expected = cast('list[object]', expected)
        if len(typed_responses) != expected_size or len(typed_expected) != expected_size:
            return False
        return all(
            isinstance(response, dict)
            and cast('dict[str, Any]', response).get('description') == value
            for response, value in zip(typed_responses, typed_expected, strict=True)
        )

    @staticmethod
    def _callbacks() -> dict[str, Callable[[_Operations, int], object]]:
        """Return the explicit latency-operation callback mapping."""
        return {
            'auth_first_request': _Operations._auth_first_request,
            'batch': _Operations._batch,
            'create': _Operations._create,
            'delete': _Operations._delete,
            'download': _Operations._download,
            'find_medium': _Operations._find_medium,
            'find_one': _Operations._find_one,
            'find_paged': _Operations._find_paged,
            'find_small': _Operations._find_small,
            'parallel_read': _Operations._parallel_read,
            'payload_decode': _Operations._payload_decode,
            'payload_encode': _Operations._payload_encode,
            'schema_entity_read': _Operations._schema_entity_read,
            'schema_field_read': _Operations._schema_field_read,
            'session_first_request': _Operations._session_first_request,
            'summarize': _Operations._summarize,
            'update': _Operations._update,
            'upload_direct': _Operations._upload_direct,
            'upload_generic': _Operations._upload_generic,
            'upload_multipart': _Operations._upload_multipart,
        }

    def _verify_created(self, result: object) -> bool:
        """Prove one create result contains its exact requested code."""
        if not isinstance(result, tuple):
            return False
        typed_result = cast('tuple[object, ...]', result)
        match len(typed_result):
            case 2:
                entity, expected = typed_result
            case _:
                return False
        if not isinstance(entity, dict):
            return False
        typed_entity = cast('dict[str, object]', entity)
        return isinstance(typed_entity.get('id'), int) and typed_entity.get('code') == expected

    def _verify_deleted(self, result: object) -> bool:
        """Prove deletion succeeded and the entity no longer resolves."""
        if not isinstance(result, tuple):
            return False
        typed_result = cast('tuple[object, ...]', result)
        match len(typed_result):
            case 2:
                deleted, entity_id = typed_result
            case _:
                return False
        if deleted is not True or not isinstance(entity_id, int) or isinstance(entity_id, bool):
            return False
        if self.state.clients[0].find_one('Shot', [['id', 'is', entity_id]]) is not None:
            return False
        self.state.verified_deleted_shot_ids.add(entity_id)
        return True

    def _verify_download(self, result: object) -> bool:
        """Prove the downloaded file exactly matches the generated source."""
        if not isinstance(result, str):
            return False
        try:
            return _file_digest(Path(result)) == _file_digest(self.state.direct_payload)
        except OSError:
            return False

    def _verify_entities(
        self,
        result: object,
        expected_entities: Sequence[Mapping[str, object]],
    ) -> bool:
        """Prove exact known entity IDs and code values."""
        if not isinstance(result, list):
            return False
        typed_result = cast('list[object]', result)
        if len(typed_result) != len(expected_entities):
            return False
        expected = {(entity.get('id'), entity.get('code')) for entity in expected_entities}
        actual = {
            (cast('dict[str, Any]', item).get('id'), cast('dict[str, Any]', item).get('code'))
            for item in typed_result
            if isinstance(item, dict)
        }
        return len(actual) == len(expected_entities) and actual == expected

    def _verify_known_entity(self, result: object) -> bool:
        """Prove one result matches the exact first prepared entity."""
        if not isinstance(result, dict):
            return False
        typed_result = cast('dict[str, object]', result)
        expected = self.state.entities[0]
        return (
            typed_result.get('id') == expected['id']
            and typed_result.get('code') == expected['code']
        )

    def _verify_payload_decode(self, result: object) -> bool:
        """Prove decoded fields preserve exact values and datetime behavior."""
        if not self._verify_entities(result, self.state.entities):
            return False
        typed = cast('list[dict[str, Any]]', result)
        by_id = {item.get('id'): item for item in typed}
        for entity in self.state.entities:
            entity_id = cast('int', entity['id'])
            item = by_id.get(entity_id)
            if item is None:
                return False
            created_at = item.get('created_at')
            if (
                not isinstance(created_at, datetime.datetime)
                or created_at.tzinfo is not self.state.module.sg_timezone.local
            ):
                return False
            if item.get('description') != self.state.expected_descriptions[entity_id]:
                return False
        return True

    def _verify_summarize(self, result: object) -> bool:
        """Prove one summary response contains the exact matching Shot count."""
        if not isinstance(result, dict):
            return False
        summaries = cast('dict[str, Any]', result).get('summaries')
        if not isinstance(summaries, dict):
            return False
        count = cast('dict[str, Any]', summaries).get('id')
        return (
            isinstance(count, (int, float))
            and not isinstance(count, bool)
            and count == self._matching_shot_count()
        )

    def _verify_update(self, result: object) -> bool:
        """Prove the update result contains its exact requested value."""
        if not isinstance(result, tuple):
            return False
        typed_result = cast('tuple[object, ...]', result)
        match len(typed_result):
            case 2:
                entity, expected = typed_result
            case _:
                return False
        if not isinstance(entity, dict):
            return False
        return cast('dict[str, Any]', entity).get('description') == expected

    def _verifiers(self) -> dict[str, Callable[[object], bool]]:
        """Return the explicit latency-operation verifier mapping."""
        return {
            'auth_first_request': self._verify_known_entity,
            'batch': lambda value: self._verify_batch_result(value, _BATCH_SIZE),
            'create': self._verify_created,
            'delete': self._verify_deleted,
            'download': self._verify_download,
            'find_medium': lambda value: self._verify_entities(value, self.state.entities[:20]),
            'find_one': self._verify_known_entity,
            'find_paged': lambda value: self._verify_entities(value, self.state.entities),
            'find_small': lambda value: self._verify_entities(value, self.state.entities[:3]),
            'parallel_read': lambda value: self._verify_entities(
                value, self.state.entities[:_PARALLEL_CALLS]
            ),
            'payload_decode': self._verify_payload_decode,
            'payload_encode': lambda value: self._verify_batch_result(value, _PAYLOAD_BATCH_SIZE),
            'schema_entity_read': lambda value: isinstance(value, dict) and 'Shot' in value,
            'schema_field_read': lambda value: isinstance(value, dict) and 'code' in value,
            'session_first_request': self._verify_known_entity,
            'summarize': self._verify_summarize,
            'update': self._verify_update,
            'upload_direct': lambda value: _verify_attachment(
                self.state,
                value,
                _AttachmentExpectation(
                    'sg_uploaded_movie',
                    cast('int', self.state.version['id']),
                    'Version',
                    self.state.direct_payload,
                ),
            ),
            'upload_generic': lambda value: _verify_attachment(
                self.state,
                value,
                _AttachmentExpectation(
                    None,
                    self._entity_ids()[0],
                    'Shot',
                    self.state.generic_payload,
                ),
            ),
            'upload_multipart': lambda value: _verify_attachment(
                self.state,
                value,
                _AttachmentExpectation(
                    'sg_uploaded_movie',
                    cast('int', self.state.version['id']),
                    'Version',
                    self.state.multipart_payload,
                ),
            ),
        }

    def _verify_result(self, name: str, result: object) -> bool:
        """Dispatch one operation's correctness proof."""
        return self._verifiers()[name](result)


class _WorkerFailureError(_BenchmarkError):
    """Report one sanitized isolated-worker failure."""


def _capability_upload_route(
    server_info: Mapping[str, object],
    entity_type: str,
    field_name: str | None,
    file_size: int,
) -> str:
    """Classify one official upload from sanitized capability evidence."""
    if not server_info.get('s3_direct_uploads_enabled'):
        return 'capability:form'
    raw_allowed = server_info.get('s3_enabled_upload_types')
    if isinstance(raw_allowed, Mapping):
        allowed = cast('Mapping[object, object]', raw_allowed)
        for key in (entity_type, '*'):
            fields = allowed.get(key)
            if fields == '*':
                break
            if isinstance(fields, list) and ('*' in fields or field_name in fields):
                break
        else:
            if entity_type != 'Version' or field_name != 'sg_uploaded_movie':
                return 'capability:form'
    elif entity_type != 'Version' or field_name != 'sg_uploaded_movie':
        return 'capability:form'
    if file_size <= _MULTIPART_LIMIT:
        return 'capability:direct'
    return 'capability:multipart'


def _client_orders(sample_count: int, *, seed: int) -> list[tuple[str, str]]:
    """Return deterministic pairwise-counterbalanced client orders."""
    if sample_count < _MIN_SAMPLE_COUNT or sample_count % _MIN_SAMPLE_COUNT:
        raise _BenchmarkError('Sample count must be a positive even integer')
    orders: list[tuple[str, str]] = []
    while len(orders) < sample_count:
        pair = len(orders) // _MIN_SAMPLE_COUNT
        digest = hashlib.sha256(f'{seed}:{pair}'.encode('ascii')).digest()
        first = ('ours', 'official') if digest[0] % 2 == 0 else ('official', 'ours')
        second = (first[1], first[0])
        orders.extend((first, second))
    return orders


def _entity_code(namespace: str, suffix: str) -> str:
    """Build one bounded ASCII code for a generated Shot or Version."""
    code = f'{namespace}-{suffix}'
    if not code.isascii() or len(code) > _MAX_ENTITY_CODE_LENGTH:
        raise _BenchmarkError('Benchmark entity code exceeds the 100-character limit')
    return code


def _file_digest(path: Path) -> str:
    """Return the SHA-256 digest of one generated transfer file."""
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _has_identity(value: object, identity: int) -> bool:
    """Return whether a dictionary's ID equals the expected identity."""
    if not isinstance(value, dict):
        return False
    return cast('dict[str, object]', value).get('id') == identity


def _markdown_table_lines(headers: list[str], rows: list[list[str]]) -> list[str]:
    """Render a GFM table with deterministic column padding."""
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]

    def render_row(values: list[str]) -> str:
        return (
            '| '
            + ' | '.join(value.ljust(width) for value, width in zip(values, widths, strict=True))
            + ' |'
        )

    separator = ['-' * width for width in widths]
    return [render_row(headers), render_row(separator), *(render_row(row) for row in rows)]


def _new_namespace(client: str, round_number: int) -> str:
    """Create a unique ASCII namespace for one client round."""
    if client not in _CLIENTS or round_number < 0:
        raise _BenchmarkError('Invalid benchmark namespace input')
    return f'sg-python-api-benchmark-{client}-{round_number}-{uuid.uuid4().hex}'


def _normalize_peak_rss(raw_value: object, platform: str) -> int:
    """Normalize `ru_maxrss` to bytes for supported platform conventions."""
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
        raise _BenchmarkError('Peak RSS value is invalid')
    if not math.isfinite(raw_value) or raw_value < 0:
        raise _BenchmarkError('Peak RSS value is invalid')
    if platform == 'darwin':
        multiplier = 1
    elif platform.startswith(('linux', 'freebsd')):
        multiplier = 1024
    else:
        raise _BenchmarkError(f'Unsupported peak RSS platform: {platform}')
    return int(raw_value * multiplier)


def _operation_route(name: str, state: _LiveState) -> str:
    """Return sanitized live route evidence for one operation."""
    if name in _GENERIC_ROUTE_OPERATIONS:
        return _capability_upload_route(
            state.server_info,
            'Shot',
            None,
            state.generic_payload.stat().st_size,
        )
    if name in _DIRECT_ROUTE_OPERATIONS:
        return _capability_upload_route(
            state.server_info,
            'Version',
            'sg_uploaded_movie',
            state.direct_payload.stat().st_size,
        )
    if name in _MULTIPART_CAPABILITY_OPERATIONS:
        return _capability_upload_route(
            state.server_info,
            'Version',
            'sg_uploaded_movie',
            state.multipart_payload.stat().st_size,
        )
    if name in _DOWNLOAD_ROUTE_OPERATIONS:
        return 'download'
    return 'rpc'


def _preflight_tools() -> None:
    """Require all local tools and modules before any live worker starts."""
    if shutil.which('git') is None:
        raise _BenchmarkError('Missing required tool: git')
    if importlib.util.find_spec('installer') is None:
        raise _BenchmarkError('Missing required Python module: installer')
    resource_module = importlib.import_module('resource')
    if not hasattr(resource_module, 'getrusage') or not hasattr(resource_module, 'RUSAGE_SELF'):
        raise _BenchmarkError('Missing required peak RSS module support')
    _normalize_peak_rss(0, sys.platform)


def _process_control(error: BaseException | None) -> BaseException | None:
    """Return an exception that must escape ordinary failure handling."""
    return error if error is not None and not isinstance(error, Exception) else None


def _attempt_cleanup_deletes(
    cleanup_order: Sequence[tuple[str, int]],
    delete: Callable[[str, int], object],
) -> BaseException | None:
    """Attempt every delete and return the first process-control exception."""
    process_control: BaseException | None = None
    for entity_type, entity_id in cleanup_order:
        capture = _FailureCapture()
        with capture:
            delete(entity_type, entity_id)
        if process_control is None:
            process_control = _process_control(capture.error)
    return process_control


def _inspect_cleanup_residue(
    cleanup_order: Sequence[tuple[str, int]],
    find_one: Callable[[str, int], object],
    process_control: BaseException | None,
) -> tuple[int, int, BaseException | None]:
    """Inspect every identity and return residue, query, and process-control facts."""
    residue = 0
    unverifiable = 0
    for entity_type, entity_id in cleanup_order:
        capture = _FailureCapture()
        found: object = None
        with capture:
            found = find_one(entity_type, entity_id)
        if process_control is None:
            process_control = _process_control(capture.error)
        if isinstance(capture.error, Exception):
            unverifiable += 1
        elif found is not None:
            residue += 1
    return residue, unverifiable, process_control


def _cleanup_resources(
    resources: Sequence[tuple[str, int]],
    delete: Callable[[str, int], object],
    find_one: Callable[[str, int], object],
) -> None:
    """Delete and independently prove absence for every tracked identity."""
    cleanup_order = tuple(reversed(resources))
    process_control = _attempt_cleanup_deletes(cleanup_order, delete)
    residue, unverifiable, process_control = _inspect_cleanup_residue(
        cleanup_order,
        find_one,
        process_control,
    )
    if process_control is not None:
        raise process_control
    if residue or unverifiable:
        parts: list[str] = []
        if residue:
            parts.append(f'{residue} confirmed residue')
        if unverifiable:
            parts.append(f'{unverifiable} unverifiable identity')
        raise _CleanupError('benchmark cleanup failed: ' + ', '.join(parts))


def _raise_operation_errors(
    name: str,
    measurement_error: BaseException | None,
    close_errors: Sequence[BaseException | None],
) -> None:
    """Raise one measured operation's highest-precedence failure."""
    measurement_process_control = _process_control(measurement_error)
    if measurement_process_control is not None:
        raise measurement_process_control
    for close_error in close_errors:
        close_process_control = _process_control(close_error)
        if close_process_control is not None:
            raise close_process_control
    close_error = next((error for error in close_errors if error is not None), None)
    if close_error is not None:
        raise _CleanupError('benchmark client close failed') from measurement_error
    if isinstance(measurement_error, _CorrectnessError):
        raise measurement_error
    if measurement_error is not None:
        raise _BenchmarkError(f'benchmark operation failed: {name}') from None


def _raise_serialization_errors(
    message: str,
    errors: Sequence[BaseException | None],
) -> None:
    """Raise the first process-control exception or one sanitized failure."""
    for error in errors:
        process_control = _process_control(error)
        if process_control is not None:
            raise process_control
    if any(error is not None for error in errors):
        raise _BenchmarkError(message) from None


def _report_latency_lines(clients: Mapping[str, Any]) -> list[str]:
    """Return validated latency-result table lines."""
    headers = [
        'Client',
        'Operation',
        'Category',
        'Route',
        'Mode',
        'Median',
        'p95',
        'Raw samples',
    ]
    rows: list[list[str]] = []
    for client_name in _CLIENTS:
        client = cast('dict[str, Any]', clients[client_name])
        operations = cast('dict[str, Any]', client['operations'])
        if set(operations) != set(_LATENCY_OPERATIONS):
            raise _BenchmarkError('Benchmark report latency inventory is incomplete')
        for name in _LATENCY_OPERATIONS:
            operation = cast('dict[str, Any]', operations[name])
            if operation.get('correct') is not True:
                raise _BenchmarkError('Benchmark report contains a correctness failure')
            for mode in ('cold', 'warm'):
                summary = cast('dict[str, Any]', operation[mode])
                raw_label = f'Raw {mode} samples: {summary["raw"]}'
                rows.append(
                    [
                        client_name,
                        name,
                        operation['category'],
                        operation['route'],
                        mode,
                        f'{summary["median"]:.9f} s',
                        f'{summary["p95"]:.9f} s',
                        raw_label,
                    ]
                )
    return ['## Latency results', '', *_markdown_table_lines(headers, rows), '']


def _report_rss_lines(clients: Mapping[str, Any]) -> list[str]:
    """Return validated peak-RSS table lines."""
    headers = [
        'Client',
        'Operation',
        'Route basis',
        'Baselines',
        'Peaks',
        'Peak deltas',
        'Max delta',
    ]
    rows: list[list[str]] = []
    for client_name in _CLIENTS:
        client = cast('dict[str, Any]', clients[client_name])
        rss = cast('dict[str, Any]', client['rss'])
        if set(rss) != set(_RSS_OPERATIONS):
            raise _BenchmarkError('Benchmark report RSS inventory is incomplete')
        for name in _RSS_OPERATIONS:
            values = cast('dict[str, Any]', rss[name])
            expected = {
                'baseline_bytes',
                'correct',
                'peak_bytes',
                'peak_delta_bytes',
                'route',
            }
            baselines = values.get('baseline_bytes')
            peaks = values.get('peak_bytes')
            deltas = values.get('peak_delta_bytes')
            if (
                set(values) != expected
                or values.get('correct') is not True
                or not isinstance(baselines, list)
                or not baselines
                or not isinstance(peaks, list)
                or not peaks
                or not isinstance(deltas, list)
                or not deltas
                or not isinstance(values.get('route'), str)
            ):
                raise _BenchmarkError('Benchmark report RSS evidence is invalid')
            typed_baselines = cast('list[int]', baselines)
            typed_deltas = cast('list[int]', deltas)
            typed_peaks = cast('list[int]', peaks)
            rows.append(
                [
                    client_name,
                    name,
                    values['route'],
                    str(typed_baselines),
                    str(typed_peaks),
                    str(typed_deltas),
                    str(max(typed_deltas)),
                ]
            )
    return ['## Isolated peak RSS', '', *_markdown_table_lines(headers, rows)]


def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    """Run one sanitized preflight command without raising automatically."""
    trusted = {str(Path(sys.executable).resolve())}
    git = shutil.which('git')
    if git is not None:
        trusted.add(str(Path(git).resolve()))
    executable = shutil.which(command[0]) if not Path(command[0]).is_absolute() else command[0]
    if executable is None or str(Path(executable).resolve()) not in trusted:
        raise _BenchmarkError('Refusing to run an untrusted benchmark command')
    return _PROCESS_RUN(command, capture_output=True, text=True, check=False)


def _checked_command(command: list[str], message: str) -> str:
    """Run one preflight command and return stripped stdout."""
    completed = _run_command(command)
    if completed.returncode != 0:
        raise _BenchmarkError(message)
    return completed.stdout.strip()


def _install_candidate(evidence: _ArtifactEvidence, install_root: Path) -> Path:
    """Install the exact wheel into a disposable target and return site-packages."""
    completed = _run_command(
        [
            sys.executable,
            '-I',
            '-m',
            'installer',
            '--destdir',
            str(install_root),
            str(evidence.wheel_path),
        ]
    )
    if completed.returncode != 0:
        raise _BenchmarkError('Could not install validated source wheel into disposable target')
    matches = sorted(
        {
            package.parent.parent.resolve()
            for package in install_root.rglob('shotgun_api3/__init__.py')
            if package.is_file()
        },
    )
    if len(matches) != 1 or not matches[0].is_relative_to(install_root.resolve()):
        raise _BenchmarkError('Could not identify one isolated candidate installation')
    return matches[0]


def _load_artifact_evidence(repo_root: Path) -> _ArtifactEvidence:
    """Validate artifact identity and freshness through the isolated helper."""
    completed = _run_command(
        [
            sys.executable,
            '-I',
            str(repo_root / 'scripts' / '_artifact_evidence.py'),
            str(repo_root),
        ]
    )
    if completed.returncode != 0:
        raise _BenchmarkError(completed.stderr.strip() or 'Artifact validation evidence failed')
    fields = completed.stdout.strip().split('|')
    if len(fields) != _ARTIFACT_FIELD_COUNT:
        raise _BenchmarkError('Artifact validation evidence produced an invalid result')
    commit, version, wheel_path, digest = fields
    return _ArtifactEvidence(commit, digest, version, Path(wheel_path))


def _sample_summary(samples: Sequence[float], *, tail_percentile: float) -> dict[str, Any]:
    """Retain samples and calculate median plus a nearest-rank tail."""
    if not samples or not 0 < tail_percentile < 1:
        raise _BenchmarkError('Samples and tail percentile are invalid')
    raw = list(samples)
    if any(not math.isfinite(value) or value < 0 for value in raw):
        raise _BenchmarkError('All samples must be finite nonnegative numbers')
    ordered = sorted(raw)
    rank = math.ceil(tail_percentile * len(ordered))
    label = f'p{round(tail_percentile * 100)}'
    return {'median': statistics.median(raw), label: ordered[rank - 1], 'raw': raw}


def _validate_disposable_path(path: Path, repo_root: Path) -> Path:
    """Require a resolved disposable path outside the repository."""
    try:
        resolved = path.resolve(strict=True)
        repository = repo_root.resolve(strict=True)
    except OSError as error:
        raise _BenchmarkError('Cannot resolve disposable benchmark path') from error
    if resolved == repository or resolved.is_relative_to(repository):
        raise _BenchmarkError('Disposable benchmark path must be outside the repository')
    return resolved


def _validate_module_origin(module_path: Path, expected_root: Path) -> Path:
    """Require a loaded module path below its isolated client root."""
    try:
        resolved = module_path.resolve(strict=True)
        root = expected_root.resolve(strict=True)
    except OSError as error:
        raise _BenchmarkError('Cannot resolve loaded client module origin') from error
    if not resolved.is_relative_to(root):
        raise _BenchmarkError('Loaded client module is outside the isolated client root')
    return resolved


def _preflight_worker(module: ModuleType, config: Mapping[str, Any]) -> dict[str, Any]:
    """Return the exact client identity loaded across the process boundary."""
    origin = _validate_module_origin(
        Path(cast('str', module.__file__)),
        Path(cast('str', config['expected_root'])),
    )
    return {
        'client': config['client'],
        'origin': str(origin),
        'version': config['expected_version'],
    }


def _validate_official_identity(
    official_root: Path,
    *,
    top_level: str,
    head: str,
    ref_commit: str,
    origin: str,
) -> None:
    """Validate the official checkout identity from sanitized Git facts."""
    if Path(top_level).resolve() != official_root.resolve():
        raise _BenchmarkError('Official checkout top level does not match the supplied path')
    if head != _OFFICIAL_COMMIT:
        raise _BenchmarkError('Official checkout HEAD does not match the pinned commit')
    if ref_commit != _OFFICIAL_COMMIT:
        raise _BenchmarkError('Official checkout ref does not match the pinned commit')
    normalized = origin.removesuffix('.git').rstrip('/')
    if normalized != _OFFICIAL_REPOSITORY:
        raise _BenchmarkError('Official checkout origin is not the pinned repository')


def _official_checkout(repo_root: Path, official_root: Path) -> Path:
    """Verify and return the pinned caller-supplied official checkout."""
    try:
        resolved = official_root.expanduser().resolve(strict=True)
    except OSError as error:
        raise _BenchmarkError('Official checkout is unavailable') from error
    if resolved == repo_root.resolve() or resolved.is_relative_to(repo_root.resolve()):
        raise _BenchmarkError('Official checkout must be outside the candidate repository')
    git = ['git', '-C', str(resolved)]
    _validate_official_identity(
        resolved,
        top_level=_checked_command(
            [*git, 'rev-parse', '--show-toplevel'],
            'Could not verify official checkout top level',
        ),
        head=_checked_command(
            [*git, 'rev-parse', '--verify', 'HEAD^{commit}'],
            'Could not verify official checkout HEAD',
        ),
        ref_commit=_checked_command(
            [*git, 'rev-parse', '--verify', f'{_OFFICIAL_REF}^{{commit}}'],
            'Could not verify pinned official ref',
        ),
        origin=_checked_command(
            [*git, 'remote', 'get-url', 'origin'],
            'Could not verify official checkout origin',
        ),
    )
    package_status = _checked_command(
        [
            *git,
            'status',
            '--porcelain',
            '--untracked-files=all',
            '--',
            'shotgun_api3',
        ],
        'Could not verify official checkout package state',
    )
    if package_status:
        raise _BenchmarkError('Official checkout has local package changes')
    return resolved


def _validate_operation_metadata() -> None:
    """Require every operation metadata class to match the benchmark inventory."""
    latency_operations = set(_LATENCY_OPERATIONS)
    for metadata_class, operation_keys in (
        ('callbacks', set(_Operations._callbacks())),
        ('categories', set(_OPERATION_CATEGORIES)),
    ):
        mismatch = sorted(latency_operations ^ operation_keys)
        if mismatch:
            raise _BenchmarkError(
                f'Benchmark operation metadata mismatch: {metadata_class}: {mismatch[0]}'
            )
    route_exceptions = (
        _DIRECT_ROUTE_OPERATIONS
        | _DOWNLOAD_ROUTE_OPERATIONS
        | _GENERIC_ROUTE_OPERATIONS
        | _MULTIPART_CAPABILITY_OPERATIONS
        | _MULTIPART_ROUTE_OPERATIONS
    )
    mismatch = sorted(route_exceptions - latency_operations - set(_RSS_OPERATIONS))
    if mismatch:
        raise _BenchmarkError(
            f'Benchmark operation metadata mismatch: route exceptions: {mismatch[0]}'
        )


def _validate_operation_verifiers(operations: _Operations) -> None:
    """Check bound verifier keys against the independent operation inventory."""
    mismatch = sorted(_VERIFIER_OPERATION_KEYS ^ set(operations._verifiers()))
    if mismatch:
        raise _BenchmarkError(f'Benchmark operation metadata mismatch: verifiers: {mismatch[0]}')


def _validate_preflight_result(
    result: Mapping[str, Any],
    location: _ClientLocation,
) -> None:
    """Require exact client identity from one isolated preflight worker."""
    expected_origin = (location.client_root / 'shotgun_api3' / '__init__.py').resolve(strict=True)
    if set(result) != {'client', 'origin', 'version'}:
        raise _BenchmarkError('Benchmark preflight result has unexpected fields')
    try:
        origin = Path(cast('str', result['origin'])).resolve(strict=True)
    except (OSError, TypeError) as error:
        raise _BenchmarkError('Benchmark preflight origin is invalid') from error
    if (
        result['client'] != location.client
        or result['version'] != location.expected_version
        or origin != expected_origin
    ):
        raise _BenchmarkError('Benchmark preflight client identity is invalid')


def _validate_report_identity(result: Mapping[str, Any]) -> None:
    """Validate the benchmark gate and exact candidate identities."""
    if result['cleanup'] != 'PASS':
        raise _BenchmarkError('Benchmark cleanup status is not PASS')
    if result['official_ref'] != _OFFICIAL_REF or result['official_commit'] != _OFFICIAL_COMMIT:
        raise _BenchmarkError('Benchmark official identity is invalid')
    if re.fullmatch(r'[0-9a-f]{40}', str(result['candidate_commit'])) is None:
        raise _BenchmarkError('Benchmark candidate commit is invalid')
    if re.fullmatch(r'[0-9a-f]{64}', str(result['artifact_sha256'])) is None:
        raise _BenchmarkError('Benchmark artifact hash is invalid')


def _validate_route_evidence(client: str, name: str, route: str) -> None:
    """Require direct and multipart routes to prove their expected path."""
    if client == 'official' and name in _GENERIC_ROUTE_OPERATIONS:
        if route not in {
            'capability:form',
            'capability:direct',
            'capability:multipart',
        }:
            raise _BenchmarkError(f'Worker route evidence is invalid: {name}')
        return
    if name not in _DIRECT_ROUTE_OPERATIONS | _MULTIPART_ROUTE_OPERATIONS:
        return
    if client == 'official':
        expected = (
            'capability:direct' if name in _DIRECT_ROUTE_OPERATIONS else 'capability:multipart'
        )
        if route != expected:
            raise _BenchmarkError(f'Worker route evidence is invalid: {name}')
        return
    if not route.startswith('observed:'):
        raise _BenchmarkError(f'Worker route evidence is invalid: {name}')
    observed = set(route.removeprefix('observed:').split('+'))
    if name in _DIRECT_ROUTE_OPERATIONS:
        valid = 'object_put' in observed and 'part_put' not in observed
    else:
        valid = {'part_put', 'finalize'}.issubset(observed) and 'object_put' not in observed
    if not valid:
        raise _BenchmarkError(f'Worker route evidence is invalid: {name}')


def _validate_latency_result(client: str, name: str, value: object) -> None:
    """Validate one latency result's exact sanitized schema."""
    fields = {'category', 'cold', 'correct', 'route', 'warm'}
    if not isinstance(value, dict):
        raise _BenchmarkError(f'Worker operation has an unexpected field: {name}')
    typed = cast('dict[str, Any]', value)
    if set(typed) != fields:
        raise _BenchmarkError(f'Worker operation has an unexpected field: {name}')
    if typed['correct'] is not True:
        raise _BenchmarkError(f'Worker correctness failed: {name}')
    if typed['category'] != _OPERATION_CATEGORIES[name]:
        raise _BenchmarkError(f'Worker category evidence is invalid: {name}')
    route = typed['route']
    if not isinstance(route, str) or not route.isascii() or not route:
        raise _BenchmarkError(f'Worker route evidence is missing: {name}')
    _validate_route_evidence(client, name, route)
    for mode in ('cold', 'warm'):
        sample = typed[mode]
        if not isinstance(sample, (int, float)) or not math.isfinite(sample) or sample < 0:
            raise _BenchmarkError(f'Worker {mode} sample is invalid: {name}')


def _validate_rss_download_handoff(config: Mapping[str, Any]) -> None:
    """Validate the sanitized measured-download handoff fields."""
    if config['mode'] != 'rss' or config.get('rss_operation') != 'rss_download':
        return
    attachment_id = config['download_attachment_id']
    digest = config['expected_download_sha256']
    if (
        not isinstance(attachment_id, int)
        or isinstance(attachment_id, bool)
        or config['download_route'] != 'download'
        or not isinstance(digest, str)
        or re.fullmatch(r'[0-9a-f]{64}', digest) is None
    ):
        raise _BenchmarkError('Worker configuration has invalid RSS download handoff')


def _validate_rss_result(client: str, name: str, value: object) -> None:
    """Validate one isolated peak-RSS result's exact sanitized schema."""
    if not isinstance(value, dict):
        raise _BenchmarkError(f'Worker RSS result has an unexpected field: {name}')
    typed = cast('dict[str, Any]', value)
    fields = {'baseline_bytes', 'correct', 'peak_bytes', 'peak_delta_bytes', 'route'}
    if set(typed) != fields:
        raise _BenchmarkError(f'Worker RSS result has an unexpected field: {name}')
    if typed['correct'] is not True:
        raise _BenchmarkError(f'Worker RSS correctness failed: {name}')
    for field in ('baseline_bytes', 'peak_bytes', 'peak_delta_bytes'):
        byte_count = typed[field]
        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
            raise _BenchmarkError(f'Worker RSS value is invalid: {name}')
    if typed['peak_bytes'] < typed['baseline_bytes']:
        raise _BenchmarkError(f'Worker RSS value is invalid: {name}')
    if typed['peak_delta_bytes'] != typed['peak_bytes'] - typed['baseline_bytes']:
        raise _BenchmarkError(f'Worker RSS value is invalid: {name}')
    route = typed['route']
    if not isinstance(route, str) or not route.isascii() or not route:
        raise _BenchmarkError(f'Worker RSS route evidence is invalid: {name}')
    if name == 'rss_upload':
        _validate_route_evidence(client, 'upload_multipart', route)


def _validate_round_result(result: Mapping[str, Any]) -> None:
    """Validate one worker round as complete, correct, and skip-free."""
    if result.get('client') not in _CLIENTS:
        raise _BenchmarkError('Worker client identity is invalid')
    operations = result.get('operations')
    rss = result.get('rss')
    if not isinstance(operations, dict):
        raise _BenchmarkError('Worker latency inventory is incomplete')
    if not isinstance(rss, dict):
        raise _BenchmarkError('Worker RSS inventory is incomplete')
    typed_operations = cast('dict[str, Any]', operations)
    typed_rss = cast('dict[str, Any]', rss)
    if set(typed_operations) != set(_LATENCY_OPERATIONS):
        raise _BenchmarkError('Worker latency inventory is incomplete')
    if set(typed_rss) != set(_RSS_OPERATIONS):
        raise _BenchmarkError('Worker RSS inventory is incomplete')
    for name in _LATENCY_OPERATIONS:
        _validate_latency_result(cast('str', result['client']), name, typed_operations[name])
    for name in _RSS_OPERATIONS:
        _validate_rss_result(cast('str', result['client']), name, typed_rss[name])


def _validated_machine(value: object) -> dict[str, str]:
    """Return allowlisted reproducibility metadata without machine paths."""
    fields = {'architecture', 'os', 'platform', 'python_version'}
    if not isinstance(value, dict):
        raise _BenchmarkError('Benchmark machine metadata is invalid')
    metadata = cast('dict[str, object]', value)
    if set(metadata) != fields:
        raise _BenchmarkError('Benchmark machine metadata is invalid')
    if any(
        not isinstance(item, str) or re.fullmatch(r'[A-Za-z0-9._+-]+', item) is None
        for item in metadata.values()
    ):
        raise _BenchmarkError('Benchmark machine metadata is invalid')
    return cast('dict[str, str]', metadata)


def _machine_metadata() -> dict[str, str]:
    """Collect sanitized Python, OS, platform, and architecture metadata."""
    return _validated_machine(
        {
            'architecture': platform.machine() or 'unknown',
            'os': platform.system() or 'unknown',
            'platform': sys.platform,
            'python_version': platform.python_version(),
        }
    )


def _validated_site(value: object) -> str:
    """Return a credential-free site identity for publication."""
    if not isinstance(value, str) or not value.isascii():
        raise _BenchmarkError('Benchmark site must be an ASCII URL')
    split = urlsplit(value)
    if split.scheme not in {'http', 'https'} or not split.hostname:
        raise _BenchmarkError('Benchmark site must be an HTTP(S) origin')
    if split.username is not None or split.password is not None:
        raise _BenchmarkError('Benchmark site must not contain credentials')
    if split.query or split.fragment:
        raise _BenchmarkError('Benchmark site must not contain query or fragment data')
    port = f':{split.port}' if split.port is not None else ''
    return f'{split.scheme}://{split.hostname}{port}'


def _verify_attachment(
    state: _LiveState,
    result: object,
    expectation: _AttachmentExpectation,
) -> bool:
    """Prove one uploaded Attachment identity, owner field, and exact bytes."""
    if not isinstance(result, int) or isinstance(result, bool):
        return False
    control = state.clients[0]
    attachment = control.find_one('Attachment', [['id', 'is', result]], ['id'])
    if not _has_identity(attachment, result):
        return False
    if expectation.field_name is not None:
        owner = control.find_one(
            expectation.owner_type,
            [['id', 'is', expectation.owner_id]],
            [expectation.field_name],
        )
        linked: object = None
        if isinstance(owner, dict):
            linked = cast('dict[str, object]', owner).get(expectation.field_name)
        if not _has_identity(linked, result):
            return False
    destination = state.work_root / f'verify-upload-{result}.bin'
    downloaded = control.download_attachment(result, file_path=str(destination))
    try:
        return (
            isinstance(downloaded, str)
            and Path(downloaded).resolve() == destination.resolve()
            and _file_digest(destination) == _file_digest(expectation.source)
        )
    except OSError:
        return False


def _verify_loaded_client(module: object, expected_root: Path, expected_version: str) -> None:
    """Verify a loaded client module's filesystem origin and version."""
    origin = getattr(module, '__file__', None)
    if not isinstance(origin, str):
        raise _BenchmarkError('Loaded client module has no filesystem origin')
    _validate_module_origin(Path(origin), expected_root)
    if getattr(module, '__version__', None) != expected_version:
        raise _BenchmarkError('Loaded client version does not match the required version')


def _import_worker_client(config: Mapping[str, Any]) -> ModuleType:
    """Import exactly one client from its preflighted root."""
    client_root = Path(cast('str', config['client_root'])).resolve(strict=True)
    expected_root = Path(cast('str', config['expected_root'])).resolve(strict=True)
    sys.path.insert(0, str(client_root))
    module = importlib.import_module('shotgun_api3')
    _verify_loaded_client(module, expected_root, cast('str', config['expected_version']))
    return module


def _verify_worker_modules(expected_root: Path) -> None:
    """Prove every loaded client submodule stayed inside its isolated root."""
    found = False
    for name, module in tuple(sys.modules.items()):
        if name == 'shotgun_api3' or name.startswith('shotgun_api3.'):
            origin = getattr(module, '__file__', None)
            if origin is not None:
                if not isinstance(origin, str):
                    raise _BenchmarkError('Loaded client module has an invalid origin')
                _validate_module_origin(Path(origin), expected_root)
            found = True
    if not found:
        raise _BenchmarkError('No client modules were loaded')


def _worker_base_config(
    *,
    location: _ClientLocation,
    namespace: str,
    round_number: int,
    work_root: Path,
) -> dict[str, object]:
    """Build the allowlisted shared worker configuration."""
    return {
        'api_key': os.environ['SG_TEST_KEY'],
        'client': location.client,
        'client_root': str(location.client_root),
        'expected_root': str(location.expected_root),
        'expected_version': location.expected_version,
        'namespace': namespace,
        'project': os.environ['SG_TEST_PROJECT'],
        'round': round_number,
        'script_name': os.environ['SG_TEST_SCRIPT'],
        'site': os.environ['SG_TEST_SITE'],
        'work_root': str(work_root),
    }


def _worker_config_extra_fields(mode: object, operation: object) -> set[str]:
    """Return the exact fields allowed for one worker mode."""
    if mode not in _WORKER_MODES:
        raise _BenchmarkError('Worker configuration has invalid mode or client')
    if mode == 'rss_download_setup':
        if operation != 'rss_download':
            raise _BenchmarkError('Worker configuration has invalid RSS operation')
        return {'rss_operation'}
    if mode != 'rss':
        return set()
    if operation not in _RSS_OPERATIONS:
        raise _BenchmarkError('Worker configuration has invalid RSS operation')
    if operation == 'rss_download':
        return {
            'download_attachment_id',
            'download_route',
            'expected_download_sha256',
            'rss_operation',
        }
    return {'rss_operation'}


def _read_worker_config(config_path: Path) -> dict[str, Any]:
    """Read and validate the shape and boundary of one transient worker config."""
    try:
        raw_config: object = json.loads(config_path.read_text(encoding='ascii'))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise _BenchmarkError('Cannot read worker configuration') from error
    if not isinstance(raw_config, dict):
        raise _BenchmarkError('Worker configuration must be an object')
    config = cast('dict[str, Any]', raw_config)
    if not _WORKER_BASE_FIELDS.issubset(config):
        raise _BenchmarkError('Worker configuration has invalid fields')
    mode = config['mode']
    if config['client'] not in _CLIENTS:
        raise _BenchmarkError('Worker configuration has invalid mode or client')
    operation = config.get('rss_operation')
    extra = _worker_config_extra_fields(mode, operation)
    if set(config) != set(_WORKER_BASE_FIELDS) | extra:
        raise _BenchmarkError('Worker configuration has invalid fields')
    _validate_rss_download_handoff(config)
    return config


def _worker_diagnostic(stderr: str, env: Mapping[str, str]) -> str | None:
    """Return one bounded benchmark-owned worker diagnostic when safe."""
    if (
        not stderr.isascii()
        or not stderr.endswith('\n')
        or stderr.count('\n') != 1
        or len(stderr) > _MAX_WORKER_DIAGNOSTIC_LENGTH + 1
    ):
        return None
    diagnostic = stderr[:-1]
    # Nested download workers add status prefixes and fixed cleanup warnings.
    # Accept only that owned grammar, never arbitrary exception text or stderr.
    base, *warnings = diagnostic.split('; warning: ')
    warnings_valid = all(
        warning in {'Benchmark worker recovery failed', 'benchmark cleanup failed'}
        for warning in warnings
    )
    status_prefix = r'Benchmark worker failed with status -?[0-9]+: '
    while re.match(status_prefix, base):
        base = re.sub(status_prefix, '', base, count=1)
    correctness_prefix = 'benchmark correctness failed for '
    operation_prefix = 'benchmark operation failed: '
    owned = warnings_valid and (
        base
        in {
            'Benchmark worker failed at an unexpected boundary',
            'Benchmark worker timed out',
            'Benchmark worker recovery failed',
            'Benchmark worker returned invalid structured output',
            'Benchmark worker returned a non-object result',
            'benchmark cleanup failed',
        }
        or re.fullmatch(r'Benchmark worker failed with status -?[0-9]+', base) is not None
        or (
            base.startswith(operation_prefix)
            and base.removeprefix(operation_prefix) in _LATENCY_OPERATIONS
        )
        or (
            base.startswith(correctness_prefix)
            and base.removeprefix(correctness_prefix) in _RSS_OPERATIONS
        )
    )
    configured_values = (env.get(name, '') for name in _REQUIRED_ENV)
    if not owned or any(value and value in diagnostic for value in configured_values):
        return None
    return diagnostic


def _launch_worker(
    config_path: Path,
    work_root: Path,
    *,
    env: Mapping[str, str] | None = None,
    run: Callable[..., subprocess.CompletedProcess[str]] = _PROCESS_RUN,
) -> dict[str, Any]:
    """Run one worker in isolated mode and return its structured result."""
    worker_env = dict(os.environ if env is None else env)
    worker_env.pop('PYTHONPATH', None)
    command = [sys.executable, '-I', str(Path(__file__).resolve()), '--worker', str(config_path)]
    completed: subprocess.CompletedProcess[str] | None = None
    with suppress(subprocess.TimeoutExpired):
        completed = run(
            command,
            cwd=work_root,
            env=worker_env,
            capture_output=True,
            text=True,
            check=False,
            timeout=3600,
        )
    if completed is None:
        raise _WorkerFailureError('Benchmark worker timed out')
    if completed.returncode != 0:
        message = f'Benchmark worker failed with status {completed.returncode}'
        diagnostic = _worker_diagnostic(completed.stderr, worker_env)
        if diagnostic is not None:
            message = f'{message}: {diagnostic}'
        raise _WorkerFailureError(message)
    invalid_output = False
    try:
        payload = json.loads(completed.stdout.splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        invalid_output = True
        payload = None
    if invalid_output:
        raise _BenchmarkError('Benchmark worker returned invalid structured output') from None
    if not isinstance(payload, dict):
        raise _BenchmarkError('Benchmark worker returned a non-object result')
    return cast('dict[str, Any]', payload)


def _measure_verified(
    name: str,
    operation: Callable[[], _T],
    verify: Callable[[_T], bool],
    accepted: list[float],
    *,
    timer: Callable[[], float] = time.perf_counter,
) -> _T:
    """Measure one call and append its latency only after correctness passes."""
    started = timer()
    result = operation()
    elapsed = timer() - started
    if not verify(result):
        raise _CorrectnessError(f'benchmark correctness failed for {name}')
    if not math.isfinite(elapsed) or elapsed < 0:
        raise _BenchmarkError(f'benchmark timer failed for {name}')
    accepted.append(elapsed)
    return result


def _raise_worker_errors(
    worker_error: BaseException | None,
    journal_error: BaseException | None,
    recovery_error: BaseException | None,
) -> None:
    """Preserve process control and sanitized worker plus recovery diagnostics."""
    for error in (worker_error, journal_error, recovery_error):
        process_control = _process_control(error)
        if process_control is not None:
            raise process_control
    if recovery_error is not None:
        message = 'Benchmark worker recovery failed'
        if worker_error is not None:
            diagnostic = (
                _worker_diagnostic(f'{worker_error}\n', os.environ)
                if isinstance(worker_error, _BenchmarkError)
                else None
            )
            message = (
                (diagnostic or 'Benchmark worker failed at an unexpected boundary')
                + '; warning: '
                + message
            )
        raise _CleanupError(message)
    if worker_error is not None:
        raise worker_error


def _run_with_cleanup(work: Callable[[], _T], cleanup: Callable[[], object]) -> _T:
    """Run work and cleanup, preserving the first process-control exception."""
    work_capture = _FailureCapture()
    result: _T | None = None
    with work_capture:
        result = work()
    cleanup_capture = _FailureCapture()
    with cleanup_capture:
        cleanup()
    work_process_control = _process_control(work_capture.error)
    if work_process_control is not None:
        raise work_process_control
    cleanup_process_control = _process_control(cleanup_capture.error)
    if cleanup_process_control is not None:
        raise cleanup_process_control
    if cleanup_capture.error is not None:
        diagnostic = (
            _worker_diagnostic(f'{work_capture.error}\n', os.environ)
            if isinstance(work_capture.error, _BenchmarkError)
            else None
        )
        message = 'benchmark cleanup failed'
        if diagnostic is not None:
            message = diagnostic + '; warning: ' + message
        raise _CleanupError(message)
    if work_capture.error is not None:
        raise work_capture.error
    return cast('_T', result)


def _cleanup_worker(module: ModuleType, config: Mapping[str, Any]) -> dict[str, str]:
    """Recover every journaled identity and prove no namespaced residue."""
    state = _LiveState(module, config)

    def work() -> dict[str, str]:
        control = state._client()
        project = control.find_one(
            'Project',
            [['name', 'is', config['project']]],
            ['name'],
        )
        if not isinstance(project, dict):
            raise _CorrectnessError('benchmark project lookup failed')
        typed_project = cast('dict[str, object]', project)
        if not isinstance(typed_project.get('id'), int):
            raise _CorrectnessError('benchmark project lookup failed')
        state.project = cast('dict[str, Any]', project)
        return {'cleanup': 'PASS'}

    return _run_with_cleanup(work, state._cleanup)


def _latency_worker(module: ModuleType, config: Mapping[str, Any]) -> dict[str, Any]:
    """Measure all latency operations with fresh clients and cold/warm separation."""
    state = _LiveState(module, config)

    def work() -> dict[str, Any]:
        state._prepare()
        operations: dict[str, Any] = {}
        for name in _LATENCY_OPERATIONS:
            client = state._client()
            capture = _RouteCapture()
            ours = config['client'] == 'ours'
            runner = _Operations(client, state, capture) if ours else _Operations(client, state)
            samples: list[float] = []
            measurement_capture = _FailureCapture()
            with measurement_capture:
                runner._prepare(name)
                for invocation in range(_COLD_WARM_INVOCATION_COUNT):
                    _measure_verified(
                        name,
                        lambda invocation=invocation, runner=runner, name=name: runner._call(
                            name, invocation
                        ),
                        lambda result, runner=runner, name=name: runner._verify_result(
                            name, result
                        ),
                        samples,
                    )
            runner_close_capture = _FailureCapture()
            with runner_close_capture:
                runner._close()
            client_close_capture = _FailureCapture()
            with client_close_capture:
                client.close()
            _raise_operation_errors(
                name,
                measurement_capture.error,
                [runner_close_capture.error, client_close_capture.error],
            )
            if ours:
                if not capture.routes:
                    raise _CorrectnessError(f'benchmark route evidence failed: {name}')
                route = 'observed:' + '+'.join(sorted(capture.routes))
                if _OPERATION_CATEGORIES[name] not in capture.categories:
                    raise _CorrectnessError(f'benchmark category evidence failed: {name}')
            else:
                route = _operation_route(name, state)
            _validate_route_evidence(cast('str', config['client']), name, route)
            operations[name] = {
                'category': _OPERATION_CATEGORIES[name],
                'cold': samples[0],
                'correct': True,
                'route': route,
                'warm': samples[1],
            }
        return {'client': config['client'], 'operations': operations, 'rss': {}}

    return _run_with_cleanup(work, state._cleanup)


def _rss_worker(module: ModuleType, config: Mapping[str, Any]) -> dict[str, Any]:
    """Measure one large transfer's peak RSS in a dedicated process."""
    name = cast('str', config['rss_operation'])
    if name not in _RSS_OPERATIONS:
        raise _BenchmarkError('Unknown isolated RSS operation')
    state = _LiveState(module, config)

    def work() -> dict[str, Any]:
        if name == 'rss_upload':
            state._prepare_rss(name)
        client = state._client()
        capture = _RouteCapture()
        if config['client'] == 'ours':
            client.config.on_request = capture
        resource_module = importlib.import_module('resource')
        baseline_raw = resource_module.getrusage(resource_module.RUSAGE_SELF).ru_maxrss
        if name == 'rss_upload':
            result = client.upload(
                'Version',
                cast('int', state.version['id']),
                str(state.multipart_payload),
                field_name='sg_uploaded_movie',
                display_name=f'{state.namespace}-rss-upload.bin',
            )
            peak_raw = resource_module.getrusage(resource_module.RUSAGE_SELF).ru_maxrss
            if isinstance(result, int) and not isinstance(result, bool):
                state._record_resource('Attachment', result)
            correct = _verify_attachment(
                state,
                result,
                _AttachmentExpectation(
                    'sg_uploaded_movie',
                    cast('int', state.version['id']),
                    'Version',
                    state.multipart_payload,
                ),
            )
            route = (
                'observed:' + '+'.join(sorted(capture.routes))
                if capture.routes
                else _operation_route(name, state)
            )
            _validate_route_evidence(cast('str', config['client']), 'upload_multipart', route)
        else:
            destination = state.work_root / 'rss-download.bin'
            result = client.download_attachment(
                cast('int', config['download_attachment_id']),
                file_path=str(destination),
            )
            peak_raw = resource_module.getrusage(resource_module.RUSAGE_SELF).ru_maxrss
            try:
                correct = (
                    isinstance(result, str)
                    and Path(result).resolve() == destination.resolve()
                    and _file_digest(destination) == config['expected_download_sha256']
                )
            except OSError:
                correct = False
            route = cast('str', config['download_route'])
        if not correct:
            raise _CorrectnessError(f'benchmark correctness failed for {name}')
        baseline = _normalize_peak_rss(baseline_raw, sys.platform)
        peak = _normalize_peak_rss(peak_raw, sys.platform)
        return {
            'client': config['client'],
            'operations': {},
            'rss': {
                name: {
                    'baseline_bytes': baseline,
                    'correct': True,
                    'peak_bytes': peak,
                    'peak_delta_bytes': max(0, peak - baseline),
                    'route': route,
                }
            },
        }

    cleanup = state._cleanup if name == 'rss_upload' else state._cleanup_clients
    return _run_with_cleanup(work, cleanup)


def _workload_budget(sample_count: int) -> dict[str, int]:
    """Return bounded server-resource and transfer totals for one full run."""
    if sample_count > _MAX_SAMPLE_COUNT:
        raise _BenchmarkError(
            f'Benchmark workload budget exceeds {_MAX_SAMPLE_COUNT} paired rounds'
        )
    client_rounds = sample_count * len(_CLIENTS)
    latency_resources = _ENTITY_COUNT + 1 + 1 + 2 + 2 + (3 * 2)
    rss_resources = 4
    upload_bytes = (
        _DIRECT_PAYLOAD_BYTES
        + 2 * (_GENERIC_PAYLOAD_BYTES + _DIRECT_PAYLOAD_BYTES + _LARGE_PAYLOAD_BYTES)
        + 2 * _LARGE_PAYLOAD_BYTES
    )
    return {
        'created_entities': client_rounds * (latency_resources + rss_resources),
        'upload_bytes': client_rounds * upload_bytes,
        'workers': len(_CLIENTS) + client_rounds * (2 + len(_RSS_OPERATIONS)),
    }


def _aggregate_results(
    rounds: Mapping[str, Sequence[Mapping[str, Any]]],
    evidence: _ArtifactEvidence,
    sample_count: int,
    seed: int,
) -> dict[str, Any]:
    """Aggregate validated round results without accepting raw response data."""
    clients: dict[str, Any] = {}
    for client_name in _CLIENTS:
        client_rounds = rounds[client_name]
        if len(client_rounds) != sample_count:
            raise _BenchmarkError('Benchmark round count is incomplete')
        operations: dict[str, Any] = {}
        rss: dict[str, Any] = {}
        for name in _LATENCY_OPERATIONS:
            cold = [cast('float', result['operations'][name]['cold']) for result in client_rounds]
            warm = [cast('float', result['operations'][name]['warm']) for result in client_rounds]
            routes = sorted(
                {cast('str', result['operations'][name]['route']) for result in client_rounds}
            )
            operations[name] = {
                'category': _OPERATION_CATEGORIES[name],
                'cold': _sample_summary(cold, tail_percentile=_TAIL_PERCENTILE),
                'correct': True,
                'route': ';'.join(routes),
                'warm': _sample_summary(warm, tail_percentile=_TAIL_PERCENTILE),
            }
        for name in _RSS_OPERATIONS:
            rss[name] = {
                'baseline_bytes': [
                    cast('int', result['rss'][name]['baseline_bytes']) for result in client_rounds
                ],
                'correct': True,
                'peak_bytes': [
                    cast('int', result['rss'][name]['peak_bytes']) for result in client_rounds
                ],
                'peak_delta_bytes': [
                    cast('int', result['rss'][name]['peak_delta_bytes'])
                    for result in client_rounds
                ],
                'route': ';'.join(
                    sorted({cast('str', result['rss'][name]['route']) for result in client_rounds})
                ),
            }
        clients[client_name] = {
            'operations': operations,
            'rss': rss,
            'version': evidence.version if client_name == 'ours' else _OFFICIAL_VERSION,
        }
    return {
        'artifact_sha256': evidence.sha256,
        'candidate_commit': evidence.candidate_commit,
        'cleanup': 'PASS',
        'clients': clients,
        'machine': _machine_metadata(),
        'official_commit': _OFFICIAL_COMMIT,
        'official_ref': _OFFICIAL_REF,
        'sample_count': sample_count,
        'seed': seed,
        'site': _validated_site(os.environ['SG_TEST_SITE']),
        'tail_percentile': _TAIL_PERCENTILE,
        'workload': _workload_budget(sample_count),
    }


def _report_context(result: Mapping[str, Any]) -> tuple[str, dict[str, Any], int, int]:
    """Validate report metadata and return its rendering context."""
    required = {
        'artifact_sha256',
        'candidate_commit',
        'clients',
        'cleanup',
        'machine',
        'official_commit',
        'official_ref',
        'sample_count',
        'seed',
        'site',
        'tail_percentile',
        'workload',
    }
    if set(result) != required:
        raise _BenchmarkError('Benchmark report result has unexpected fields')
    _validate_report_identity(result)
    clients = result['clients']
    if not isinstance(clients, dict):
        raise _BenchmarkError('Benchmark report client inventory is invalid')
    typed_clients = cast('dict[str, Any]', clients)
    if set(typed_clients) != set(_CLIENTS):
        raise _BenchmarkError('Benchmark report client inventory is invalid')
    sample_count = result['sample_count']
    seed = result['seed']
    tail = result['tail_percentile']
    if isinstance(sample_count, bool) or not isinstance(sample_count, int) or sample_count < 1:
        raise _BenchmarkError('Benchmark report sample count is invalid')
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise _BenchmarkError('Benchmark report seed is invalid')
    if tail != _TAIL_PERCENTILE:
        raise _BenchmarkError('Benchmark report tail percentile is invalid')
    if result['workload'] != _workload_budget(sample_count):
        raise _BenchmarkError('Benchmark report workload budget is invalid')
    _validated_machine(result['machine'])
    return _validated_site(result.get('site')), typed_clients, sample_count, seed


def _wrapped_paragraph_lines(lines: list[str]) -> list[str]:
    """Wrap one generated report paragraph at the repository Markdown width."""
    return textwrap.wrap(
        ' '.join(lines),
        width=_REPORT_WRAP_WIDTH,
        break_long_words=False,
        break_on_hyphens=False,
    )


def _report_header_lines(
    result: Mapping[str, Any],
    site: str,
    clients: Mapping[str, Any],
    sample_count: int,
    seed: int,
) -> list[str]:
    """Return benchmark report evidence and methodology lines."""
    return [
        '# Benchmarks',
        '',
        'Results status: complete',
        '',
        '## Evidence',
        '',
        f'- Site: `{site}`',
        f'- Our version: `{clients["ours"]["version"]}`',
        f'- Artifact SHA-256: `{result["artifact_sha256"]}`',
        f'- Official ref: `{result["official_ref"]}`',
        f'- Official version: `{clients["official"]["version"]}`',
        f'- Python version: `{result["machine"]["python_version"]}`',
        f'- OS: `{result["machine"]["os"]}`',
        f'- Platform: `{result["machine"]["platform"]}`',
        f'- Architecture: `{result["machine"]["architecture"]}`',
        f'- Paired sample count: `{sample_count}`',
        f'- Ordering seed: `{seed}`',
        f'- Worker budget: `{result["workload"]["workers"]}`',
        f'- Created-entity budget: `{result["workload"]["created_entities"]}`',
        f'- Upload-byte budget: `{result["workload"]["upload_bytes"]}`',
        '- Tail statistic: nearest-rank p95',
        '- Correctness status: `PASS`',
        '- Cleanup status: `PASS`',
        '',
        '## Methodology',
        '',
        *_wrapped_paragraph_lines(
            [
                'Each client ran in a separate isolated Python process.',
                'The replacement was loaded only from the hash-verified validated wheel',
                'installation. The official client was loaded only from the caller-supplied',
                'checkout at the recorded official ref.',
                'Client order was pairwise counterbalanced from the recorded seed.',
                'Every result was checked before its elapsed time was accepted.',
                'Cold and warm samples remain separate.',
                'Transfer-only RSS workers record normalized process-high-water baselines,',
                'peaks, and peak deltas, never as latency samples.',
                'Upload and download correctness uses exact generated-file hashes.',
                'Every isolated worker has a 3,600-second wall-clock timeout.',
                'After any live worker failure, a fresh serial worker recovers',
                'journaled resources and verifies the benchmark namespace.',
                'Cleanup failure or timeout blocks report',
                'publication. At the default 2 paired rounds, the workload budget is 18 worker',
                'processes, 184 server entities, and 332,032,784 uploaded bytes. At the',
                '`BENCH_N=20` hard ceiling, the budget is 162 worker processes, 1,840 server',
                'entities, and 3,320,327,840 uploaded bytes.',
                'RSS download uses a fresh download-only',
                'process after setup. A larger even `BENCH_N` up to 20 remains an explicit',
                'publication choice.',
                'The official row uses one thread-owned client per persistent thread, while the',
                'replacement row shares one client across four persistent threads.',
                'Both issue four',
                'concurrent reads and exclude thread and client setup and closure from timing.',
            ]
        ),
        '',
    ]


def _render_report(result: Mapping[str, Any]) -> str:
    """Render one validated, structured, ASCII benchmark result."""
    site, clients, sample_count, seed = _report_context(result)
    lines = _report_header_lines(result, site, clients, sample_count, seed)
    lines.extend(_report_latency_lines(clients))
    lines.extend(_report_rss_lines(clients))
    lines.extend(
        [
            '',
            '## Limitations',
            '',
            *_wrapped_paragraph_lines(
                [
                    'These measurements describe the recorded site, artifact, official checkout,',
                    'machine, and run conditions only.',
                    'Network and server load are not controlled.',
                    'Route labels are observed from public operation metadata or live server',
                    'capability evidence.',
                    'No result establishes a legacy fallback route on clouds where that route',
                    'was not observed.',
                    'Nearest-rank p95 is descriptive at the recorded sample count.',
                    'At the default 2 samples it is the maximum observed value and is',
                    'descriptive. A maintainer may explicitly choose a larger even `BENCH_N`',
                    'up to 20 for publication.',
                    'RSS deltas approximate transfer growth in process high-water RSS and are not',
                    'allocation profiles.',
                    'This page makes no unsupported claim that either client is faster.',
                ]
            ),
            '',
        ]
    )
    rendered = '\n'.join(lines)
    try:
        rendered.encode('ascii')
    except UnicodeEncodeError as error:
        raise _BenchmarkError('Benchmark report contains non-ASCII content') from error
    return rendered


def _write_all_and_sync(descriptor: int, payload: bytes) -> None:
    """Write every byte and sync a durable file descriptor."""
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError('durable write made no progress')
        offset += written
    os.fsync(descriptor)


def _write_atomic_snapshot(destination: Path, payload: bytes, failure_message: str) -> None:
    """Replace one owner-only snapshot after durable write and close."""
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f'.{destination.name}.',
        )
    except OSError:
        raise _BenchmarkError(failure_message) from None
    temporary = Path(temporary_name)
    write_capture = _FailureCapture()
    with write_capture:
        _write_all_and_sync(descriptor, payload)
    close_capture = _FailureCapture()
    with close_capture:
        os.close(descriptor)
    replace_capture = _FailureCapture()
    if write_capture.error is None and close_capture.error is None:
        with replace_capture:
            temporary.replace(destination)
    unlink_capture = _FailureCapture()
    with unlink_capture:
        temporary.unlink(missing_ok=True)
    _raise_serialization_errors(
        failure_message,
        (
            write_capture.error,
            close_capture.error,
            replace_capture.error,
            unlink_capture.error,
        ),
    )


def _write_payload(path: Path, size: int) -> None:
    """Create one deterministic nontrivial transfer payload."""
    digest = hashlib.sha256(b'sg-python-api benchmark transfer payload').digest()
    block = digest * (_PAYLOAD_BLOCK_SIZE // len(digest))
    remaining = size
    with path.open('wb') as handle:
        while remaining >= len(block):
            handle.write(block)
            remaining -= len(block)
        if remaining:
            handle.write(block[:remaining])


def _write_report_atomic(destination: Path, result: Mapping[str, Any]) -> None:
    """Atomically write a sanitized structured benchmark report."""
    rendered = _render_report(result)
    destination.parent.mkdir(parents=True, exist_ok=True)
    _write_atomic_snapshot(
        destination,
        rendered.encode('ascii'),
        'Could not atomically write benchmark report',
    )


def _write_worker_config(path: Path, config: dict[str, object]) -> None:
    """Write a mode-0600 transient worker configuration."""
    try:
        encoded = (json.dumps(config, sort_keys=True) + '\n').encode('ascii')
    except (TypeError, UnicodeEncodeError) as error:
        raise _BenchmarkError('Worker configuration is not sanitized ASCII data') from error
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except OSError:
        raise _BenchmarkError('Could not write complete worker configuration') from None
    written: int | None = None
    write_capture = _FailureCapture()
    with write_capture:
        written = os.write(descriptor, encoded)
    close_capture = _FailureCapture()
    with close_capture:
        os.close(descriptor)
    _raise_serialization_errors(
        'Could not write complete worker configuration',
        (write_capture.error, close_capture.error),
    )
    if written != len(encoded):
        raise _BenchmarkError('Could not write complete worker configuration')


def _recover_worker(
    config: Mapping[str, object],
    root: Path,
    sequence: int,
) -> None:
    """Run one isolated cleanup worker and require complete recovery."""
    config_path = root / f'cleanup-{sequence}.json'
    recovery_capture = _FailureCapture()
    with recovery_capture:
        _ResourceJournal._load(root)
        cleanup_config = {field: config[field] for field in _WORKER_BASE_FIELDS}
        cleanup_config['mode'] = 'cleanup'
        _write_worker_config(config_path, cleanup_config)
        result = _launch_worker(config_path, root)
        if result != {'cleanup': 'PASS'}:
            raise _CleanupError('benchmark worker recovery failed')
        if _ResourceJournal._load(root)._resources:
            raise _CleanupError('benchmark worker recovery failed')
    removal_capture = _FailureCapture()
    with removal_capture:
        config_path.unlink(missing_ok=True)
    recovery_process_control = _process_control(recovery_capture.error)
    if recovery_process_control is not None:
        raise recovery_process_control
    removal_process_control = _process_control(removal_capture.error)
    if removal_process_control is not None:
        raise removal_process_control
    if recovery_capture.error is not None or removal_capture.error is not None:
        raise _CleanupError('Benchmark worker recovery failed')


def _launch_configured_worker(
    config: dict[str, object],
    root: Path,
    sequence: int,
) -> dict[str, Any]:
    """Persist, run, and remove one transient worker configuration."""
    config_path = root / f'worker-{sequence}.json'
    live_worker = config.get('mode') in _WORKER_MODES - {'cleanup', 'preflight'}
    if live_worker:
        _ResourceJournal._create(root)
    _write_worker_config(config_path, config)

    def work() -> dict[str, Any]:
        worker_capture = _FailureCapture()
        result: dict[str, Any] | None = None
        with worker_capture:
            result = _launch_worker(config_path, root)
        if not live_worker:
            if worker_capture.error is not None:
                raise worker_capture.error
            return cast('dict[str, Any]', result)
        journal_capture = _FailureCapture()
        journal: _ResourceJournal | None = None
        with journal_capture:
            journal = _ResourceJournal._load(root)
        journal_nonempty = journal is not None and bool(journal._resources)
        recovery_capture = _FailureCapture()
        with recovery_capture:
            if (
                journal_capture.error is not None
                or journal_nonempty
                or worker_capture.error is not None
            ):
                _recover_worker(config, root, sequence)
            if journal_capture.error is not None:
                raise _CleanupError('Benchmark worker recovery failed')
        _raise_worker_errors(worker_capture.error, journal_capture.error, recovery_capture.error)
        if journal_nonempty:
            raise _BenchmarkError('Benchmark worker cleanup contract failed')
        return cast('dict[str, Any]', result)

    return _run_with_cleanup(
        work,
        lambda: config_path.unlink(missing_ok=True),
    )


def _controller(repo_root: Path) -> int:
    """Preflight, execute counterbalanced isolated workers, and publish results."""
    _preflight_tools()
    evidence = _load_artifact_evidence(repo_root)
    official_root = _official_checkout(repo_root, Path(os.environ['SG_OFFICIAL_CLIENT']))
    _validated_site(os.environ['SG_TEST_SITE'])
    try:
        sample_count = int(os.environ.get('BENCH_N', str(_DEFAULT_SAMPLE_COUNT)))
        seed = int(os.environ.get('BENCH_SEED', '0'))
    except ValueError as error:
        raise _BenchmarkError('BENCH_N and BENCH_SEED must be integers') from error
    _workload_budget(sample_count)
    orders = _client_orders(sample_count, seed=seed)
    with tempfile.TemporaryDirectory(prefix='sg-python-api-benchmark-') as temporary_name:
        root = _validate_disposable_path(Path(temporary_name), repo_root)
        install_root = root / 'installed'
        install_root.mkdir()
        candidate_root = _install_candidate(evidence, install_root)
        client_configs = {
            'official': _ClientLocation(
                'official', official_root, official_root, _OFFICIAL_VERSION
            ),
            'ours': _ClientLocation('ours', candidate_root, install_root, evidence.version),
        }
        rounds: dict[str, list[dict[str, Any]]] = {'official': [], 'ours': []}
        sequence = 0
        for client_name in _CLIENTS:
            location = client_configs[client_name]
            preflight_root = root / f'preflight-{client_name}'
            preflight_root.mkdir()
            base = _worker_base_config(
                location=location,
                namespace=f'preflight-{client_name}',
                round_number=0,
                work_root=preflight_root,
            )
            preflight = _launch_configured_worker(
                {**base, 'mode': 'preflight'},
                preflight_root,
                sequence,
            )
            sequence += 1
            _validate_preflight_result(preflight, location)
        for round_number, order in enumerate(orders):
            for client_name in order:
                location = client_configs[client_name]
                worker_root = root / f'{round_number}-{client_name}'
                worker_root.mkdir()
                base = _worker_base_config(
                    location=location,
                    namespace=_new_namespace(client_name, round_number),
                    round_number=round_number,
                    work_root=worker_root,
                )
                latency = _launch_configured_worker(
                    {**base, 'mode': 'latency'}, worker_root, sequence
                )
                sequence += 1
                combined = latency
                for rss_name in _RSS_OPERATIONS:
                    rss_root = root / f'{round_number}-{client_name}-{rss_name}'
                    rss_root.mkdir()
                    rss_base = _worker_base_config(
                        location=location,
                        namespace=_new_namespace(client_name, round_number),
                        round_number=round_number,
                        work_root=rss_root,
                    )
                    rss_result = _launch_configured_worker(
                        {
                            **rss_base,
                            'mode': (
                                'rss_download_setup' if rss_name == 'rss_download' else 'rss'
                            ),
                            'rss_operation': rss_name,
                        },
                        rss_root,
                        sequence,
                    )
                    sequence += 1
                    combined['rss'].update(rss_result['rss'])
                _validate_round_result(combined)
                rounds[client_name].append(combined)
        result = _aggregate_results(rounds, evidence, sample_count, seed)
        _write_report_atomic(repo_root / 'dist' / 'benchmarks' / 'benchmark-results.md', result)
    print(f'benchmark results written: {sample_count} paired rounds')
    return 0


def _rss_download_setup_worker(
    module: ModuleType,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Create, hand off, and clean one large download fixture."""
    state = _LiveState(module, config)

    def work() -> dict[str, Any]:
        state._prepare_rss('rss_download')
        measured_root = state.work_root / 'rss-download-worker'
        measured_root.mkdir()
        measured_config: dict[str, object] = {
            **config,
            'download_attachment_id': state.download_attachment_id,
            'download_route': 'download',
            'expected_download_sha256': _file_digest(state.multipart_payload),
            'mode': 'rss',
            'work_root': str(measured_root),
        }
        return _launch_configured_worker(measured_config, measured_root, 0)

    return _run_with_cleanup(work, state._cleanup)


def _worker_result(config_path: Path) -> dict[str, Any]:
    """Run one validated worker mode and return its sanitized result."""
    config = _read_worker_config(config_path)
    work_root = Path(cast('str', config['work_root']))
    if config_path.parent.resolve() != work_root.resolve():
        raise _BenchmarkError('Worker configuration is outside its disposable root')
    module = _import_worker_client(config)
    mode = config['mode']
    if mode == 'cleanup':
        result = _cleanup_worker(module, config)
    elif mode == 'latency':
        result = _latency_worker(module, config)
    elif mode == 'preflight':
        result = _preflight_worker(module, config)
    elif mode == 'rss_download_setup':
        result = _rss_download_setup_worker(module, config)
    else:
        result = _rss_worker(module, config)
    _verify_worker_modules(Path(cast('str', config['expected_root'])))
    return result


def _worker(config_path: Path) -> NoReturn:
    """Run the isolated worker entrypoint."""
    capture = _FailureCapture()
    result: dict[str, Any] | None = None
    with capture:
        result = _worker_result(config_path)
    if isinstance(capture.error, _BenchmarkError):
        print(str(capture.error), file=sys.stderr)
        raise SystemExit(1) from None
    process_control = _process_control(capture.error)
    if process_control is not None:
        raise process_control
    if capture.error is not None:
        print('Benchmark worker failed at an unexpected boundary', file=sys.stderr)
        raise SystemExit(1) from None
    print(json.dumps(result, sort_keys=True, separators=(',', ':')))
    raise SystemExit(0)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark harness.

    Args:
        argv: Optional command arguments.

    Returns:
        Process exit status.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker', type=Path, help=argparse.SUPPRESS)
    arguments = parser.parse_args(argv)
    _validate_operation_metadata()
    if arguments.worker is not None:
        _worker(arguments.worker)
    missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        parser.error(f'missing required environment variables: {", ".join(missing)}')
    return _controller(Path(__file__).resolve().parents[1])


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except _BenchmarkError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from None
