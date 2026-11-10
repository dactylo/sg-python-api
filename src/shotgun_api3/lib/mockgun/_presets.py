"""Validate and select the observed additional filter preset combinations."""

import datetime
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import cast

from ._errors import MockgunError, ServerFaultError
from ._links import RecordState, hidden
from ._schema import SiteSchema
from ._store import FieldValue, Row, Store

_ASSUME: ContextVar[Mapping[tuple[str, int], RecordState] | None] = ContextVar(
    'mockgun_preset_assume', default=None
)
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
_LATEST_MODES = {
    'Version': ('ENTITIES_CREATED_AT', 'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT'),
    'PublishedFile': ('ENTITIES_CREATED_AT', 'VERSION_NUMBER'),
    'Cut': ('REVISION_NUMBER',),
}
_MAX_COMBINED_PRESETS = 2


@dataclass(frozen=True)
class Preset:
    """Represent an immutable validated preset and its selection arguments.

    Attributes:
        name: `LATEST` or `CUT_SHOT_VERSIONS`.
        latest_by: Ranking mode for `LATEST`, otherwise `None`.
        cut_id: Cut identifier for `CUT_SHOT_VERSIONS`, or `None` when no Cut
            was supplied.
    """

    name: str
    latest_by: str | None = None
    cut_id: int | None = None


def _error(entity_type: str, detail: str, name: str = 'LATEST') -> MockgunError:
    """Name the preset and its affected field or group in an unsettled case."""
    return MockgunError(f'mockgun cannot select {entity_type} {name} preset: {detail}')


def _identity(value: FieldValue) -> tuple[str, int] | None:
    """Return a stored link's identity, treating a null link as its own group."""
    if value is None:
        return None
    link = cast('Row', value)
    return cast('str', link['type']), cast('int', link['id'])


def _cut_items(store: Store, cut_id: int | None) -> list[Row] | None:
    """Resolve reached CutItems with the preset's retirement and stored-cut checks."""
    if cut_id is None or store.row('Cut', cut_id) is None:
        return None
    items: list[Row] = []
    for retired in (False, True):
        for item in store.rows('CutItem', retired=retired):
            # A retired Cut still resolves; reading CutItem.cut through hidden() would
            # incorrectly hide precisely that supported case.
            store.check_known(item, 'cut')
            if _identity(item.get('cut')) != ('Cut', cut_id):
                continue
            assumed = (_ASSUME.get() or {}).get(
                (cast('str', item['type']), cast('int', item['id']))
            )
            if store.tainted(item) or (assumed.retired if assumed is not None else retired):
                raise _error('Version', 'retired CutItem.cut entity link', 'CUT_SHOT_VERSIONS')
            items.append(item)
    return items


def _known_value(store: Store, row: Row, field: str) -> FieldValue:
    """Read a stored preset value after rejecting its uncertainty marker."""
    store.check_known(row, field)
    return row.get(field)


def _linked(store: Store, link: FieldValue, entity_type: str) -> Row | None:
    """Resolve a stored link without schema, retirement or group checks."""
    identity = _identity(link)
    if identity is None:
        return None
    try:
        row = store.row(*identity)
    except ServerFaultError as exc:
        raise _error(entity_type, f'missing linked record {identity}') from exc
    if row is None:
        raise _error(entity_type, f'missing linked record {identity}')
    return row


def _pair(value: FieldValue) -> tuple[int, int]:
    """Encode a nullable integer or creation timestamp without losing microseconds."""
    if value is None:
        return 0, 0
    if isinstance(value, datetime.datetime):
        delta = value.astimezone(datetime.UTC) - _EPOCH
        return 1, (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
    return 1, cast('int', value)


def _read(store: Store, row: Row, field: str, preset: str = 'LATEST') -> FieldValue:
    """Read a declared preset field and reject missing or hidden link targets."""
    entity_type = cast('str', row['type'])
    if not store.schema.declares(entity_type, field):
        raise _error(entity_type, f'{entity_type}.{field} is undeclared', preset)
    store.check_known(row, field)
    value = row.get(field)
    if isinstance(value, dict):
        if hidden(store, row, field, value, assume=_ASSUME.get()):
            raise _error(entity_type, f'{entity_type}.{field} is a hidden entity link', preset)
        _linked(store, value, entity_type)
    return value


def _cut_versions(store: Store, rows: list[Row], cut_id: int | None) -> list[Row]:
    """Select Versions by active CutItems' shot links, ignoring their versions."""
    items = _cut_items(store, cut_id)
    if items is None:
        return []
    shots: set[tuple[str, int] | None] = set()
    for item in items:
        shot = _read(store, item, 'shot', 'CUT_SHOT_VERSIONS')
        identity = _identity(shot)
        if identity is not None and identity[0] == 'Shot':
            shots.add(identity)
    return [
        row for row in rows if _identity(_read(store, row, 'entity', 'CUT_SHOT_VERSIONS')) in shots
    ]


def _step(store: Store, row: Row) -> Row | None:
    """Read pipeline links with their hidden-link and declaration guards."""
    task = _linked(store, _read(store, row, 'sg_task'), 'Version')
    if task is None:
        return None
    step = _linked(store, _read(store, task, 'step'), 'Version')
    if step is not None and _read(store, step, 'list_order') is None:
        raise _error('Version', 'Step.list_order number is null')
    return step


def _validate_fields(schema: SiteSchema, entity_type: str, preset: Preset) -> None:
    """Require declared selection fields even when the candidate set is empty."""
    fields = [(entity_type, 'entity')]
    if preset.name == 'CUT_SHOT_VERSIONS':
        fields += [('CutItem', 'cut'), ('CutItem', 'shot')]
    else:
        if entity_type == 'PublishedFile':
            fields += [(entity_type, field) for field in ('task', 'name', 'published_file_type')]
        if entity_type == 'Cut':
            fields += [('Cut', 'code'), ('Cut', 'revision_number')]
        if preset.latest_by == 'VERSION_NUMBER':
            fields.append((entity_type, 'version_number'))
        else:
            fields.append((entity_type, 'created_at'))
        if preset.latest_by == 'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT':
            fields += [('Version', 'sg_task'), ('Task', 'step'), ('Step', 'list_order')]
    for kind, field in fields:
        if not schema.declares(kind, field):
            raise _error(entity_type, f'{kind}.{field} is undeclared', preset.name)


def _validate_group(store: Store, entity_type: str, group: list[Row], mode: str) -> None:
    """Check field and pipeline invariants before comparing ranking keys."""
    if entity_type == 'PublishedFile':
        types = {_identity(_read(store, row, 'published_file_type')) for row in group}
        if len(types) > 1:
            raise _error(entity_type, 'published_file_type entity differs within group')
    if mode == 'BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT':
        steps = [step for row in group if (step := _step(store, row)) is not None]
        if steps:
            best = max(cast('int', step['list_order']) for step in steps)
            if len({cast('int', step['id']) for step in steps if step['list_order'] == best}) > 1:
                raise _error(entity_type, 'Step.list_order number has distinct best Steps')
    if (
        mode == 'VERSION_NUMBER'
        and len(group) > 1
        and any(_known_value(store, row, 'version_number') is None for row in group)
    ):
        raise _error(entity_type, 'version_number number is null in a comparison')
    if (
        mode == 'REVISION_NUMBER'
        and len(group) > 1
        and all(_known_value(store, row, 'revision_number') is None for row in group)
    ):
        raise _error(entity_type, 'revision_number number is null throughout group')


def _validate_latest(entity_type: str, mode: object) -> None:
    """Reject unsupported latest modes, preserving ambiguous validation precedence."""
    if entity_type not in _LATEST_MODES:
        if mode is None or not any(mode in modes for modes in _LATEST_MODES.values()):
            raise _error(entity_type, 'unsupported type and latest_by')
        raise ServerFaultError(
            f'API read() LATEST preset is not supported for {entity_type}', error_code=103
        )
    if mode is None:
        raise ServerFaultError('API read() LATEST preset requires latest_by', error_code=104)
    if mode not in _LATEST_MODES[entity_type]:
        raise ServerFaultError(
            f'API read() latest_by {mode} is not valid for {entity_type}', error_code=104
        )


def _validate_item(entity_type: str, item: object) -> Preset:
    """Distinguish one observed rejection from overlapping or unsettled failures."""
    if not isinstance(item, dict):
        raise _error(entity_type, 'preset item is not a dictionary')
    data = cast('dict[str, object]', item)
    if data.keys() - {'preset_name', 'latest_by', 'cut_id'}:
        raise _error(entity_type, 'unknown preset keys')
    if 'cut_id' in data and type(data['cut_id']) is not int:
        raise _error(entity_type, 'cut_id must be an int')
    name, mode = data.get('preset_name'), data.get('latest_by')
    if name == 'LATEST':
        _validate_latest(entity_type, mode)
    elif name == 'CUT_SHOT_VERSIONS':
        if entity_type != 'Version':
            if 'cut_id' not in data:
                raise _error(
                    entity_type, 'unsupported type and missing cut_id', 'CUT_SHOT_VERSIONS'
                )
            raise ServerFaultError(
                f'API read() CUT_SHOT_VERSIONS preset is not supported for {entity_type}',
                error_code=103,
            )
    else:
        raise ServerFaultError(f'API read() unknown preset_name {name}', error_code=103)
    if (name == 'LATEST' and 'cut_id' in data) or (
        name == 'CUT_SHOT_VERSIONS' and 'latest_by' in data
    ):
        raise _error(entity_type, 'unsupported preset argument combination')
    return Preset(
        cast('str', name), cast('str | None', mode), cast('int | None', data.get('cut_id'))
    )


@contextmanager
def assuming(
    assume: Mapping[tuple[str, int], RecordState] | None,
) -> Generator[None, None, None]:
    """Use link-state assumptions for this scope and restore the enclosing state.

    Args:
        assume: States keyed by `(entity_type, entity_id)`, or `None` to
            evaluate this scope without inherited link-state assumptions.
    """
    token = _ASSUME.set(assume)
    try:
        yield
    finally:
        _ASSUME.reset(token)


def latest_groups(store: Store, entity_type: str, rows: list[Row]) -> list[list[Row]]:
    """Group eligible LATEST candidates using their stored membership fields.

    Versions group by entity, PublishedFiles by entity, task and name, and Cuts
    by code and entity. Non-Cut rows without an entity are excluded;
    PublishedFiles without a published-file type are also excluded.

    Args:
        store: Site records used to resolve grouping fields and links.
        entity_type: Candidate entity type.
        rows: Candidate rows in encounter order.

    Returns:
        Groups in first-encounter order, containing the original row objects in
        encounter order.

    Raises:
        MockgunError: If required grouping fields or linked records are
            undeclared, missing, hidden, unknown, or otherwise unsettled.
    """
    groups: dict[tuple[object, ...], list[Row]] = {}
    for row in rows:
        entity = _identity(_read(store, row, 'entity'))
        if entity_type != 'Cut' and entity is None:
            continue
        if entity_type == 'PublishedFile':
            if _read(store, row, 'published_file_type') is None:
                continue
            task = _identity(_read(store, row, 'task'))
            name = _read(store, row, 'name')
            if task is None or name is None:
                raise _error(entity_type, 'task entity or name text is null')
            key = (entity, task, name)
        elif entity_type == 'Cut':
            code = _read(store, row, 'code')
            if code is None:
                raise _error(entity_type, 'code text is null')
            key = (code, entity)
        else:
            key = (entity,)
        groups.setdefault(key, []).append(row)
    return list(groups.values())


def latest_rank_key(store: Store, entity_type: str, row: Row, latest_by: str) -> tuple[int, ...]:
    """Return a stored-value ranking key comparable within one LATEST group.

    No marker, schema, hidden-link or group checks run here. Greater keys rank
    higher.

    Args:
        store: Site records used to resolve task and pipeline-step links.
        entity_type: `Version`, `PublishedFile`, or `Cut`.
        row: Candidate row whose stored values supply the ranking key.
        latest_by: `ENTITIES_CREATED_AT` for Versions or PublishedFiles,
            `VERSION_NUMBER` for PublishedFiles, `REVISION_NUMBER` for Cuts, or
            `BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT` for Versions.

    Returns:
        A lexicographically comparable key for the requested ranking mode.

    Raises:
        MockgunError: If a linked record is missing or the entity type and
            ranking mode are unsupported together.
    """
    if latest_by not in _LATEST_MODES.get(entity_type, ()):
        raise _error(entity_type, f'unsupported latest_by {latest_by}')
    if latest_by == 'ENTITIES_CREATED_AT':
        return _pair(row.get('created_at'))
    if latest_by == 'VERSION_NUMBER':
        return _pair(row.get('version_number'))
    if latest_by == 'REVISION_NUMBER':
        return (
            *_pair(row.get('revision_number')),
            *_pair(row.get('created_at')),
        )
    task = _linked(store, row.get('sg_task'), entity_type)
    step = _linked(store, task.get('step'), entity_type) if task is not None else None
    if step is None:
        return (0, 0, 0, *_pair(row.get('created_at')))
    return (
        1,
        *_pair(step.get('list_order')),
        *_pair(row.get('created_at')),
    )


def _winner(store: Store, entity_type: str, group: list[Row], mode: str) -> Row:
    """Check unsettled group comparisons before choosing a unique greatest key."""
    _validate_group(store, entity_type, group, mode)
    fields = ('version_number',) if mode == 'VERSION_NUMBER' else ('created_at',)
    if mode == 'REVISION_NUMBER':
        fields += ('revision_number',)
    for row in group:
        for field in fields:
            store.check_known(row, field)
    keys = [latest_rank_key(store, entity_type, row, mode) for row in group]
    best_key = max(keys)
    if mode != 'VERSION_NUMBER':
        contenders = [
            row for row, key in zip(group, keys, strict=True) if key[:-2] == best_key[:-2]
        ]
        if len(contenders) > 1 and any(
            _known_value(store, row, 'created_at') is None for row in contenders
        ):
            raise MockgunError(
                f'mockgun cannot find/find_one {entity_type} LATEST preset: '
                'created_at date_time is null in the deciding comparison'
            )
    winners = [row for row, key in zip(group, keys, strict=True) if key == best_key]
    if len(winners) != 1:
        raise _error(entity_type, f'{mode} ranking key ties')
    return winners[0]


def apply_presets(
    store: Store, entity_type: str, rows: list[Row], presets: tuple[Preset, ...], *, retired: bool
) -> list[Row]:
    """Apply validated presets after filters and archive scope, before ordering.

    Link checks use the assumptions installed by the enclosing `assuming()`
    scope, if any.

    Args:
        store: Site records used by preset selection.
        entity_type: Candidate entity type.
        rows: Candidates already restricted by filters and archive scope.
        presets: Validated preset combination.
        retired: Whether candidates come from the retired-record scope.

    Returns:
        Selected original row objects. With no presets, returns `rows` itself.
        A LATEST preset selects no retired rows.

    Raises:
        MockgunError: If required schema, links, grouping, or ranking are
            unsettled, or CUT_SHOT_VERSIONS is requested for retired rows.
        ServerFaultError: If a required record type is undeclared.
    """
    for preset in presets:
        _validate_fields(store.schema, entity_type, preset)
    cut = next((preset for preset in presets if preset.name == 'CUT_SHOT_VERSIONS'), None)
    if cut is not None:
        if retired:
            raise _error(entity_type, 'retired_only is unsettled', cut.name)
        rows = _cut_versions(store, rows, cut.cut_id)
    latest = next((preset for preset in presets if preset.name == 'LATEST'), None)
    if latest is not None:
        if retired:
            return []
        rows = [
            _winner(store, entity_type, group, cast('str', latest.latest_by))
            for group in latest_groups(store, entity_type, rows)
        ]
    return rows


def preset_dependencies(store: Store, presets: tuple[Preset, ...]) -> list[tuple[Row, str]]:
    """Return external link reads reached even when no candidate survives.

    `CutItem.cut` is resolved as stored, including a retired Cut. Only the
    reached CutItems' shot links participate in uncertain visibility
    evaluation.

    Args:
        store: Site records used to resolve the preset's Cut and CutItems.
        presets: Validated preset combination.

    Returns:
        `(row, field_name)` pairs for live stored CutItem rows whose `shot`
        links must be considered, or an empty list when none are reached.

    Raises:
        MockgunError: If a reached CutItem's cut membership or retirement state
            is unsettled.
        ServerFaultError: If a required record type is undeclared.
    """
    cut = next((preset for preset in presets if preset.name == 'CUT_SHOT_VERSIONS'), None)
    return [] if cut is None else [(item, 'shot') for item in _cut_items(store, cut.cut_id) or []]


def validate_presets(schema: SiteSchema, entity_type: str, presets: object) -> tuple[Preset, ...]:
    """Validate preset arguments and required fields before reading candidates.

    Args:
        schema: Loaded field declarations.
        entity_type: Entity type being queried.
        presets: A list of dictionaries naming `LATEST` with `latest_by`, or
            `CUT_SHOT_VERSIONS` with optional `cut_id`. Only Version queries
            support combining those two presets.

    Returns:
        Validated presets in request order, with duplicate identical LATEST
        entries collapsed.

    Raises:
        ServerFaultError: If the request has a modeled invalid preset name,
            argument, or entity-type combination.
        MockgunError: If the combination or validation precedence is unsettled,
            or required fields are undeclared.
    """
    if not isinstance(presets, list):
        raise ServerFaultError(
            'API read() additional_filter_presets must be an array', error_code=103
        )
    parsed: list[Preset] = []
    names: list[object] = []
    faults: list[ServerFaultError] = []
    for item in cast('list[object]', presets):
        try:
            preset = _validate_item(entity_type, item)
        except ServerFaultError as exc:
            faults.append(exc)
            names.append(cast('dict[str, object]', item).get('preset_name'))
            continue
        if preset.name != 'LATEST' or preset not in parsed:
            parsed.append(preset)
            names.append(preset.name)
    if len(names) > 1 and (
        entity_type != 'Version'
        or len(names) != _MAX_COMBINED_PRESETS
        or 'LATEST' not in names
        or 'CUT_SHOT_VERSIONS' not in names
    ):
        raise _error(entity_type, 'unsupported preset combination')
    if len(faults) > 1:
        raise _error(entity_type, 'multiple invalid presets')
    if faults:
        raise faults[0]
    for preset in parsed:
        _validate_fields(schema, entity_type, preset)
    return tuple(parsed)
