"""Match current display names and construct bounded search results."""

import re
from collections.abc import Mapping
from itertools import groupby
from typing import cast

from ._errors import MockgunError, ServerFaultError
from ._links import derived_name, display_name, hidden
from ._query import ReadScope, check_collation, collation_key, project_rows, select
from ._schema import SiteSchema
from ._sessions import RequestContext
from ._store import Row, Store

_MIN_TEXT_LENGTH = 3
_SEARCHABLE_TYPES = {
    'Shot',
    'Asset',
    'Version',
    'Task',
    'Note',
    'Sequence',
    'Playlist',
    'Cut',
    'CutItem',
    'PublishedFile',
}
_UNSEARCHED_FIELDS = {
    'Shot': {'description'},
    'Asset': {'description'},
    'Task': {'sg_description'},
    'Version': {
        'client_code',
        'sg_department',
        'sg_path_to_movie',
        'sg_translation_type',
        'frame_range',
        'sg_path_to_frames',
        'description',
        'sg_path_to_geometry',
    },
}


def _natural_key(name: str) -> list[str | int]:
    """Split ASCII names into lowercased text runs and numeric digit runs."""
    return [int(part) if part.isdecimal() else part.lower() for part in re.split(r'(\d+)', name)]


def _select_candidates(
    store: Store,
    schema: SiteSchema,
    entity_types: dict[object, object],
    scope: ReadScope,
    *,
    short_text: bool,
) -> list[Row]:
    """Validate each filter and collect its selected rows in request order."""
    candidates: list[Row] = []
    for entity_type, tree in entity_types.items():
        if (
            not isinstance(entity_type, str)
            or not schema.declares(entity_type)
            or not isinstance(tree, dict)
        ):
            raise MockgunError('mockgun cannot query_display_name_cache this request yet')
        try:
            rows = select(
                store,
                entity_type,
                cast('dict[str, object]', tree),
                [],
                scope=scope,
            )
        except ServerFaultError as error:
            if short_text:
                raise MockgunError(
                    f'mockgun cannot query_display_name_cache short text and filter: {error}'
                ) from error
            raise
        candidates.extend(rows)
    return candidates


def _unsupported(row: Mapping[str, object], terms: list[str], reason: str) -> MockgunError:
    """Identify the search candidate and terms whose behavior remains unsettled."""
    return MockgunError(
        f'mockgun cannot query_display_name_cache {row["type"]} {row["id"]} '
        f'terms {terms!r}: {reason}'
    )


def _candidate_name(store: Store, row: Row, terms: list[str]) -> str:
    """Read a modeled, non-derived ASCII display name."""
    kind, identity = cast('str', row['type']), cast('int', row['id'])
    name = display_name(store, kind, identity)
    if name is None or not name.isascii() or derived_name(store, kind, identity):
        raise _unsupported(row, terms, 'unmodeled, non-ASCII or derived display name')
    return name


def _match_links(store: Store, schema: SiteSchema, row: Row, terms: list[str]) -> list[str]:
    """Return observed link labels, rejecting other link shapes."""
    kind = cast('str', row['type'])
    if kind in {'Task', 'Version'}:
        raw_link = row.get('entity')
        if isinstance(raw_link, dict) and hidden(store, row, 'entity', raw_link):
            raise _unsupported(row, terms, 'hidden entity')
        values = project_rows(
            store, schema, [row], ['entity'] if schema.declares(kind, 'entity') else []
        )[0]
        link = cast('dict[str, object] | None', values.get('entity'))
        if not isinstance(link, dict) or link.get('type') != 'Shot':
            raise _unsupported(row, terms, 'entity is not a visible Shot')
        metadata = cast('dict[str, object]', schema.entities.get('Shot', {}).get('name', {}))
        if metadata.get('value') != 'Shot':
            raise _unsupported(row, terms, 'Shot display label')
        shot_id = cast('int', link['id'])
        shot_name = display_name(store, 'Shot', shot_id)
        if shot_name is None or derived_name(store, 'Shot', shot_id):
            raise _unsupported(row, terms, 'Shot display name')
        return ['Shot', shot_name]
    if kind in {'Asset', 'Playlist'} and any(
        row.get(field)
        for field in schema.fields[kind]
        if field not in {'project', 'created_by', 'updated_by'}
        and schema.field_type(kind, field) in {'entity', 'multi_entity'}
    ):
        raise _unsupported(row, terms, 'stored entity links')
    return ['', '']


def _match(
    store: Store, schema: SiteSchema, row: Row, name: str, terms: list[str]
) -> dict[str, object]:
    """Project the observed match shape without reading unrelated linked values."""
    kind = cast('str', row['type'])
    links = _match_links(store, schema, row, terms)
    fields = [
        field for field in ('project', 'sg_status_list', 'image') if schema.declares(kind, field)
    ]
    if kind == 'Task' and schema.declares('Shot', 'image'):
        fields.append('entity.Shot.image')
    values = project_rows(store, schema, [row], fields)[0]
    project = values.get('project')
    if not isinstance(project, dict):
        raise _unsupported(row, terms, 'missing project')
    if values.get('image') is not None or values.get('entity.Shot.image') is not None:
        raise _unsupported(row, terms, 'image')
    return {
        'id': values['id'],
        'type': kind,
        'name': name,
        'project_id': project['id'],
        'links': links,
        'status': values.get('sg_status_list'),
        'image': None,
    }


def _matches_name(store: Store, schema: SiteSchema, row: Row, name: str, terms: list[str]) -> bool:
    """Match literal substrings while rejecting unsettled alternative search inputs."""
    lowered = name.lower()
    for term in terms:
        if (
            '_' in term
            and term not in lowered
            and re.search(re.escape(term).replace('_', '.'), lowered, flags=re.DOTALL)
        ):
            raise _unsupported(row, terms, f'wildcard interpretation of {term!r}')
    if row['type'] == 'Note' and '-' in terms:
        values = project_rows(store, schema, [row], ['subject', 'content'])[0]
        subject, content = values.get('subject'), values.get('content')
        if (
            isinstance(subject, str)
            and isinstance(content, str)
            and '-' not in subject
            and '-' not in content
        ):
            raise _unsupported(row, terms, 'Note name join')
    if all(term in lowered for term in terms):
        return True
    kind = cast('str', row['type'])
    fields = [
        field
        for field in schema.fields[kind]
        if field != 'cached_display_name'
        and field not in _UNSEARCHED_FIELDS.get(kind, set())
        and schema.field_type(kind, field) == 'text'
    ]
    values = project_rows(store, schema, [row], fields)[0]
    other_text = [value.lower() for field in fields if isinstance(value := values.get(field), str)]
    if all(term in lowered or any(term in value for value in other_text) for term in terms):
        raise _unsupported(row, terms, 'unobserved text fields')
    return False


def _matching_rows(
    store: Store, schema: SiteSchema, rows: list[Row], terms: list[str], project_ids: list[object]
) -> list[dict[str, object]]:
    """Keep supported matches after applying explicit Project selection."""
    matches: list[dict[str, object]] = []
    for row in rows:
        kind = cast('str', row['type'])
        if project_ids and schema.declares(kind, 'project'):
            project = project_rows(store, schema, [row], ['project'])[0].get('project')
            if isinstance(project, dict) and project['id'] not in project_ids:
                continue
        if kind == 'Reply':
            continue
        if kind not in _SEARCHABLE_TYPES:
            raise _unsupported(row, terms, 'unobserved candidate type')
        name = _candidate_name(store, row, terms)
        if _matches_name(store, schema, row, name, terms):
            match = _match(store, schema, row, name, terms)
            # C8 Q2 observed one Shot-linked Task with a single-token name.
            # Keep earlier answers for multi-row, spaced-name and multi-term forms.
            if (
                kind == 'Task'
                and not project_ids
                and len(rows) == 1
                and len(terms) == 1
                and not any(character.isspace() for character in name)
            ):
                continue
            matches.append(match)
    return matches


def _order_matches(matches: list[dict[str, object]], terms: list[str]) -> None:
    """Order matches only where position and every surviving name order agree."""
    if len(matches) <= 1:
        return
    error = _unsupported(matches[0], terms, 'unsettled match order')
    if len(terms) != 1:
        raise error
    term = terms[0]
    for match in matches:
        name = cast('str', match['name']).lower()
        if name.find(term, name.index(term) + 1) != -1:
            raise error
    positions = {cast('str', match['name']).lower().index(term) for match in matches}
    if len(positions) > 1 and len({match['type'] for match in matches}) > 1:
        raise error
    matches.sort(key=lambda match: cast('str', match['name']).lower().index(term))
    ordered: list[dict[str, object]] = []
    for _, group in groupby(
        matches, key=lambda match: cast('str', match['name']).lower().index(term)
    ):
        siblings = list(group)
        names = [cast('str', match['name']) for match in siblings]
        if (
            len({len(name) for name in names}) != 1
            or len({name.lower() for name in names}) != len(names)
            or len({tuple(_natural_key(name)) for name in names}) != len(names)
        ):
            raise error
        check_collation(
            names,
            entity_type=','.join(sorted({cast('str', match['type']) for match in siblings})),
            field_name='name',
        )
        canonical = sorted(names)
        if not (
            canonical
            == sorted(names, key=str.lower)
            == sorted(names, key=collation_key)
            == sorted(names, key=_natural_key)
        ):
            raise error
        siblings.sort(key=lambda match: collation_key(cast('str', match['name'])))
        ordered.extend(siblings)
    matches[:] = ordered


def _validate_project_ids(
    store: Store, schema: SiteSchema, ids: list[object], context: RequestContext
) -> None:
    """Require every requested Project to exist and remain active."""
    if not ids:
        return
    if not schema.declares('Project') or any(type(item) is not int for item in ids):
        raise MockgunError('mockgun cannot query_display_name_cache project_ids')
    tree: dict[str, object] = {
        'logical_operator': 'and',
        'conditions': [{'path': 'id', 'relation': 'in', 'values': ids}],
    }
    active = select(
        store,
        'Project',
        tree,
        [],
        scope=ReadScope(retired=False, context=context, include_archived_projects=True),
    )
    retired = select(
        store,
        'Project',
        tree,
        [],
        scope=ReadScope(retired=True, context=context, include_archived_projects=True),
    )
    active_ids = {row['id'] for row in project_rows(store, schema, active, ['id'])}
    retired_ids = {row['id'] for row in project_rows(store, schema, retired, ['id'])}
    if any(item not in active_ids or item in retired_ids for item in ids):
        raise MockgunError('mockgun cannot query_display_name_cache project_ids')


def _validate_text(query: str) -> None:
    """Reject search text outside the characterized ASCII vocabulary."""
    if (
        query.startswith(' ')
        or query.endswith(' ')
        or (
            len(query) >= _MIN_TEXT_LENGTH
            and sum(character != ' ' for character in query) < _MIN_TEXT_LENGTH
        )
        or any(
            not ('a' <= character <= 'z' or 'A' <= character <= 'Z' or '0' <= character <= '9')
            and character not in ' -.,_'
            for character in query
        )
    ):
        raise MockgunError('mockgun cannot query_display_name_cache text')


def search(
    store: Store, schema: SiteSchema, params: Mapping[str, object], *, context: RequestContext
) -> dict[str, object]:
    """Return current matching display names within the characterized search bounds.

    Raises:
        MockgunError: If request, matching, ordering or result values are
            unsettled.
        ServerFaultError: If the search text is too short or a filter is
            invalid.
    """
    if params.keys() - {'text', 'entity_types', 'project_ids', 'max_results'}:
        raise MockgunError('mockgun cannot query_display_name_cache unknown parameters')
    query = params.get('text')
    entity_types = params.get('entity_types')
    project_ids = params.get('project_ids')
    max_results = params.get('max_results')
    if (
        not isinstance(query, str)
        or not isinstance(entity_types, dict)
        or not entity_types
        or not isinstance(project_ids, list)
        or (max_results is not None and (type(max_results) is not int))
    ):
        raise MockgunError('mockgun cannot query_display_name_cache this request yet')
    if max_results is not None and max_results < 1:
        raise MockgunError('mockgun cannot query_display_name_cache max_results')
    _validate_text(query)
    requested_projects = cast('list[object]', project_ids)
    _validate_project_ids(store, schema, requested_projects, context)

    candidates = _select_candidates(
        store,
        schema,
        cast('dict[object, object]', entity_types),
        ReadScope(
            retired=False,
            context=context,
            include_archived_projects=bool(requested_projects),
        ),
        short_text=len(query) < _MIN_TEXT_LENGTH,
    )

    if len(query) < _MIN_TEXT_LENGTH:
        raise ServerFaultError(
            'API query_display_name_cache() query text too short! '
            f'must be 3 or more characters :\n{params!r}',
            error_code=103,
        )
    terms = query.lower().split()
    matches = _matching_rows(store, schema, candidates, terms, requested_projects)
    _order_matches(matches, terms)
    return {'matches': matches[:max_results], 'terms': terms}
