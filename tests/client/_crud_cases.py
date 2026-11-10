from collections.abc import Callable

INVALID_RETURN_FIELDS = ['code', '', b'code', b'', 0, 1, False, ['code', 1], ('code', None)]

FIND_METHOD_NAMES = ['find', 'find_one']

STRING_FIELDS = ['code', b'code']

STRING_FIELD_SEQUENCES = [['code'], ('code',)]

RETURN_FIELD_CASES: list[tuple[Callable[[], object], list[str]]] = [
    (lambda: None, ['id']),
    (list, ['id']),
    (tuple, ['id']),
    (lambda: ['code', ''], ['code', '']),
    (lambda: ('code', ''), ['code', '']),
    (lambda: (field for field in ['code', '']), ['code', '']),
    (lambda: (field for field in ''), []),
]
