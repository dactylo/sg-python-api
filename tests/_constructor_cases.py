CONSTRUCTOR_TYPE_ERROR_CASES: list[tuple[tuple[object, ...], dict[str, object], str]] = [
    ((), {}, 'base_url'),
    (
        ('https://x.example',) * 13,
        {},
        'positional',
    ),
    (
        ('https://x.example',),
        {'unexpected': True},
        'unexpected',
    ),
    (
        ('https://x.example', 's'),
        {'script_name': 'other'},
        'script_name',
    ),
]
