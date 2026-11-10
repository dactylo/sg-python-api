_CREATE_ENTRY_ERROR = (
    'user_subscriptions_create users must contain dictionaries with '
    'humanUserId and subscription keys'
)

SUBSCRIPTION_DUPLICATE_IDS = [('7', '7'), (7, 7), ('7', 7), (7, '7')]

SUBSCRIPTION_RESULT_CASES: list[tuple[object, bool]] = [
    ({}, True),
    ({'7': None, '9': None}, True),
    ({'7': None, '9': 'sensitive'}, False),
    ({'7': False}, False),
    ({'7': 0}, False),
    ({'7': ''}, False),
    ({'7': []}, False),
    ({'7': {}}, False),
]

SUBSCRIPTION_NON_MAPPING_RESULTS = [True, False, None, 1, 'sensitive', [], [None]]

SUBSCRIPTION_ENTRY_CASES = [
    (
        {'humanUserId': '7', 'subscription': 'artist'},
        'user_subscriptions_create users must be a list',
    ),
    (
        [{'humanUserId': '7', 'subscription': 'artist'}, 'sensitive-member'],
        _CREATE_ENTRY_ERROR,
    ),
    (
        [{'subscription': 'artist'}],
        _CREATE_ENTRY_ERROR,
    ),
    (
        [{'user_id': '7', 'subscription': 'artist'}],
        _CREATE_ENTRY_ERROR,
    ),
    (
        [{'humanUserId': '7'}],
        _CREATE_ENTRY_ERROR,
    ),
    (
        [{'humanUserId': 7.0, 'subscription': 'artist'}],
        'user_subscriptions_create users must contain string or integer humanUserId values',
    ),
    (
        [{'humanUserId': True, 'subscription': 'artist'}],
        'user_subscriptions_create users must contain string or integer humanUserId values',
    ),
    (
        [{'humanUserId': False, 'subscription': 'artist'}],
        'user_subscriptions_create users must contain string or integer humanUserId values',
    ),
    (
        [{'humanUserId': None, 'subscription': 'artist'}],
        'user_subscriptions_create users must contain string or integer humanUserId values',
    ),
    (
        [{'humanUserId': '7', 'subscription': 1}],
        'user_subscriptions_create users must contain valid subscription values',
    ),
    (
        [{'humanUserId': '7', 'subscription': True}],
        'user_subscriptions_create users must contain valid subscription values',
    ),
    (
        [{'humanUserId': '7', 'subscription': {'sensitive': 'value'}}],
        'user_subscriptions_create users must contain valid subscription values',
    ),
    (
        [{'humanUserId': '7', 'subscription': ['artist', 2]}],
        'user_subscriptions_create users must contain valid subscription values',
    ),
    (['sensitive-member'], _CREATE_ENTRY_ERROR),
]

SUBSCRIPTION_READ_ERROR_CASES = [
    (['sensitive-result'], 'user_subscriptions_read did not return a dictionary'),
    ({'': 'artist'}, 'user_subscriptions_read returned an invalid user ID'),
    ({'7': {'sensitive': 'value'}}, 'user_subscriptions_read returned an invalid subscription'),
    ({'7': 1}, 'user_subscriptions_read returned an invalid subscription'),
    ({'7': True}, 'user_subscriptions_read returned an invalid subscription'),
    ({'7': ['artist', 2]}, 'user_subscriptions_read returned an invalid subscription'),
]
