from collections.abc import Callable
from typing import Literal, cast

from tests._live_safety import LiveBudget
from tests.live._verification import LiveCase, LiveTarget


class CleanupSite:
    def __init__(self, kind: str) -> None:
        self.kind = kind
        self.rows: dict[tuple[str, int], dict[str, object]] = {}
        self.follows: list[dict[str, object]] = []
        self.subscriptions: list[dict[str, object]] = []
        self.actions: list[str] = []
        self._next_id = 10
        self.lose = ''

    def _find(self, name: str, args: tuple[object, ...]) -> object:
        matches = [
            row.copy() for (entity_type, _), row in self.rows.items() if entity_type == args[0]
        ]
        for key, relation, value in cast('list[list[object]]', args[1]):
            matches = [
                row
                for row in matches
                if (
                    str(row.get(str(key), '')).startswith(str(value))
                    if relation == 'starts_with'
                    else row.get(str(key)) == value
                )
            ]
        return matches if name == 'find' else next(iter(matches), None)

    def client(self, **options: object) -> object:
        site = self

        class Client:
            def __getattr__(self, name: str) -> Callable[..., object]:
                def invoke(*args: object, **kwargs: object) -> object:
                    return site.invoke(name, args, {**kwargs, 'sudo': bool(options)})

                async def async_invoke(*args: object, **kwargs: object) -> object:
                    return invoke(*args, **kwargs)

                return async_invoke if site.kind == 'async' else invoke

        return Client()

    def invoke(self, name: str, args: tuple[object, ...], kwargs: dict[str, object]) -> object:
        self.actions.append(
            name + (':' + str(args[0]) if args and isinstance(args[0], str) else '')
        )
        read_only = {
            'close': None,
            'following': list(self.follows),
            'user_subscriptions_read': list(self.subscriptions),
        }
        if name in read_only:
            return read_only[name]
        if name == 'unfollow':
            self.follows.remove(cast('dict[str, object]', args[1]))
            return {}
        if name in {'find', 'find_one'}:
            return self._find(name, args)
        if name == 'create':
            row = {'type': args[0], 'id': self._next_id, **cast('dict[str, object]', args[1])}
            self._next_id += 1
            self.rows[(str(args[0]), cast('int', row['id']))] = row
            if kwargs.get('sudo'):
                target = row if args[0] == 'Note' else cast('dict[str, object]', row['entity'])
                self.follows.append({'type': target['type'], 'id': target['id']})
            if self.lose == args[0]:
                raise OSError('response lost')
            return row
        key = (str(args[0]), cast('int', args[1]))
        if name == 'update':
            self.rows[key].update(cast('dict[str, object]', args[2]))
            return self.rows[key].copy()
        if name == 'delete':
            del self.rows[key]
            return True
        raise AssertionError(name)


def case(kind: Literal['sync', 'async']) -> tuple[LiveCase, CleanupSite]:
    site = CleanupSite(kind)
    site.rows[('Project', 1)] = {'type': 'Project', 'id': 1, 'archived': False}
    target = LiveTarget(site.client, LiveBudget(session='L4', cleanup_requests=100))
    target.human = {'type': 'HumanUser', 'id': 2}
    target.baselines = {'F': [], 'W': [], 'S': [], 'P': site.rows[('Project', 1)].copy()}
    return LiveCase(kind, {'type': 'Project', 'id': 1}, 'owned', target=target), site
