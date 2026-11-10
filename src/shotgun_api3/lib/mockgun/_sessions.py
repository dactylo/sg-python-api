"""Resolve fake-site credentials and preserve their actors across token exchanges."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast

from ._errors import MockgunError, ServerFaultError
from ._schema import SiteSchema
from ._store import Store


@dataclass(frozen=True)
class _Actor:
    """An authenticated identity scoped to one fake site."""

    entity_type: Literal['ApiUser', 'HumanUser']
    entity_id: int
    key: str


@dataclass(frozen=True)
class RequestContext:
    """The actor and timestamp shared by one server request.

    Attributes:
        actor: Resolved identity, or `None` during bootstrap or anonymous
            calls.
        now: Time captured once when the request entered the server.
        session_uuid: Optional caller-supplied event session identifier.
        sudo: Whether script credentials selected a human actor.
        actual: Script identity behind sudo, otherwise `None`.
        session_kind: Sudo, script, or human classification; anonymous calls
            use the human classification.
    """

    actor: _Actor | None
    now: datetime
    session_uuid: str | None = None
    sudo: bool = False
    actual: _Actor | None = None

    @property
    def session_kind(self) -> Literal['script', 'sudo', 'human']:
        """Classify sudo first, then script actors, with anonymous calls treated as human."""
        if self.sudo:
            return 'sudo'
        if self.actor is not None and self.actor.entity_type == 'ApiUser':
            return 'script'
        return 'human'


class Sessions:
    """Site-owned credentials and tokens resolved under the server's request lock."""

    def __init__(self, store: Store, schema: SiteSchema) -> None:
        """Bind identity resolution to a site's records and schema declarations."""
        self._store = store
        self._schema = schema
        self._admitted_logins: set[str] = set()
        self._created_humans: dict[str, _Actor] = {}
        self._created_scripts: dict[str, _Actor] = {}
        self._internal: dict[Literal['ApiUser', 'HumanUser'], dict[str, _Actor]] = {
            'ApiUser': {},
            'HumanUser': {},
        }
        self._tokens: dict[str, _Actor] = {}
        self._token_counter = 0
        self._unknown_token_counter = 0

    def _existing_human(self, login: str) -> _Actor | None:
        """Match an existing HumanUser by login, including retired records."""
        if not self._schema.declares('HumanUser'):
            return self._internal['HumanUser'].get(login)
        if not self._schema.declares('HumanUser', 'login'):
            return self._created_humans.get(login)
        for retired in (False, True):
            for row in self._store.rows('HumanUser', retired=retired):
                if row.get('login') == login:
                    return _Actor('HumanUser', cast('int', row['id']), login)
        return None

    def _internal_actor(self, entity_type: Literal['ApiUser', 'HumanUser'], key: str) -> _Actor:
        """Return or allocate an identity without adding a schema record."""
        identities = self._internal[entity_type]
        if key not in identities:
            identities[key] = _Actor(entity_type, len(identities) + 1, key)
        return identities[key]

    def _human(self, login: str, *, now: datetime) -> _Actor:
        """Find or create a HumanUser using only declared optional fields."""
        existing = self._existing_human(login)
        if existing is not None:
            return existing
        if not self._schema.declares('HumanUser'):
            return self._internal_actor('HumanUser', login)
        fields: dict[str, object] = {}
        fields.update(
            {
                name: value
                for name, value in {
                    'login': login,
                    'name': login,
                    'sg_status_list': 'act',
                }.items()
                if self._schema.declares('HumanUser', name)
            }
        )
        row = self._store.create('HumanUser', fields, context=RequestContext(None, now))
        actor = _Actor('HumanUser', cast('int', row['id']), login)
        self._store.record_actor(actor.entity_type, actor.entity_id, actor.key)
        if not self._schema.declares('HumanUser', 'login'):
            self._created_humans[login] = actor
        return actor

    def _from_token(self, token: str, *, now: datetime) -> _Actor:
        """Reuse a known token actor or create its stable placeholder human."""
        if token not in self._tokens:
            self._unknown_token_counter += 1
            self._tokens[token] = self._human(
                f'mockgun-session-user-{self._unknown_token_counter}', now=now
            )
        return self._tokens[token]

    def _script(self, script: str, *, now: datetime) -> _Actor:
        """Find or create an ApiUser for a script when the schema declares it."""
        if not self._schema.declares('ApiUser'):
            return self._internal_actor('ApiUser', script)
        if self._schema.declares('ApiUser', 'firstname'):
            for retired in (False, True):
                for row in self._store.rows('ApiUser', retired=retired):
                    if row.get('firstname') == script:
                        return _Actor('ApiUser', cast('int', row['id']), script)
        else:
            existing = self._created_scripts.get(script)
            if existing is not None:
                return existing
        fields: dict[str, object] = {}
        if self._schema.declares('ApiUser', 'firstname'):
            fields['firstname'] = script
        row = self._store.create('ApiUser', fields, context=RequestContext(None, now))
        actor = _Actor('ApiUser', cast('int', row['id']), script)
        self._store.record_actor(actor.entity_type, actor.entity_id, actor.key)
        if not self._schema.declares('ApiUser', 'firstname'):
            self._created_scripts[script] = actor
        return actor

    def admit_login(self, login: str) -> None:
        """Allow constructor credentials to create this login on first use."""
        self._admitted_logins.add(login)

    def issue_token(self, actor: _Actor) -> str:
        """Issue a site-local token that retains the supplied actor's identity."""
        while True:
            self._token_counter += 1
            token = f'mockgun-session-{self._token_counter}'
            if token not in self._tokens:
                self._tokens[token] = actor
                return token

    def resolve(self, auth: Mapping[str, object], *, now: datetime) -> _Actor:
        """Resolve complete credentials, accepting arbitrary password and key values.

        Raises:
            ServerFaultError: If credentials are incomplete, the login is
                unknown, or sudo accompanies a session token.
            MockgunError: If sudo accompanies human credentials.
        """
        script = auth.get('script_name')
        sudo = auth.get('sudo_as_login')
        if isinstance(script, str) and isinstance(auth.get('script_key'), str):
            if isinstance(sudo, str):
                return self._human(sudo, now=now)
            return self._script(script, now=now)
        if sudo is not None and isinstance(auth.get('session_token'), str):
            raise ServerFaultError(
                'The sudo_as_login option can only be used with script authentication.',
                error_code=102,
            )
        if sudo is not None:
            raise MockgunError('mockgun does not emulate sudo with human credentials yet')
        login = auth.get('user_login')
        if isinstance(login, str) and isinstance(auth.get('user_password'), str):
            if login in self._admitted_logins or self._existing_human(login) is not None:
                return self._human(login, now=now)
            raise ServerFaultError('Authentication failed.', error_code=102)
        token = auth.get('session_token')
        if isinstance(token, str):
            return self._from_token(token, now=now)
        raise ServerFaultError('Authentication failed.', error_code=102)

    def script_actor(self, auth: Mapping[str, object], *, now: datetime) -> _Actor | None:
        """Resolve the script behind complete script credentials, if present.

        Args:
            auth: Request credentials in their wire representation.
            now: Request instant used when creating the script's ApiUser.

        Returns:
            The existing or newly created script actor, or `None` for other
            credential forms.
        """
        script = auth.get('script_name')
        if isinstance(script, str) and isinstance(auth.get('script_key'), str):
            return self._script(script, now=now)
        return None
