# Changelog

## 0.2.0 (2026-11-09)

### Breaking changes

- `shotgun_api3.__version__` reports `3.10.5`, the version of Autodesk's client that this package
  matches. Read the sg-python-api version with `importlib.metadata.version('sg-python-api')`.
- `shotgun_api3.lib.mockgun` answers as a server does, so tests that relied on its old shortcuts
  can fail. When it cannot tell what a server would answer, it raises `MockgunError`, which is not
  a `ShotgunError`. The migration guide's "Migrate tests that use mockgun" section lists each
  difference; the most common follow.
- Give a mockgun client one complete credential form, as the real client requires, or only a base
  URL. Partial or combined forms now raise.
- mockgun's `update()` returns a dictionary instead of a list. Remove the `[0]` lookup.
- mockgun's `_db`, `_schema` and `_schema_entity` are gone. Seed and inspect data through client
  calls or `client.site`, and change schemas with the schema write methods.
- mockgun links carry the target's display `name`. Give linked records a display field, such as
  `code`, and compare links by `type` and `id`.
- mockgun returns datetimes timezone-aware in local time, without microseconds. Compare them as
  instants.
- mockgun rejects raw `logical_operator` and `conditions` filter trees. Use `filter_operator` and
  `filters` groups.

### Added

- `sg.config.max_connection_idle_secs` sets how many seconds a connection can sit idle before the
  client replaces it. The default is `60`; `None` or `0` turns this off.
- `sg.config.tcp_keepalive_timeout_secs` sets roughly how many seconds a call waits on a dead
  connection before failing. The default is `60`; `None` or `0` turns this off.
- `mockgun.Shotgun` and `mockgun.AsyncShotgun` subclass the real clients, so
  `isinstance(client, shotgun_api3.Shotgun)` holds for a mockgun client.
- mockgun emulates `summarize()`, `text_search()`, relative-date filters, schema writes, file
  uploads and downloads, thumbnails, Note threads, activity streams, follows, preferences, work
  schedules, subscriptions and Project last access.
- A mockgun client's `site` property controls the in-memory site, such as its clock, server
  metadata and preferences, and `new_client()` returns another client on the same site.
- `shotgun_api3.lib.mockgun.mockgun` provides the same names as `shotgun_api3.lib.mockgun`, for
  code written for the module layout of Autodesk's mockgun.

### Fixed

- `find()` and `find_one()` raise `ShotgunError` when `fields` is a string, instead of returning
  none of the requested fields.

## 0.1.2 (2026-10-13)

### Fixed

- `note_thread_read()`, `nav_search_string()` and `nav_search_entity()` no longer fail with
  `Fault`.
- `user_subscriptions_read()` no longer raises `ShotgunError`, and returns the documented
  `humanUserId` and `subscription` dictionaries.
- `user_subscriptions_create()` now assigns subscriptions. Give each entry a `humanUserId`, as a
  string or an integer, instead of `user_id`.

## 0.1.1 (2026-10-12)

### Fixed

- `entity_types()` and `schema()` now raise `ShotgunError`, as in Autodesk's client. Use
  `schema_entity_read()` and `schema_field_read()` instead. The `deprecated-entity-types-alias` and
  `deprecated-schema-alias` compatibility warning codes are no longer emitted.
- `create()` and `update()` upload file paths given for `image` and `filmstrip_image` as
  thumbnails, as Autodesk's client does.
- `update()` with no fields to change returns the entity's `id` and `type` instead of raising
  `Fault`.

## 0.1.0 (2026-10-11)

Initial release.
