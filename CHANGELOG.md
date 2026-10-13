# Changelog

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
