"""Compatibility target reported by `__version__` and the package release version."""

from importlib.metadata import distributions

COMPATIBILITY_VERSION = '3.10.5'
PACKAGE_VERSION = next(
    (distribution.version for distribution in distributions(name='sg-python-api')), '0+unknown'
)
