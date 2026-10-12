import os


def git_env() -> dict[str, str]:
    # Fixture repositories must not inherit the caller's repository or Git options.
    return {name: value for name, value in os.environ.items() if not name.startswith('GIT_')}
