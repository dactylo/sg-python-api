import hashlib
import zipfile
from collections.abc import Mapping
from pathlib import Path


def write_wheel(
    path: Path,
    files: Mapping[str, str | bytes],
    *,
    fixed_timestamp: bool = False,
) -> str:
    with zipfile.ZipFile(path, mode='w') as archive:
        for name, contents in files.items():
            archive.writestr(zipfile.ZipInfo(name) if fixed_timestamp else name, contents)
    return hashlib.sha256(path.read_bytes()).hexdigest()
