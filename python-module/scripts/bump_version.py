import re
from pathlib import Path


PYPROJECT = Path("/Users/user/distributed-processing-project/python-module/pyproject.toml")


def bump_patch_version():
    content = PYPROJECT.read_text()

    match = re.search(
        r'(?m)^version\s*=\s*"(\d+)\.(\d+)\.(\d+)"',
        content,
    )

    if not match:
        raise RuntimeError("Could not find version in pyproject.toml")

    major, minor, patch = map(int, match.groups())

    old_version = f"{major}.{minor}.{patch}"
    new_version = f"{major}.{minor}.{patch + 1}"

    content = re.sub(
        r'(?m)^version\s*=\s*"\d+\.\d+\.\d+"',
        f'version = "{new_version}"',
        content,
        count=1,
    )

    PYPROJECT.write_text(content)

    print(f"Version: {old_version} -> {new_version}")
    return new_version


if __name__ == "__main__":
    bump_patch_version()