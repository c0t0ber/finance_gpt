import tomllib


def _get_version():
    with open("pyproject.toml", "rb") as f:
        _META = tomllib.load(f)
    return _META["tool"]["poetry"]["version"]


__name__ = "lead_scaner"
__version__ = _get_version()
