import importlib.metadata
__version__ = importlib.metadata.version("dataclients")


def metadata():
    return dict(importlib.metadata.metadata("dataclients"))
