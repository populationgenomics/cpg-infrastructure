# flake8: noqa: ANN401
"""
MainUploadBucket named tuple.
"""

from typing import Any, NamedTuple


class MainUploadBucket(NamedTuple):
    bucket: Any
    uploaders: list[str]
    is_dropbox: bool = False
