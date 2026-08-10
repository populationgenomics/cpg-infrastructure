# flake8: noqa: ANN401
"""
SampleMetadataAccessorMembership named tuple.
"""

from typing import Any, Iterable, NamedTuple


class SampleMetadataAccessorMembership(NamedTuple):
    name: str
    member: Any
    permissions: Iterable[str]
