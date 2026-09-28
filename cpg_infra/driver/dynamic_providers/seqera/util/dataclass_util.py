"""Dataclass helpers shared by the Seqera input models."""

from dataclasses import fields, is_dataclass
from typing import Any


def to_input_dict(instance: Any) -> dict[str, Any]:
    """Serialize a dataclass to a dict for Pulumi resource inputs.
    Nested dataclasses (and lists of them) are recursively serialized.
    Nones are dropped.
    """
    result: dict[str, Any] = {}
    for f in fields(instance):
        v = getattr(instance, f.name)
        if v is None:
            continue
        if is_dataclass(v):
            v = to_input_dict(v)
        elif isinstance(v, list) and v and is_dataclass(v[0]):
            v = [to_input_dict(item) for item in v]
        result[f.name] = v
    return result
