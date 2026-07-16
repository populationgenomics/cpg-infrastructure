"""
Base model for CPG infra config

Contains some sensible defaults:

frozen=True ensures that config isn't mutated after creation
extra='forbid' ensures that accidentally misnamed config keys aren't accepted

"""

from typing import Any, Self

from pydantic import BaseModel, ConfigDict


class ConfigModel(BaseModel):
    """Base for all CPG infra config models: validates on construction, immutable."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    @classmethod
    def from_dict(cls: type[Self], d: dict[str, Any]) -> Self:
        """Allow for places that still use from_dict, just alias to model_validate"""
        return cls.model_validate(d)
