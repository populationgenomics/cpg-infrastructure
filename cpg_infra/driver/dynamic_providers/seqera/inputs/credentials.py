"""Input models for Seqera credential resources."""

from dataclasses import dataclass
from typing import Optional

import pulumi
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from cpg_infra.driver.dynamic_providers.seqera.util.credentials_util import (
    MAX_CRED_NAME_LENGTH,
)


@dataclass
class GoogleWifCredentialConfig:
    """WIF credential inputs for SeqeraComputeEnv.

    The compute env owns the underlying Seqera credentials live resource.
    """

    workload_identity_provider: pulumi.Input[str]
    service_account_email: pulumi.Input[str]
    token_audience: Optional[pulumi.Input[str]] = None


class GoogleWifCredentialArgs(BaseModel):
    """Validate props of the WIF credentials passed to ComputeEnvArgs."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    workload_identity_provider: str
    service_account_email: str
    token_audience: Optional[str] = None
    id: Optional[str] = None
    name: Optional[str] = Field(None, max_length=MAX_CRED_NAME_LENGTH)


class GithubCredentialArgs(BaseModel):
    """Validate props for the GitHub credential dynamic resource."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    workspace_id: int
    name: str = Field(min_length=1, max_length=MAX_CRED_NAME_LENGTH)
    username: str
    access_token_secret_name: str
    base_url: str
    resolved_secret_version: Optional[str] = None
    credentials_id: Optional[str] = None
