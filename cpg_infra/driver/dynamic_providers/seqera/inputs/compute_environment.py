from dataclasses import dataclass, fields, is_dataclass
from typing import Any, Literal, Optional

import pulumi
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

MAX_CE_NAME_LENGTH = 100
MAX_CRED_NAME_LENGTH = 100


def _to_input_dict(instance: Any) -> dict[str, Any]:
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
            v = _to_input_dict(v)
        elif isinstance(v, list) and v and is_dataclass(v[0]):
            v = [_to_input_dict(item) for item in v]
        result[f.name] = v
    return result


@dataclass
class ConfigEnvVariable:
    """Defines shape of the env-variables passed to `GoogleBatchConfig.environment` field"""

    name: pulumi.Input[str]
    value: pulumi.Input[str]
    compute: Optional[pulumi.Input[bool]] = None
    head: Optional[pulumi.Input[bool]] = None

    def to_input_dict(self) -> dict[str, Any]:
        return _to_input_dict(self)


@dataclass
class GoogleWifCredentialConfig:
    """WIF credential inputs for SeqeraComputeEnv.

    The compute env owns the underlying Seqera credentials live resource"""

    workload_identity_provider: pulumi.Input[str]
    service_account_email: pulumi.Input[str]
    token_audience: Optional[pulumi.Input[str]] = None

    def to_input_dict(self) -> dict[str, Any]:
        return _to_input_dict(self)


@dataclass
class GoogleBatchConfig:
    """Input for SeqeraComputeEnv resource"""

    location: pulumi.Input[str]
    work_dir: pulumi.Input[str]

    pre_run_script: Optional[pulumi.Input[str]] = None
    post_run_script: Optional[pulumi.Input[str]] = None

    service_account: Optional[pulumi.Input[str]] = None
    project_id: Optional[pulumi.Input[str]] = None

    network: Optional[pulumi.Input[str]] = None
    subnetwork: Optional[pulumi.Input[str]] = None
    network_tags: Optional[list[pulumi.Input[str]]] = None
    use_private_address: Optional[pulumi.Input[bool]] = None

    machine_type: Optional[pulumi.Input[str]] = None
    compute_jobs_machine_type: Optional[list[pulumi.Input[str]]] = None
    cpu_platform: Optional[pulumi.Input[str]] = None
    boot_disk_image: Optional[pulumi.Input[str]] = None
    boot_disk_size_gb: Optional[pulumi.Input[int]] = None
    debug_mode: Optional[pulumi.Input[int]] = None

    head_job_cpus: Optional[pulumi.Input[int]] = None
    head_job_memory_mb: Optional[pulumi.Input[int]] = None
    head_job_instance_template: Optional[pulumi.Input[str]] = None
    compute_jobs_instance_template: Optional[pulumi.Input[str]] = None

    spot: Optional[pulumi.Input[bool]] = None
    wave_enabled: Optional[pulumi.Input[bool]] = None
    fusion2_enabled: Optional[pulumi.Input[bool]] = None
    fusion_snapshots: Optional[pulumi.Input[bool]] = None

    nextflow_config: Optional[pulumi.Input[str]] = None
    environment: Optional[list[ConfigEnvVariable]] = None

    labels: Optional[dict[str, pulumi.Input[str]]] = None

    nfs_mount: Optional[pulumi.Input[str]] = None
    nfs_target: Optional[pulumi.Input[str]] = None
    ssh_daemon: Optional[pulumi.Input[bool]] = None
    ssh_image: Optional[pulumi.Input[str]] = None
    copy_image: Optional[pulumi.Input[str]] = None

    def to_input_dict(self) -> dict[str, Any]:
        return _to_input_dict(self)


class ConfigEnvVariableArgs(BaseModel):
    """Validate props of the `GoogleBatchConfig.environment`"""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    name: str
    value: str
    compute: Optional[bool] = None
    head: Optional[bool] = None


class GoogleBatchConfigArgs(BaseModel):
    """Validate props of the GoogleBatchConfig passed to compute environment."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    location: str
    work_dir: str

    pre_run_script: Optional[str] = None
    post_run_script: Optional[str] = None

    service_account: Optional[str] = None
    project_id: Optional[str] = None

    network: Optional[str] = None
    subnetwork: Optional[str] = None
    network_tags: Optional[list[str]] = None
    use_private_address: Optional[bool] = None

    machine_type: Optional[str] = None
    compute_jobs_machine_type: Optional[list[str]] = None
    cpu_platform: Optional[str] = None
    boot_disk_image: Optional[str] = None
    boot_disk_size_gb: Optional[int] = None
    debug_mode: Optional[int] = None

    head_job_cpus: Optional[int] = None
    head_job_memory_mb: Optional[int] = None
    head_job_instance_template: Optional[str] = None
    compute_jobs_instance_template: Optional[str] = None

    spot: Optional[bool] = None
    wave_enabled: Optional[bool] = None
    fusion2_enabled: Optional[bool] = None
    fusion_snapshots: Optional[bool] = None

    nextflow_config: Optional[str] = None
    environment: Optional[list[ConfigEnvVariableArgs]] = None

    labels: Optional[dict[str, str]] = None

    nfs_mount: Optional[str] = None
    nfs_target: Optional[str] = None
    ssh_daemon: Optional[bool] = None
    ssh_image: Optional[str] = None
    copy_image: Optional[str] = None


class GoogleWifCredentialArgs(BaseModel):
    """Validate props of the WIF credentials passed to ComputeEnvArgs."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    workload_identity_provider: str
    service_account_email: str
    token_audience: Optional[str] = None
    id: Optional[str] = None
    name: Optional[str] = Field(None, max_length=MAX_CRED_NAME_LENGTH)


# Extend when new platforms are added. Also they will require defining new `config` Input classes
ComputeEnvPlatform = Literal['google-batch']


class ComputeEnvArgs(BaseModel):
    """Validate props of the compute environment dynamic resource."""

    workspace_id: int
    name: str = Field(min_length=1, max_length=MAX_CE_NAME_LENGTH)
    credentials: GoogleWifCredentialArgs
    config: GoogleBatchConfigArgs
    description: Optional[str] = Field(None, max_length=2000)
    platform: ComputeEnvPlatform
    label_ids: Optional[list[int]] = None
    compute_env_id: Optional[str] = None
