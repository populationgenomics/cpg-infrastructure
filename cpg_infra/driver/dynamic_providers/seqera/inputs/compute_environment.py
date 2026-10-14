from dataclasses import dataclass
from typing import Literal

import pulumi
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from cpg_infra.driver.dynamic_providers.seqera.inputs.credentials import (
    GoogleWifCredentialArgs,
)

MAX_CE_NAME_LENGTH = 100


@dataclass
class ConfigEnvVariable:
    """Defines shape of the env-variables passed to `GoogleBatchConfig.environment` field"""

    name: pulumi.Input[str]
    value: pulumi.Input[str]
    compute: pulumi.Input[bool] | None = None
    head: pulumi.Input[bool] | None = None


@dataclass
class GoogleBatchConfig:
    """Input for SeqeraComputeEnv resource"""

    location: pulumi.Input[str]
    work_dir: pulumi.Input[str]

    pre_run_script: pulumi.Input[str] | None = None
    post_run_script: pulumi.Input[str] | None = None

    service_account: pulumi.Input[str] | None = None
    project_id: pulumi.Input[str] | None = None

    network: pulumi.Input[str] | None = None
    subnetwork: pulumi.Input[str] | None = None
    network_tags: list[pulumi.Input[str]] | None = None
    use_private_address: pulumi.Input[bool] | None = None

    machine_type: pulumi.Input[str] | None = None
    compute_jobs_machine_type: list[pulumi.Input[str]] | None = None
    cpu_platform: pulumi.Input[str] | None = None
    boot_disk_image: pulumi.Input[str] | None = None
    boot_disk_size_gb: pulumi.Input[int] | None = None
    debug_mode: pulumi.Input[int] | None = None

    head_job_cpus: pulumi.Input[int] | None = None
    head_job_memory_mb: pulumi.Input[int] | None = None
    head_job_instance_template: pulumi.Input[str] | None = None
    compute_jobs_instance_template: pulumi.Input[str] | None = None

    spot: pulumi.Input[bool] | None = None
    wave_enabled: pulumi.Input[bool] | None = None
    fusion2_enabled: pulumi.Input[bool] | None = None
    fusion_snapshots: pulumi.Input[bool] | None = None

    nextflow_config: pulumi.Input[str] | None = None
    environment: list[ConfigEnvVariable] | None = None

    labels: dict[str, pulumi.Input[str]] | None = None

    nfs_mount: pulumi.Input[str] | None = None
    nfs_target: pulumi.Input[str] | None = None
    ssh_daemon: pulumi.Input[bool] | None = None
    ssh_image: pulumi.Input[str] | None = None
    copy_image: pulumi.Input[str] | None = None


class ConfigEnvVariableArgs(BaseModel):
    """Validate props of the `GoogleBatchConfig.environment`"""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    name: str
    value: str
    compute: bool | None = None
    head: bool | None = None


class GoogleBatchConfigArgs(BaseModel):
    """Validate props of the GoogleBatchConfig passed to compute environment."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    location: str
    work_dir: str

    pre_run_script: str | None = None
    post_run_script: str | None = None

    service_account: str | None = None
    project_id: str | None = None

    network: str | None = None
    subnetwork: str | None = None
    network_tags: list[str] | None = None
    use_private_address: bool | None = None

    machine_type: str | None = None
    compute_jobs_machine_type: list[str] | None = None
    cpu_platform: str | None = None
    boot_disk_image: str | None = None
    boot_disk_size_gb: int | None = None
    debug_mode: int | None = None

    head_job_cpus: int | None = None
    head_job_memory_mb: int | None = None
    head_job_instance_template: str | None = None
    compute_jobs_instance_template: str | None = None

    spot: bool | None = None
    wave_enabled: bool | None = None
    fusion2_enabled: bool | None = None
    fusion_snapshots: bool | None = None

    nextflow_config: str | None = None
    environment: list[ConfigEnvVariableArgs] | None = None

    labels: dict[str, str] | None = None

    nfs_mount: str | None = None
    nfs_target: str | None = None
    ssh_daemon: bool | None = None
    ssh_image: str | None = None
    copy_image: str | None = None


# Extend when new platforms are added. Also they will require defining new `config` Input classes
ComputeEnvPlatform = Literal['google-batch']


class ComputeEnvArgs(BaseModel):
    """Validate props of the compute environment dynamic resource."""

    workspace_id: int
    name: str = Field(min_length=1, max_length=MAX_CE_NAME_LENGTH)
    credentials: GoogleWifCredentialArgs
    config: GoogleBatchConfigArgs
    description: str | None = Field(None, max_length=2000)
    platform: ComputeEnvPlatform
    label_ids: list[int] | None = None
    compute_env_id: str | None = None
