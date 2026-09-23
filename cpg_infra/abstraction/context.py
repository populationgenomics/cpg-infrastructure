from dataclasses import dataclass


@dataclass(frozen=True)
class InfraContext:
    """Context a CloudInfraBase instance needs about its owning entity."""

    name_prefix: str  # get_pulumi_name prefix, GCP project name fallback
    gcp_project_id: str | None = (
        None  # desired GCP project id (falls back to name_prefix)
    )
    gcp_region: str | None = None  # per-scope region override
