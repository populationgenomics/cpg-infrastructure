from typing import Optional

from pydantic import BaseModel


class GoogleCredentialsArgs(BaseModel):
    """Validate props for the Google credential dynamic resource."""

    workspace_id: int
    name: str
    credentials_id: Optional[str] = None

    workload_identity_provider: str
    service_account_email: str
    token_audience: Optional[str] = None