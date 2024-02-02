# flake8: noqa: ANN001,ARG002
"""
Contains pulumi.dynamic.ResourceProvider implementations for Google Groups settings.
"""

from functools import cache

import google.auth
import googleapiclient.discovery
import pulumi
import pulumi.dynamic
from google.auth.transport.requests import Request


class GoogleGroupSettings(pulumi.dynamic.Resource):
    """A Pulumi dynamic resource for Google Groups settings."""

    group_email: pulumi.Output[str]
    # See https://developers.google.com/admin-sdk/groups-settings/v1/reference/groups
    # for the possible settings.
    settings: pulumi.Output[dict]

    def __init__(
        self,
        name: str,
        group_email: str,
        settings: dict,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(
            GoogleGroupSettingsProvider(),
            name,
            {
                'group_email': group_email,
                'settings': settings,
            },
            opts,
        )


class GoogleGroupSettingsProvider(pulumi.dynamic.ResourceProvider):
    """A Pulumi dynamic resource provider for Google Groups settings."""

    def create(self, props):
        """Creates a Google Groups settings resource."""
        group_email = props['group_email']
        settings = props['settings']
        updated_settings = update_group_settings(group_email, settings)
        # The response contains *all* settings, so subset to relevant keys.
        updated_settings = {k: updated_settings[k] for k in settings}
        outputs = {
            'group_email': group_email,
            'settings': updated_settings,
        }
        return pulumi.dynamic.CreateResult(
            id_=f'google_group_settings::{group_email}',
            outs=outputs,
        )

    def read(self, id_, props):
        """Reads a Google Groups settings resource."""
        group_email = props['group_email']
        settings = props['settings']
        current_settings = get_group_settings(group_email)
        # The response contains *all* settings, so subset to relevant keys.
        current_settings = {k: current_settings.get(k) for k in settings}
        outputs = {
            'group_email': group_email,
            'settings': current_settings,
        }
        return pulumi.dynamic.ReadResult(id_=id_, outs=outputs)

    def delete(self, unused_id, unused_inputs):
        """Deletes a Google Groups settings resource."""
        # Since we're only updating settings, there's no need to delete anything.

    def diff(self, unused_id, old_inputs, new_inputs):
        """Checks if the Google Groups settings resource needs to be updated."""
        replaces = [
            key
            for key in ('group_email', 'settings')
            if old_inputs[key] != new_inputs[key]
        ]

        if replaces:
            return pulumi.dynamic.DiffResult(changes=True, replaces=replaces)
        return pulumi.dynamic.DiffResult(changes=False)


@cache
def get_groups_credentials():
    """Returns credentials for the Google Groups Settings API."""
    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/apps.groups.settings'],
    )
    credentials.refresh(Request())
    return credentials


# The service client can't be cached, as it's not multi-thread safe.
def get_groups_settings_service():
    """Returns the Google Groups settings service."""
    return googleapiclient.discovery.build(
        'groupssettings',
        'v1',
        credentials=get_groups_credentials(),
    ).groups()


def update_group_settings(group_email, settings):
    """Updates Google Groups settings and returns the updated settings."""
    service = get_groups_settings_service()
    return service.update(groupUniqueId=group_email, body=settings).execute()


def get_group_settings(group_email):
    """Returns Google Groups settings."""
    service = get_groups_settings_service()
    return service.get(groupUniqueId=group_email).execute()
