# flake8: noqa: ANN001,ARG002
"""
Contains pulumi.dynamic.ResourceProvider implementations for Google Groups settings.
"""

from functools import cache
from typing import Literal, TypedDict

import google.auth
import googleapiclient.discovery
import pulumi
import pulumi.dynamic
from google.auth.transport.requests import Request

# API enums use 'true'/'false' strings, not Python bools.
_BoolStr = Literal['true', 'false']


class GoogleGroupSettingsDict(TypedDict, total=False):
    """Documented (partial, non-exhaustive) Google Groups Settings API keys.

    The keys cpg_infra sets or is likely to. `total=False`: every key is
    optional. The `Literal` value sets are the API's enums as of writing — see
    the authoritative reference for the full/updated key and value lists:
    https://developers.google.com/admin-sdk/groups-settings/v1/reference/groups
    """

    allowExternalMembers: _BoolStr
    whoCanPostMessage: Literal[
        'NONE_CAN_POST',
        'ALL_MANAGERS_CAN_POST',
        'ALL_OWNERS_CAN_POST',
        'ALL_MEMBERS_CAN_POST',
        'ALL_IN_DOMAIN_CAN_POST',
        'ANYONE_CAN_POST',  # world-postable
    ]
    whoCanJoin: Literal[
        'ANYONE_CAN_JOIN',
        'ALL_IN_DOMAIN_CAN_JOIN',
        'INVITED_CAN_JOIN',
        'CAN_REQUEST_TO_JOIN',
    ]
    whoCanViewGroup: Literal[
        'ANYONE_CAN_VIEW',
        'ALL_IN_DOMAIN_CAN_VIEW',
        'ALL_MEMBERS_CAN_VIEW',
        'ALL_MANAGERS_CAN_VIEW',
        'ALL_OWNERS_CAN_VIEW',
    ]
    whoCanViewMembership: Literal[
        'ALL_IN_DOMAIN_CAN_VIEW',
        'ALL_MEMBERS_CAN_VIEW',
        'ALL_MANAGERS_CAN_VIEW',
        'ALL_OWNERS_CAN_VIEW',
    ]
    messageModerationLevel: Literal[
        'MODERATE_ALL_MESSAGES',
        'MODERATE_NON_MEMBERS',
        'MODERATE_NEW_MEMBERS',
        'MODERATE_NONE',
    ]
    spamModerationLevel: Literal['ALLOW', 'MODERATE', 'SILENTLY_MODERATE', 'REJECT']
    replyTo: Literal[
        'REPLY_TO_CUSTOM',
        'REPLY_TO_SENDER',
        'REPLY_TO_LIST',
        'REPLY_TO_OWNER',
        'REPLY_TO_IGNORE',
        'REPLY_TO_MANAGERS',
    ]
    archiveOnly: _BoolStr
    membersCanPostAsTheGroup: _BoolStr


class GoogleGroupSettings(pulumi.dynamic.Resource):
    """A Pulumi dynamic resource for Google Groups settings."""

    group_email: pulumi.Output[str]
    # Allowed keys are documented (partially) by GoogleGroupSettingsDict above;
    # full reference:
    # https://developers.google.com/admin-sdk/groups-settings/v1/reference/groups
    settings: pulumi.Output[GoogleGroupSettingsDict]

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
