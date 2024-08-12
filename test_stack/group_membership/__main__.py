from cpg_infra.abstraction.google_group_membership import (
    GoogleGroupMembership,
    GoogleGroupMembershipInputs,
)


def from_pulumi():
    GoogleGroupMembership(
        'test-group-membership',
        props=GoogleGroupMembershipInputs(
            group_key='abc',
            member_key='foo@bar.com',
        ),
    )


if __name__ == '__main__':
    from_pulumi()
