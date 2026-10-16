from cpg_infra.driver.groups import Group, GroupMember, GroupProvider


def _group(name: str, members: dict) -> Group:
    return Group(name=name, group=None, members=members, cache_members=False)


def test_resolve_group_members_keeps_config_order_and_dedupes():
    alice, bob, carol = (GroupMember(f'{n}@example.com', None) for n in 'abc')
    inner = _group('inner', {'bob': bob, 'alice': alice})
    outer = _group('outer', {'carol': carol, 'inner': inner, 'alice': alice})

    provider = GroupProvider()
    first = provider.resolve_group_members(outer)
    cached = provider.resolve_group_members(outer)

    assert first == [carol, bob, alice]
    assert cached == first
