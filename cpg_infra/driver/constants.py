# flake8: noqa: PGH003,ANN204,C901,ERA001,ANN401,SIM102
"""
Constants and helper functions used across the driver package.
"""

import re
from typing import Iterable, Type

import toml
import xxhash
from toml_sort import TomlSort

# NOTE: Importing these registers them as CloudInfraBase subclasses so that
# NAME_TO_INFRA_CLASS below picks them up.
from cpg_infra.abstraction.azure import AzureInfra  # noqa: F401
from cpg_infra.abstraction.base import CloudInfraBase, DryRunInfra  # noqa: F401
from cpg_infra.abstraction.gcp import GcpInfrastructure  # noqa: F401


AccessLevel = str


SM_TEST_READ = 'test-read'
SM_TEST_WRITE = 'test-write'
SM_TEST_CONTRIBUTE = 'test-contribute'
SM_MAIN_READ = 'main-read'
SM_MAIN_WRITE = 'main-write'
SM_MAIN_CONTRIBUTE = 'main-contribute'
METAMIST_PERMISSIONS = [
    SM_TEST_READ,
    SM_TEST_WRITE,
    SM_TEST_CONTRIBUTE,
    SM_MAIN_READ,
    SM_MAIN_WRITE,
    SM_MAIN_CONTRIBUTE,
]


NON_NAME_REGEX = re.compile(r'[^A-Za-z\d_-]')
TOML_CONFIG_JOINER = '\n||||'


NAME_TO_INFRA_CLASS: dict[str, Type[CloudInfraBase]] = {
    c.name(): c  # type: ignore
    for c in CloudInfraBase.__subclasses__()
}


def access_levels(*, include_test: bool) -> Iterable[AccessLevel]:
    if include_test:
        return ('test', 'standard', 'full')
    return ('standard', 'full')


def dict_to_toml(d: dict) -> str:
    """
    Convert dictionary to a sorted (and stable) TOML
    """
    # there's not an easy way to convert dictionary to the
    # internal tomlkit.TOMLDocument, as it has its own parser,
    # so let's just easy dump to string, to use the library from there.
    return TomlSort(toml.dumps(d)).sorted()


def compute_hash(dataset: str, member: str, cloud: str) -> str:
    """
    >>> compute_hash('dataset', 'hello.world@email.com', '')
    'HW-d51b65ee'
    """
    initials = ''.join(n[0] for n in member.split('@')[0].split('.')).upper()
    # I was going to say "add a salt", but we're displaying the initials,
    # so let's call it something like salt, monosodium glutamate ;)
    msg = dataset + member + cloud
    computed_hash = xxhash.xxh32(msg.encode()).hexdigest()
    return initials + '-' + computed_hash
