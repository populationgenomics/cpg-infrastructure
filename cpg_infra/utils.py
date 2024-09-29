# flake8: noqa: ERA001
"""Utility methods that may be useful for multiple projects"""

import contextlib
import os
from typing import Iterable

import pulumi
import toml
import xxhash
from toml_sort import TomlSort

DEFAULT_ALLOWED_EXTENSIONS = frozenset({".py", ".txt", ".json"})

AccessLevel = str


def access_levels(*, include_test: bool) -> Iterable[AccessLevel]:
    if include_test:
        return ("test", "standard", "full")
    return ("standard", "full")


def archive_folder(
    path: str,
    allowed_extensions: frozenset[str] = DEFAULT_ALLOWED_EXTENSIONS,
    extra_assets: dict[str, pulumi.Asset] | None = None,
) -> pulumi.AssetArchive:
    """Archive a folder into a pulumi asset archive

    extra_assets: map of file name and extra assets to add to the archive

    e.g:
    {
        "requirements.txt": StringAsset / FileAsset(...)
    }

    """
    assets = {}

    # python 3.11 thing, but allows you to temporarily change directory
    # into the path we're archiving, so we're not archiving the directory,
    # but just the code files. Otherwise the deploy fails.
    with contextlib.chdir(path):
        for filename in os.listdir("."):
            if not any(filename.endswith(ext) for ext in allowed_extensions):
                # print(f'Skipping {filename} for invalid extension')
                continue

            if extra_assets and filename in extra_assets:
                # Skipping filename as it is in extra_assets
                continue

            with open(filename, encoding="utf-8") as file:
                # do it this way to stop any issues with changing paths
                assets[filename] = pulumi.StringAsset(file.read())

    if extra_assets:
        assets.update(extra_assets)

    return pulumi.AssetArchive(assets)


def compute_hash(dataset: str, member: str, cloud: str) -> str:
    """
    >>> compute_hash('dataset', 'hello.world@email.com', '')
    'HW-d51b65ee'
    """
    initials = "".join(n[0] for n in member.split("@")[0].split(".")).upper()
    # I was going to say "add a salt", but we're displaying the initials,
    # so let's call it something like salt, monosodium glutamate ;)
    msg = dataset + member + cloud
    computed_hash = xxhash.xxh32(msg.encode()).hexdigest()
    return initials + "-" + computed_hash


def dict_to_toml(d: dict) -> str:
    """
    Convert dictionary to a sorted (and stable) TOML
    """
    # there's not an easy way to convert dictionary to the
    # internal tomlkit.TOMLDocument, as it has its own parser,
    # so let's just easy dump to string, to use the library from there.
    return TomlSort(toml.dumps(d)).sorted()
