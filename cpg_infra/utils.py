# flake8: noqa: ERA001
"""Utility methods that may be useful for multiple projects"""

import contextlib
import os

import pulumi

DEFAULT_ALLOWED_EXTENSIONS = frozenset({'.py', '.txt', '.json'})


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
        for filename in os.listdir('.'):
            if not any(filename.endswith(ext) for ext in allowed_extensions):
                # print(f'Skipping {filename} for invalid extension')
                continue

            if extra_assets and filename in extra_assets:
                # Skipping filename as it is in extra_assets
                continue

            with open(filename, encoding='utf-8') as file:
                # do it this way to stop any issues with changing paths
                assets[filename] = pulumi.StringAsset(file.read())

    if extra_assets:
        assets.update(extra_assets)

    return pulumi.AssetArchive(assets)
