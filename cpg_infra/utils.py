"""Utility methods that may be useful for multiple projects"""
import contextlib
import os
from typing import Any, Callable

import pulumi

DEFAULT_ALLOWED_EXTENSIONS = frozenset({'.py', '.txt', '.json'})


def archive_folder(
    path: str,
    allowed_extensions: frozenset[str] = DEFAULT_ALLOWED_EXTENSIONS,
    apply_fun_map: dict[str, tuple[Callable, Any]] | None = None,
) -> pulumi.AssetArchive:
    """Archive a folder into a pulumi asset archive

    apply_fun_map contains map of file name to callable function and its extra parameter

    e.g. apply_fun_map = {
        'metamist-6.2.0.tar.gz': (add_binary_file, None),
        'requirements.txt': (update_requirements, {
            'private_repo_url': 'https://...',
            'private_repos': ['repo1','repo2'],
        }),
    }

    where e.g. add_binary_file is defined as:
    def add_binary_file(**kwargs):
        return pulumi.FileAsset(f"{kwargs.get('path')}/{kwargs.get('filename')}")

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

            # apply function to update file content
            if apply_fun_map and filename in apply_fun_map:
                (fun_name, extra_fun_params) = apply_fun_map[filename]
                fun_params = {'filename': filename, 'path': path}
                if extra_fun_params:
                    # if extra params provided, then update fun_params
                    fun_params.update(extra_fun_params)

                # call the function to prepare the file asset
                file_as_asset = fun_name(**fun_params)
                assets[filename] = file_as_asset
                continue

            with open(filename, encoding='utf-8') as file:
                # do it this way to stop any issues with changing paths
                assets[filename] = pulumi.StringAsset(file.read())
        return pulumi.AssetArchive(assets)
