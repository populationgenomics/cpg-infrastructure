#!/usr/bin/env python3

import sys

import hailtop.batch as hb

from cpg_utils.git import (
    prepare_git_job,
    get_git_commit_ref_of_current_repository,
    get_organisation_name_from_current_directory,
    get_repo_name_from_current_directory,
)
from cpg_utils.hail_batch import (
    authenticate_cloud_credentials_in_job,
    copy_common_env,
    image_path,
    remote_tmpdir,
    output_path,
)
from cpg_utils.config import get_config


def main():
    if len(sys.argv) < 2:
        print('Usage: main.py <dataset1> <dataset2> ...')
        sys.exit(1)

    service_backend = hb.ServiceBackend(
        billing_project=get_config()['hail']['billing_project'],
        remote_tmpdir=remote_tmpdir(),
    )
    batch = hb.Batch(name='Storage visualization driver', backend=service_backend)

    # Process all datasets in parallel, as separate jobs.
    dataset_jobs = []
    for dataset in sys.argv[1:]:
        job = batch.new_job(name=f'Process {dataset}')

        copy_common_env(job)
        authenticate_cloud_credentials_in_job(job)

        # Make disk_usage.py available to child job.
        prepare_git_job(
            job=job,
            organisation=get_organisation_name_from_current_directory(),
            repo_name=get_repo_name_from_current_directory(),
            commit=get_git_commit_ref_of_current_repository(),
        )

        job.command(f'disk_usage.py {dataset}')

        dataset_jobs.append(job)


if __name__ == '__main__':
    main()
