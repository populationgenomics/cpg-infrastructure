#!/usr/bin/env python3

"""Main entrypoint for the storage visualization driver."""

import sys

# See requirements.txt for why we're disabling the linter warnings here.
import hailtop.batch as hb  # pylint: disable=import-error

from cpg_utils.config import get_config
from cpg_utils.git import (
    prepare_git_job,
    get_git_commit_ref_of_current_repository,
    get_organisation_name_from_current_directory,
    get_repo_name_from_current_directory,
)
from cpg_utils.hail_batch import (
    copy_common_env,
    remote_tmpdir,
    output_path,
    web_url,
)
from cpg_utils.slack import send_message


def prepare_job(job, clone_repo):
    """Sets up the given job to run scripts in the same repository."""
    job.image(get_config()['workflow']['driver_image'])
    copy_common_env(job)
    if clone_repo:
        prepare_git_job(
            job=job,
            organisation=get_organisation_name_from_current_directory(),
            repo_name=get_repo_name_from_current_directory(),
            commit=get_git_commit_ref_of_current_repository(),
        )


def main():
    """Main entrypoint."""
    if len(sys.argv) < 2:
        print('Usage: main.py <dataset1> <dataset2> ...')
        sys.exit(1)

    service_backend = hb.ServiceBackend(
        billing_project=get_config()['hail']['billing_project'],
        remote_tmpdir=remote_tmpdir(),
    )
    batch = hb.Batch(name='Storage visualization driver', backend=service_backend)

    # Process all datasets in parallel, as separate jobs.
    job_output_paths = {}
    for dataset in sys.argv[1:]:
        job = batch.new_job(name=f'process-{dataset}')
        prepare_job(job, clone_repo=True)

        # Reading all blob metadata is expensive and can take a long time, so don't risk
        # getting preempted.
        job._preemptible = False  # pylint: disable=protected-access
        job.cpu(0.5)

        path = output_path(f'{dataset}.json.gz', dataset='common', category='analysis')
        job.command(f'storage_visualization/disk_usage.py {dataset} {path}')

        job_output_paths[job] = path

    # Process the combined output of all jobs to generate a web report.
    treemap_job = batch.new_job(name='treemap')
    prepare_job(treemap_job, clone_repo=True)
    for job in job_output_paths:
        treemap_job.depends_on(job)

    web_path = output_path('treemap.html', dataset='common', category='web')
    treemap_job.command(
        f'storage_visualization/treemap.py --output {web_path} --group-by-dataset {" ".join(f"--input {path}" for path in job_output_paths.values())}'
    )

    # Send a Slack message when the HTML page has been generated.
    slack_job = batch.new_python_job(name='slack')
    prepare_job(slack_job, clone_repo=False)
    slack_job.depends_on(treemap_job)
    slack_job.call(
        send_message,
        'New storage visualization: ' + web_url('treemap.html', dataset='common'),
    )

    batch.run(wait=False)


if __name__ == '__main__':
    main()
