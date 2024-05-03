#!/usr/bin/env python3
# flake8: noqa: PLR2004,SLF001
"""Main entrypoint for the storage visualization driver."""

import sys

from cloudpathlib import AnyPath

# See requirements.txt for why we're disabling the linter warnings here.
import hailtop.batch as hb  # pylint: disable=import-error

from cpg_utils.config import output_path
from cpg_utils.git import (
    get_git_commit_ref_of_current_repository,
    get_organisation_name_from_current_directory,
    get_repo_name_from_current_directory,
)
from cpg_utils.hail_batch import (
    copy_common_env,
    get_batch,
    prepare_git_job,
)
from cpg_utils.slack import upload_file

DOCKER_IMAGE = (
    'australia-southeast1-docker.pkg.dev/cpg-common/images/storage-visualization:latest'
)


def prepare_job(job: hb.batch.job.Job, clone_repo: bool):
    """Sets up the given job to run scripts in the same repository."""
    job.image(DOCKER_IMAGE)
    copy_common_env(job)
    if clone_repo:
        prepare_git_job(
            job=job,
            organisation=get_organisation_name_from_current_directory(),
            repo_name=get_repo_name_from_current_directory(),
            commit=get_git_commit_ref_of_current_repository(),
        )


def post_to_slack():
    """Posts the URL of the generated treemap together with a preview image to Slack."""
    with AnyPath(output_path('treemap.png', dataset='common', category='web')).open(
        'rb',
    ) as f:
        content = f.read()

    upload_file(
        content=content,
        comment='Storage visualization: '
        + output_path('treemap.html', dataset='common', category='web_url'),
    )


def main():
    """Main entrypoint."""
    if len(sys.argv) < 2:
        print('Usage: main.py <dataset1> <dataset2> ...')
        sys.exit(1)

    batch = get_batch(name='Storage visualization driver')

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

    web_path = output_path('treemap', dataset='common', category='web')
    treemap_job.command(
        f'storage_visualization/treemap.py --output-prefix {web_path} --group-by-dataset {" ".join(f"--input {path}" for path in job_output_paths.values())}',
    )

    # Send a Slack message when the HTML page has been generated.
    slack_job = batch.new_python_job(name='slack')
    prepare_job(slack_job, clone_repo=False)
    slack_job.depends_on(treemap_job)
    slack_job.call(post_to_slack)

    batch.run(wait=False)


if __name__ == '__main__':
    main()
