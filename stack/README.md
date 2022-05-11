# Deploying a stack for a dataset

The buckets and permission groups as described in the [storage policies](https://github.com/populationgenomics/team-docs/tree/main/storage_policies)
can be brought up using Pulumi.

To create a new Pulumi stack, run [`new_stack.py`](new_stack.py). Run `./new_stack.py --help` to see the list of supported options.

Make sure to also [update the access permissions](#access-permissions) afterwards.

## Dependencies

If a dataset requires access to other datasets, this can be specified through the optional `depends_on` stack config setting. For example, if you want the new `$DATASET` to be a _dependency_ of `seqr`, you would add the `$DATASET` to to the `depends_on` config for seqr.

**Note:** Before adding a dependency, make sure that it's okay to grant everybody who has access to `<dataset>` these implicit permissions to dependent datasets.

```shell
pulumi config set depends_on '["thousand-genomes", "hgdp"]'
```

This will grant read permissions to the `test` / `main` buckets of those dependencies, based on the access level of the service account. This can for example be useful for joint-calling multiple datasets.

## Access permissions

Add users to the `<dataset>-access@populationgenomics.org.au` Google Group to enable access through the analysis-runner. To be able to see Hail Batch logs for analysis-runner invocations, users also need to be added to the `<dataset>` Hail Batch billing project.

Add a `<dataset>` to `ALLOWED_REPOS` in [tokens](../tokens) and follow the instructions to update the analysis-runner server config.

## Updating all stacks

After any configuration change, you should apply the changes across all datasets, e.g. using [`update_all_stacks.py`](update_all_stacks.py) (which requires Python >= 3.9). However, make sure that any changes will also be reflected in the `main` branch, as when the state in the repository differs from what's deployed in production, debugging becomes extremely difficult.
