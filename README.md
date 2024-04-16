# CPG Infrastructure

The CPG manages its cloud infrastructure through Pulumi. Specifically, we have developed an abstraction over the GCP and Azure clouds, that allow us with one `driver.py` to spin up infrastructure on both GCP and Azure. We manage all infrastructure for all datasets in one stack.

This repository contains all the driving code to build our pulumi stack, but none of the actual configuration. In your own environment, once you have a `CPGInfrastructureConfig` and each dataset's `CPGDatasetConfig`, you can instantiate a `CPGInfrastructure` object and call `main` within a pulumi context:

```python
# inside a pulumi context
config = CPGInfrastructureConfig.from_dict(...)
datasets = [CPGDatasetConfig.from_dict(...) for d in _datasets]
infra = CPGInfrastructure(config, datasets)
infra.main()
```

This creates pulumi resources, which pulumi could then create.

## Overview

There are 3 levels to our infrastructure which is represented by the 3 driver classes below:

- CPG wide infrastructure
- Dataset infrastructure (regardless of cloud)
- Cloud-specific dataset infrastructure

Noting that we often refer to a `common` dataset, which is where we place most CPG-wide infrastructure - and by default, all datasets have access to resources within this _common_ dataset.

### Configuration

The core of configuration is the `CPGInfrastructureConfig` and `CPGDatasetConfig`. These validate on construction, so you'll know if you have a valid config before running any more code.

The `CPGInfrastructureConfig` provides config information about the whole infrastructure. This still contains references to resources that were created manually - and is structured into sections. See the `CPGInfrastructureConfig` class for more information (it's fairly well documented).

The `CPGDatasetConfig` contains configuration information for each datasets to deploy. Note you MUST supply a config for the `common` dataset.

### Driver

Driver classes:

- `CPGInfrastructure`: CPG wide infrastructure
- `CPGDatasetInfrastructure`: Dataset infrastructure (regardless of cloud)
- `CPGDatasetCloudInfrastructure`: Cloud-specific dataset infrastructure

For the most part, each tangible resource (bucket, artifact registry, secret) is a cached property so that we can use it multiple times without creating multiple copies AND has the benefit that if the property isn't accessed, the resource isn't created. Another benefit is we don't have to fully initialise a dataset before other drivers can use it.

```python
@cached_property
def main_bucket(self):
    return self.infra.create_bucket(
        'main',
        lifecycle_rules=[self.infra.bucket_rule_undelete()],
        autoclass=self.dataset_config.autoclass,
    )
```

When the `CPGInfrastructure` creates each `CPGDatasetInfrastructure` which creates each `CPGDatasetCloudInfrastructure`, it passes a reference to itself, so that a dataset could access org-wide resources.

### Group memberships

We manage groups and memberships related to datasets in Pulumi. This allows us to have:

- transparency with who is in a group,
- a record of changes through GitHub,
- a clear, managed by GitHub approval chain
- fewer permissions for people who can action this
- ability to completely resolve nested groups at pulumi run time.

There are 4 places where group memberships are stored:

- In the Google / Azure groups themselves, we DON'T expand nested group members
- Cached in a blob in a _members cache_ location. This bucket is created in the `CPGInfrastructure` driver, and exported into an infra config, and a pulumi export.
- Provided to metamist as it uses its own [inbuilt groups](https://github.com/populationgenomics/metamist/pull/568) to manage permissions.
- Provided to Hail batch as it uses its own billing projects to manage access.

Note, in our implementation, we create _placeholder groups_ through the majority of the code, and at the end, we call `CPGInfrastructure.finalize_groups` to create outputs to the aforementioned 4 places.

### Abstraction

We want to _effectively_ mirror our infrastructure across GCP and Azure, to reduce code duplication we have a cloud abstraction, which provides an interface each cloud implements to achieve a desired functionality.

This abstraction, and our infra model was created with GCP in mind first, then Azure partially implemented later. There may be cloud concepts in the abstraction that don't exist, or aren't reasonable to ask of this interface.

For this reason, and the fact we're still primarily GCP, there are still places in each driver where we only create infrastructure on GCP, or have written cloud-specific implementations.

### Plugins

There is sometimes behaviour that we want to make optional, or not declare it in this reposistory. For that use case, we have `CpgInfrastructurePlugin`, which are exposed through [Python entrypoints](https://amir.rachum.com/python-entry-points/) using the key `"cpginfra.plugins"`.

Currently the [`BillingAggregator`](cpg_infra/billing_aggregator/), and [`MetamistInfra`](https://github.com/populationgenomics/metamist/tree/dev/metamist_infrastructure) are the two plugins used in our deploy.

### Internal datasets

This concept was designed to make it easier to have developers added to _internal_ Hail Batch projects and dataproc logs to facilitate debugging. 

To do this:

- a user config must have `can_access_internal_dataset_logs = True`, and
- the dataset config must have `is_internal_dataset = True`.

## Infrastructure

### Dataset infrastructure

> See [Reproducible analyses](https://github.com/populationgenomics/team-docs/blob/main/reproducible_analyses.md) and [Storage policies](https://github.com/populationgenomics/team-docs/tree/main/storage_policies) for more information.

Each dataset consists of a number of resources - some resources like buckets, and machine accounts are grouped into the different namespaces:

- main
- test (optional)

Members have different roles within a dataset, those roles include:

- data-manager
  - Write access to metadata
  - Read access to main-upload
  - Read access to main
  - \+ analysis
- analysis:
  - Trigger the analysis-runner
- List contents to main buckets
  - \+ metadata
- metadata:
  - Gives you read-access to metadata
  - \+ web
- web:
  - Access to view main-web bucket through web server
- upload:
  - Upload data into main-upload

## Setup

In our production environment, we have a:

- `cpg.toml`: CPG InfrastructureConfig
- `production.yaml`: A dictionary of `dataset: <config-options>`
- `$DATASET/members.yaml`: A dictionary of group name to members
- `$DATASET/budgets.yaml`: A dictionary of the `CPGDatasetConfig.budgets` option

And some code that takes this format, and transforms this into the required classes. We structure it this way, to allow for easier code-reviews and CODEOWNERS.

### CPG setup

You can't deploy (`up`) the stack locally, as you won't have permissions. But you will be able to `preview`.

```shell
# install pulumi
brew install pulumi

# use our GCS bucket for state
pulumi login gs://cpg-pulumi-state/

# inside the cpg-infrastructure directory
virtualenv cpg-infra
pip install -e .

# our pulumi stack is fairly large, so we'll run in a non-interactive view
PULUMI_EXPERIMENTAL=true PULUMI_SKIP_CHECKPOINTS=true pulumi preview \
  --non-interactive --diff -p 20

```

## Third party setup

## Context

Date: August, 2022

> The CPG’s current infrastructure has been in place for 2 years. With the addition of Azure as well as GCP, now is a good time to reconsider how we achieve certain infrastructure components for future proofing.

To manage infrastructure across GCP and Azure, as suggested by Greg Smith (Microsoft), we should write an abstraction on top of Pulumi for spinning up infrastructure in GCP and Azure without having to duplicate the “infrastructure design”.

Structure:

- It all belongs in `cpg_infra`
- The `config.py` defines a dataset configuration
- The `abstraction/` folder:
  - `base.py` declares an interface for a cloud abstraction
  - `gcp.py` / `azure.py` - implementations for specific clouds
- The `driver.py` turns this configuration in a pulumi state by calling methods on a `infra`.

To develop, you can run the driver file directly, which given a config TOML, will print infrastructure to the console.

### Motiviations

This abstraction is still trying to address a number of difficult problems:

- How do you manage the same (ish) infrastructure across two clouds?
- How do you elegantly handle different code pathways for different clouds?
- Can we modularise our deployment for each dataset a bit more?

Still to solve problems:

- How do you elegantly _import_ resources to use, for example notebooks / cromwell
