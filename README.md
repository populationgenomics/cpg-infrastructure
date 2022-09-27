# CPG Infrastructure

The CPG manages its cloud infrastructure through Pulumi. Specifically, we have developed an abstraction
over the GCP and Azure clouds, that allow us with one `driver.py` to spin up infrastructure
on both GCP and Azure.

Currently, we manage our infrastructure as one Pulumi stack per dataset.
First, ensure that the pulumi virtual environment contains the `cpg_infra` module:

```shell
# install as in-place (-e), so changes to cpg_infra don't need to be reinstalled
stack/venv/bin/python -m pip install -e .
```

You can deploy a stack with:

```shell
cd stack
pulumi up -s [stack]
```


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

```shell
export CPG_CONFIG_PATH=$(readlink -f cpg_infra/cpg.toml)
python -m stack.driver
# prints what resources it would create here (no Pulumi used for now)
```

### Motiviations

This abstraction is still trying to address a number of difficult problems:

- How do you manage the same (ish) infrastructure across two clouds?
- How do you elegantly handle different code pathways for different clouds?
- Can we modularise our deployment for each dataset a bit more?

Still to solve problems:

- How do you define resources that the same stack on a different cloud can use:
  - (eg: I want to put GCP service-account credentials in an Azure secret)
- How do you elegantly _import_ resources to use, for example notebooks / cromwell
