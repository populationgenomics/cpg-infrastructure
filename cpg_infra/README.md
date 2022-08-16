# CPG Infrastructure (rewrite)

Date: August, 2022

> The CPG’s current infrastructure has been in place for 2 years. With the addition of Azure as well as GCP, now is a good time to reconsider how we achieve certain infrastructure components for future proofing.

To manage infrastructure across GCP and Azure, as suggested by Greg Smith (Microsoft), we should write an abstraction on top of Pulumi for spinning up infrastructure in GCP and Azure without having to duplicate the “infrastructure design”.


## Proposed process

To manage this process in a controlled way, without requiring massive code reviews, here is an proposal for the implementation process:

1. Move infrastructure code from analysis-runner repo to NEW cpg-infrastructure
2. Add Pulumi cloud abstraction proposed above, and implement EXISTING infrastructure
3. Migrate group access caching to groups managed in version-control
4. Migrate to NEW group structure

## This abstraction

- It all belongs in `cpg_infra`
- The `config.py` defines a dataset configuration
- The `abstraction/` folder:
  - `base.py` declares an interface for a cloud abstraction
  - `gcp.py` / `azure.py` - implementations for specific clouds
- The `driver.py` turns this configuration in a pulumi state by calling methods on a `infra`.

Currently it's in a DEV state, it won't create any Pulumi state and there is a logger if you run:

```shell
python -m cpg_infra.driver
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
