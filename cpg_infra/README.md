# CPG Infrastructure abstraction

This module allows spinning up infrastructure on multiple clouds (GCP + Azure),
by providing a rudimentary abstraction. We have one implementation of an
infrastructure in the `cpg_infra.driver` module.

We currently treat a dataset as a separate stack

Structure:

- The `config.py` defines a dataset's configuration
- The `abstraction/` folder:
  - `base.py` declares an interface for a cloud abstraction
  - `gcp.py` / `azure.py` - implementations for specific clouds
- The `driver.py` turns this configuration in a pulumi state by calling methods on an `infra`.

To develop, you can run the driver file directly, which given a config TOML, will print infrastructure to the console.

```shell
export CPG_CONFIG_PATH=$(readlink -f cpg_infra/cpg.toml)
python -m cpg_infra.driver
# prints what resources it would create here (no Pulumi used for now)
```

## Running pulumi

First, ensure that the pulumi venv contains all the required
dependencies. To do so from the root folder run:

```shell
stack/venv/bin/python -m pip install -r stack/requirements.txt
stack/venv/bin/python -m pip install -r stack/requirements-dev.txt
stack/venv/bin/python -m pip install -e ./
```

In order to deploy a particular pulumi stack run the following

```shell
pulumi up -s [stack] --config-file ../stack/Pulumi.[stack].yaml
```

## Motivations

This abstraction is trying to address a number of difficult problems:

- How do you manage the same (ish) infrastructure across two clouds?
- How do you elegantly handle different code pathways for different clouds?
