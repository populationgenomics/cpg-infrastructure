# flake8: noqa: F401,PGH003
"""
Registry of concrete ``CloudInfraBase`` subclasses, keyed by their ``name()``.

Sole documented purpose of this module: import every concrete
``CloudInfraBase`` subclass so it registers with ``__subclasses__()``, then
expose the ``name() -> class`` lookup as ``NAME_TO_INFRA_CLASS``.

**Add a new CloudInfraBase subclass here.** If you add a new cloud provider
(e.g. Seqera / Nextflow / whatever comes next) and forget to import it in this
module, ``NAME_TO_INFRA_CLASS`` will silently omit it and
``CPGDatasetInfrastructure`` will fail to instantiate the deploy location.
"""

from typing import Type

# Side-effect imports: bringing these into the process registers each subclass
# with ``CloudInfraBase.__subclasses__()``. They MUST stay even if a linter
# reports them as unused.
from cpg_infra.abstraction.azure import AzureInfra
from cpg_infra.abstraction.base import CloudInfraBase, DryRunInfra
from cpg_infra.abstraction.gcp import GcpInfrastructure


NAME_TO_INFRA_CLASS: dict[str, Type[CloudInfraBase]] = {
    c.name(): c  # type: ignore
    for c in CloudInfraBase.__subclasses__()
}
