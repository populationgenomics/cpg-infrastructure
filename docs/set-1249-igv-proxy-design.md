# SET-1249 — IGV desktop proxy integration: design

Design and implementation plan for [SET-1249](https://cpg-populationanalysis.atlassian.net/browse/SET-1249),
a child of epic SET-1248 ("IGV Desktop Proxy External User Access").

**Status:** implemented. See the first note under "Notes for review" for the one
correction the implementation forced on this design.

## Problem

Users listed under a new `igv-desktop-access` key in a dataset's `members.yaml` are, by
definition, people who do *not* hold personal IAM on that dataset's buckets. The IGV desktop
proxy today forwards the caller's own OAuth token to GCS and never uses its own identity, so
GCS IAM does all the enforcement and these users get nothing.

To serve them, the proxy needs two things that only `cpg-infrastructure` can provide:

1. an allow-list — a secret mapping user email to the buckets they may reach via the proxy;
2. read access for the proxy's own service account on those buckets, so it can serve a user
   who has no personal grant.

Proxy-side enforcement (read the secret, check the user, serve the object as the SA) is
epic SET-1248 work in the `igv-desktop-proxy` repos, **not** this ticket.

## Deployment topology

The proxy runs as two independent Pulumi stacks in **two separate GCP projects** — one for
`prod`, one for `dev` — each with its own Cloud Run service and its own runtime service
account. This matters: a secret is created in exactly one project, so serving both stacks
means two secrets, not one secret with two readers.

Project ids and service account emails are configuration, and live in
`cpg-infrastructure-private`. They are deliberately not reproduced here.

> **Naming trap for whoever fills in the config:** the dev project also contains an App
> Engine default service account whose name differs from the correct proxy runtime SA only
> in its domain. `server_machine_account` is an unvalidated `str`, so nothing in this repo
> would catch the wrong one being pasted. Both correct values were verified live against
> their projects with `gcloud iam service-accounts list --project <project>` on 2026-09-07.

## Decisions

| # | Decision |
|---|---|
| **D1** | **Both stacks are in scope.** The prod SA gets READ on `cpg-<dataset>-main` always, plus `cpg-<dataset>-test` when opted in (D4). The dev SA gets READ on `cpg-<dataset>-test` **only, never main**. |
| **D2** | **One shared `igv-desktop-access` member key** in `members.yaml` drives both stacks. No dev-specific key. |
| **D3** | Config block is **keyed by proxy stack** (`prod`/`dev`), nested under `gcp:` so `gcp:` stays the cloud discriminator. `dev` is optional. |
| **D4** | prod gets `main` always; `test` only when explicitly opted in, via `include_test_buckets: bool = False` on a **prod-only** sub-block. When on, the prod SA also gets READ on `cpg-<dataset>-test` **and** the prod secret lists the `-test` buckets — IAM without a matching allow-list entry would be inert. |
| **D5** | **Two secrets**, same `secret_id` `igv-proxy-config`, one in each stack's own project. No cross-project IAM. |
| **D6** | Both secrets share schema and user list. `cpg-infrastructure` performs the `-main`→`-test` rewrite when writing the **dev** secret, so the dev secret is self-consistent and the proxy needs no per-stack special-casing. |
| **D7** | **One summary `pulumi.warn` per deploy** naming the affected datasets — not one per bucket or per dataset. |
| **D8** | **`raise ValueError`** on an unknown member key, or a member with no `clouds['gcp']` entry. Follows the data-dropbox model. |
| **D9** | This repo **does not create** the proxy service accounts. They already exist; their emails are passed in as config strings. |
| **D10** | The feature **no-ops entirely** when `igv_proxy` is absent; the dev half no-ops when `gcp.dev` is absent. |

Also binding, carried over from earlier design discussion:

- **No Google Group** is created for `igv-desktop-access`. It is read as a plain list; these
  users get no IAM anywhere. (Precedent: `dataset_seqera_infrastructure.py:391`.)
- **Exception list only** — the secret contains only explicit `igv-desktop-access` members,
  not everyone with effective read on the bucket.
- **No `depends_on` transitivity** — a member listed under `dataset-a` gets
  `cpg-dataset-a-main` and nothing else.

An earlier decision restricted this to the `main` bucket only. D1/D4/D6 deliberately
supersede it: access is now per-stack, prod→main and dev→test.

## Implementation

### 1. `cpg_infra/config/config.py`

New `IgvProxy(ConfigModel)` beside `DataDropbox`, plus a top-level
`igv_proxy: IgvProxy | None = None` field beside `data_dropbox`, and
`'igv-desktop-access'` added to the module-level `GroupName` `Literal` that types
`CPGDatasetConfig.members` (see the first note under "Notes for review" — this was not in
the original design).

```python
class IgvProxy(ConfigModel):
    class GCPDeployment(ConfigModel):
        project: str
        server_machine_account: str

    class GCPProdDeployment(GCPDeployment):
        # When True, the prod proxy SA additionally gets READ on cpg-<dataset>-test,
        # and the prod secret lists the '-test' buckets alongside '-main'.
        # Deliberately NOT on the base class: ConfigModel is extra='forbid', so a
        # dev: block setting this fails validation. The dev proxy can never be
        # granted main-namespace access through config.
        include_test_buckets: bool = False

    class GCP(ConfigModel):
        prod: 'CPGInfrastructureConfig.IgvProxy.GCPProdDeployment'
        dev: 'CPGInfrastructureConfig.IgvProxy.GCPDeployment | None' = None

    gcp: GCP
```

Two notes on the shape:

- **Forward refs must be fully qualified from the module root.** `IgvProxy` is nested inside
  `CPGInfrastructureConfig`, so it is not a module-level name and pydantic cannot resolve
  `'IgvProxy.GCPProdDeployment'`. Unquoted references fail too — a nested class body cannot
  see names from the enclosing class body. The existing `Seqera` block uses the correct
  idiom (`config.py:301-302`). `gcp: GCP` and the `GCPProdDeployment(GCPDeployment)` base
  are same-body references and are fine unquoted.
- **The prod/dev asymmetry is load-bearing.** A symmetric
  `namespaces: list[Literal['main', 'test']]` would be tidier and fully config-driven, but
  it would make `dev: {namespaces: ['main']}` an expressible config. D1 exists to make that
  impossible, not merely discouraged. `extra='forbid'` on the asymmetric classes enforces it.

### 2. `cpg_infra/driver/constants.py` and `cpg_infra/driver/infrastructure.py`

- Add `IGV_DESKTOP_ACCESS = 'igv-desktop-access'` to `driver/constants.py`.
- Add `generate_igv_proxy_config()` on `CPGInfrastructure`, mirroring
  `generate_dropbox_config()`, called alongside it. Returns immediately if
  `not self.config.igv_proxy`.
- Build the **base map** once: for each dataset with a non-empty
  `dataset_config.members[IGV_DESKTOP_ACCESS]`, resolve each member key via
  `config.users[key].clouds['gcp'].id` and append
  `f'{config.gcp.dataset_storage_prefix}{dataset}-main'` to that email's list. Raise
  `ValueError` on an unknown key or a missing gcp entry (D8).
- **Two base maps, one per namespace** — `-main` names for every participating dataset, and
  `-test` names for those participating datasets that also have `setup_test` on. *(Revised
  during implementation. The original plan built one `-main` map and rewrote it with
  `removesuffix('-main') + '-test'` for dev. Deriving each namespace's name from the dataset
  directly is equivalent, drops the `removesuffix`-vs-`replace` trap entirely, and — the
  reason for the change — lets the `-test` names be gated on `setup_test`.
  `setup_storage_test_buckets_permissions` only runs when `setup_test` is on, so a
  `setup_test: false` dataset would otherwise be handed a `-test` bucket that has neither a
  binding nor an existence.)*
- **Prod payload** = the main map, plus the test map merged in only if
  `gcp.prod.include_test_buckets`.
- **Dev payload** = the test map on its own — never the prod payload with its names
  rewritten, which would double up the `-test` entries when `include_test_buckets` is on.
- Bucket names are **derived plain strings**, not read off Pulumi resources, so the payload
  stays a static string with no `Output` interpolation. They repeat the naming scheme
  `create_bucket` uses; the driver tests pin the resulting names, which is what would catch
  a drift.
- **Sort** the user keys and each bucket list before `json.dumps`, otherwise dict ordering
  churns a new secret version on every deploy. **Sort only — no `set()`**: de-duplicating at
  serialisation time would mask a payload that wrongly repeats a bucket, which is exactly
  the regression the dev-payload test above exists to catch.
- Write each secret with `create_secret(...)` → `add_secret_version(...)` →
  `add_secret_member(..., SecretMembership.ACCESSOR)`, using each stack's own project and SA.
- Emit **one** `pulumi.warn` per deploy (D7), **iff the dev secret grants the dev proxy
  something** — stating the access level it walks away with (read on `cpg-<dataset>-test`,
  never main-namespace data) and naming those datasets. The message deliberately describes
  the *grant*, not a `-main`→`-test` transformation: the implementation derives each
  namespace's bucket name from the dataset directly, so no rewrite step exists to describe.
  A grant of nothing means nothing to flag, so there is no warning on a prod-only deploy
  (including with `include_test_buckets: true`), nor on a dev deploy where no participating
  dataset has a test namespace. The datasets named are only the dev-readable ones, not
  every participating one.

> **Pass an explicit `resource_key` to both `create_secret` and `add_secret_version` for
> each stack.** Both secrets are created on `common_gcp_infra`, and `get_pulumi_name`
> prefixes only `{dataset}-{cloud}-` — so the same `secret_id` in two *different GCP
> projects* still collides on the Pulumi resource name. The data-dropbox precedent does not
> pass `resource_key`; copying it verbatim breaks. Use e.g. `igv-proxy-config-prod` /
> `igv-proxy-config-dev`.

### 3. `cpg_infra/driver/dataset_cloud_infrastructure.py`

Per-dataset bindings, each guarded like the web-server SA precedent —
`isinstance(self.infra, GcpInfrastructure)`, `self.config.igv_proxy is not None`, and a
non-empty `dataset_config.members.get(IGV_DESKTOP_ACCESS)`:

- **main bucket** — bind the prod SA to `self.main_bucket` with `BucketMembership.READ`.
- **test bucket** (in `setup_storage_test_buckets_permissions`) — up to two bindings on
  `self.test_bucket`, separately guarded:
  - `gcp.dev is not None` ⇒ bind the dev SA;
  - `gcp.prod.include_test_buckets` ⇒ bind the prod SA (D4).

Bind the **SA email directly**, not via an existing group. Reusing a group was considered
and rejected on the evidence below; the original note here cited only `main-tmp` and
`main-analysis`, which undersold the gap.

| Group | Read-only on data? | Metamist | Transitive via `depends_on`? |
|---|---|---|---|
| `main_read_group` | **No** — `APPEND` (`StorageViewerAndCreator`) on `main-web`; also READ on `main-tmp`, `main-analysis`, every `main-upload` | `SM_MAIN_READ` | **Yes** |
| `test_read_group` | Yes on buckets (all five `test-*`) | **`SM_TEST_WRITE`** | **Yes** (when `setup_test`) |
| `external_repository_reader_group` | **Yes** — READ on `main` and nothing else | none | No |

`external_repository_reader_group` is the only group that is read-only on data with no write
anywhere and no transitivity, and it covers `main` only — there is no test-namespace
equivalent, so it could not serve the dev stack. It is also populated from the
`external-repository-reader` key in `members.yaml`, so putting a service identity in it
would conflate the proxy with the humans granted external-repository read access, and would
tie the proxy's access to a group whose membership changes for unrelated reasons.

Group membership is also transitive *upward*: `<dataset>-main-read` is added as a member of
`<dependency>-main-read` (`dataset_cloud_infrastructure.py:2506`), so joining it would grant
the proxy read across every `depends_on` / `depends_on_readonly` dataset and the common
dataset — contradicting the "no `depends_on` transitivity" decision above, and weakening the
allow-list secret as a bound on effective access while proxy-side enforcement (SET-1248)
does not yet exist.

The two test-bucket `resource_key`s must differ from each other: same dataset, same bucket,
two members. Dataset scoping is automatic via `get_pulumi_name`, so keys only need to be
distinct within a dataset.

## Tests

`test/test_config_validation.py`:

- the `IgvProxy` block parses; `gcp.dev` omitted is valid; absent `igv_proxy` is valid;
- `include_test_buckets` defaults to `False` when omitted from `prod`;
- **`dev: {include_test_buckets: true}` raises a pydantic `ValidationError`** — this pins
  D1's safety property and must not be deleted.

New driver tests, in the style of `test/test_seqera_infrastructure.py`:

- prod payload matches the ticket's shape with `-main` names (`include_test_buckets` off);
- a `setup_test: false` dataset contributes `-main` but never `-test`, in either payload;
- exactly one `pulumi.warn` per deploy, stating the access granted and naming only the
  dev-readable datasets; and **no** warning when the dev proxy is granted nothing — a
  prod-only deploy (with `include_test_buckets` both off and on), a dev deploy where no
  participating dataset has a test namespace, and a deploy where no dataset participates
  at all;
- `include_test_buckets` on ⇒ prod payload gains the `-test` names and a prod test-bucket
  binding is emitted;
- `include_test_buckets` on **and** `gcp.dev` set ⇒ the dev payload still contains only
  `-test` names with no duplicates (guards the base-map-vs-prod-payload bug);
- dev payload is the same users and datasets with `-test` names;
- deterministic ordering — the same input twice produces byte-identical `json.dumps`;
- `ValueError` on an unknown member key;
- `ValueError` on a member with no `clouds['gcp']` entry;
- `gcp.dev is None` ⇒ no dev secret and no dev test-bucket binding;
- `igv_proxy is None` ⇒ complete no-op.

## Notes for review

- **Corrected during implementation — this PR must land first.** The design assumed
  `CPGDatasetConfig.members` was free-form. It is not: it is typed
  `dict[GroupName, list[MemberKey]]`, and `GroupName` is a `Literal` of the nine existing
  group names, so pydantic rejects an unrecognised key outright (verified: an unknown
  member key raises `ValidationError`). `igv-desktop-access` therefore had to be added to
  that `Literal`, and adding the key to `members.yaml` in `cpg-infrastructure-private`
  **fails validation until this change is deployed**. The rest of the original reasoning
  still holds: `setup_externally_specified_members` iterates a hardcoded list of group
  objects, so the new key creates no Google Group and grants no IAM by itself.
- `cpg-infrastructure-private`'s membership check workflow already validates the new group
  with no change needed — it is group-name-agnostic and blocks the config PR. It only checks
  that a key exists in `users.yaml`; it does **not** check for a `clouds['gcp']` entry, which
  is why D8 raises here.
- Note that `pulumi preview` does not run automatically on `cpg-infrastructure-private` PRs;
  that workflow is `workflow_dispatch:` only.
- **Architectural consequence worth an explicit sign-off:** granting a proxy SA read on
  participating dataset buckets moves part of the authorization boundary off GCS IAM and
  onto this secret plus proxy-side enforcement. That enforcement does not exist yet. The
  proxy's request-authentication behaviour should be reviewed as part of SET-1248 before
  these grants are relied on in production.
