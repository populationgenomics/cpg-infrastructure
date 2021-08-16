# Deploying a stack for a dataset

The buckets and permission groups as described in the [storage policies](https://github.com/populationgenomics/team-docs/tree/main/storage_policies)
can be brought up using Pulumi.

1. Create a new GCP project for the stack, corresponding to `$PROJECT` below.
1. [Set a billing budget](https://github.com/populationgenomics/gcp-cost-control#add-billing-budgets) for the GCP project.
1. Create `<dataset>-test`, `<dataset>-standard`, and `<dataset>-full` [service accounts](https://auth.hail.populationgenomics.org.au/users) in Hail Batch (check "Service Account", don't set an email address). Associate them with a new `<dataset>` Hail Batch [billing project](https://batch.hail.populationgenomics.org.au/billing_projects).
1. Configure the Pulumi stack options, either by following the following steps or using an existing `Pulumi.<dataset>.yaml` file as a template:

   - See this [issue](https://github.com/hashicorp/terraform-provider-google/issues/7477)
     regarding the use of the `user_project_override` and `billing_project`
     options below.
   - Retrieve the Hail service account emails from the Kubernetes secret (look for `client_email`):

     ```bash
     for access_level in test standard full; do kubectl get secret $DATASET-$access_level-gsa-key -o json | jq '.data | map_values(@base64d)'; done
     ```

   ```shell
   cd stack
   gcloud auth application-default login
   export PULUMI_CONFIG_PASSPHRASE=
   pulumi login gs://cpg-pulumi-state
   pulumi stack init $DATASET
   pulumi config set gcp:project $PROJECT
   pulumi config set gcp:billing_project $PROJECT
   pulumi config set gcp:user_project_override true
   pulumi config set hail_service_account_test $HAIL_SERVICE_ACCOUNT_TEST
   pulumi config set hail_service_account_standard $HAIL_SERVICE_ACCOUNT_STANDARD
   pulumi config set hail_service_account_full $HAIL_SERVICE_ACCOUNT_FULL
   ```

   - If you want to create a release bucket and access group:

     ```shell
     pulumi config set enable_release true
     ```

   - If you want to customize the archival age in days:

     ```shell
     pulumi config set archive_age 90
     ```

   - If this dataset requires access to other datasets, this can be specified through the optional `depends_on` config setting.

     **Note:** Before adding a dependency, make sure that it's okay to grant everybody who has access to `<dataset>` these implicit permissions to dependent datasets.

     ```shell
     pulumi config set depends_on '["thousand-genomes", "hgdp"]'
     ```

     This will grant read permissions to the `test` / `main` buckets of those dependencies, based on the access level of the service account. This can for example be useful for joint-calling multiple datasets.

1. Deploy the stack:

   ```shell
   gcloud auth application-default login
   python3 -m venv venv
   source venv/bin/activate
   pip3 install -r requirements.txt
   PULUMI_CONFIG_PASSPHRASE= pulumi up  # empty passphrase
   ```

1. Add users to the `<dataset>-access@populationgenomics.org.au` Google Group to enable access through the analysis-runner. To be able to see Hail Batch logs for analysis-runner invocations, users also need to be added to the `<dataset>` Hail Batch billing project.

## Updating all stacks

After any configuration change, you should apply the changes across all datasets, e.g. using [`update_all_stacks.sh`](update_all_stacks.sh). However, make sure that any changes will also be reflected in the `main` branch, as when the state in the repository differs from what's deployed in production, debugging becomes extremely difficult.
