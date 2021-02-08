# Deploying a stack for a dataset

The buckets and permission groups as described in the [storage policies](https://github.com/populationgenomics/team-docs/tree/main/storage_policies)
can be brought up using Pulumi.

1. Create a new GCP project for the stack, corresponding to `$PROJECT` below.
1. Configure the Pulumi stack options:

   - See this [issue](https://github.com/hashicorp/terraform-provider-google/issues/7477)
     regarding the use of the `user_project_override` and `billing_project`
     options below.
   - Retrieve the Hail service account email from the Kubernetes secret:
     `kubectl get secret $PROJECT-gsa-key -o json | jq '.data | map_values(@base64d)'`

   ```shell
   cd stack
   gcloud auth application-default login
   pulumi login gs://cpg-pulumi-state
   pulumi stack init $STACK
   pulumi config set gcp:project $PROJECT
   pulumi config set gcp:billing_project $PROJECT
   pulumi config set gcp:user_project_override true
   pulumi config set hail_service_account $HAIL_SERVICE_ACCOUNT
   ```

   - If you want to create a release bucket and access group:

     ```shell
     pulumi config set enable_release true
     ```

   - If you want to customize the archival age in days:

     ```shell
     pulumi config set archive_age 90
     ```

1. Deploy the stack:

   ```shell
   gcloud auth application-default login
   python3 -m venv venv
   source venv/bin/activate
   pip3 install -r requirements.txt
   PULUMI_CONFIG_PASSPHRASE= pulumi up  # empty passphrase
   ```
