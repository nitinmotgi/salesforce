# Salesforce to GCS Action

## Description

This plugin retrieves data from Salesforce for a given Object using the Salesforce
[Bulk API](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/),
and stores it in a GCS bucket.

## Credentials

### Google Cloud

If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access BigQuery and Google Cloud Storage.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

### Salesforce

This plugin uses OAuth2 for authentication with Salesforce. It expects users to provide a Client ID,
a Client Secret, a user name and password to authenticate with the Salesforce login API and retrieve an OAuth2 token
and the instance URL.

## Properties

**Client ID**: The client ID for the connected app on your Salesforce instance. Available as "Consumer Key" on the
Salesforce UI.

**Client Secret**: The client secret key for the connected app on your Salesforce instance. Available as
"Consumer Secret" on the Salesforce UI

**Username**: Your Salesforce username.

**Password**: Your Salesforce password.

**Object**: The Salesforce object to retrieve data from

**API Version**: The Salesforce Developer API version. Defaults to 45.0.

**Login URL**: The URL to use to authenticate to Salesforce. Defaults to https://login.salesforce.com/services/oauth2/token.

**Query**: The SOQL query to use to retrieve data.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Bucket**: The bucket where Salesforce data should be stored

**Path**: Relative path to a directory under the specified bucket where the Salesforce data should be stored.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.
