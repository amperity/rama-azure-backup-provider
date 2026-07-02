A backup provider for Rama that uses Azure Blob Storage (Azure Data Lake Storage Gen2).

# Usage

To use the provider, download the provided jar from the releases page and include it in the `lib/` directory of the Conductor and Supervisor nodes.

Set the `backup.provider` config to:

`com.amperity.rama.backup.AzureBlobBackupProvider <storage-account-name>:<container-name>`

Or to use a subdirectory within the container:

`com.amperity.rama.backup.AzureBlobBackupProvider <storage-account-name>:<container-name>/<path>`

Replace `<storage-account-name>` with your Azure storage account name, `<container-name>` with the container you wish to use, and optionally `<path>` with a subdirectory path.

It is advisable to create the container with the desired permissions and configuration before using it with Rama.

# Credentials

By default the provider authenticates with [Managed Identity](https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview), which is the recommended way to provide credentials when running Rama on Azure. Managed Identity only works on an Azure VM — off-VM it fails trying to reach the instance metadata endpoint (`169.254.169.254`).

For local development, set `AZURE_USE_DEFAULT_CREDENTIAL=true` to switch to the Azure [DefaultAzureCredential](https://learn.microsoft.com/en-us/java/api/com.azure.identity.defaultazurecredential) flow, which supports multiple authentication methods in the following order:

1. Environment variables (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`)
2. Managed Identity (when running on Azure)
3. Azure CLI credentials (`az login`)
4. Azure PowerShell credentials
5. Interactive browser authentication

# Tests

To run integration tests:

```bash
mvn verify
```

The tests require valid Azure credentials (see Credentials section above) and expect the following environment variables:

- `AZURE_STORAGE_ACCOUNT`: The Azure storage account name
- `AZURE_CONTAINER`: The container name (optionally with path prefix)

Alternatively, you can hardcode test configuration in the test files, but using environment variables is recommended.

## Running locally

The suite talks to a real storage account, so a local run needs Azure CLI credentials rather than Managed Identity:

```bash
az login                                    # authenticate the Azure CLI
export AZURE_USE_DEFAULT_CREDENTIAL=true    # use CLI creds instead of Managed Identity
export AZURE_STORAGE_ACCOUNT=amperityaztest # test storage account
export AZURE_CONTAINER=test
mvn verify
```

Notes:

- Without `AZURE_USE_DEFAULT_CREDENTIAL=true` the provider tries Managed Identity and each test hangs for ~170s before failing with a `NoRouteToHost` to `169.254.169.254`.
- The logged-in identity needs a data-plane role (e.g. **Storage Blob Data Contributor**) on the account; control-plane roles alone return 403 on blob operations.
- If the storage account restricts network access, connect to the VPN first.
