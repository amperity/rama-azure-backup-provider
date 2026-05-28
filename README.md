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

The Azure backup provider uses the Azure [DefaultAzureCredential](https://learn.microsoft.com/en-us/java/api/com.azure.identity.defaultazurecredential) authentication flow, which supports multiple authentication methods in the following order:

1. Environment variables (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`)
2. Managed Identity (when running on Azure)
3. Azure CLI credentials
4. Azure PowerShell credentials
5. Interactive browser authentication

The recommended way to provide credentials when running Rama on Azure is to use [Managed Identity](https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview).

For local development, use `az login` to authenticate with the Azure CLI.

# Tests

To run integration tests:

```bash
mvn verify
```

The tests require valid Azure credentials (see Credentials section above) and expect the following environment variables:

- `AZURE_STORAGE_ACCOUNT`: The Azure storage account name
- `AZURE_CONTAINER`: The container name (optionally with path prefix)

Alternatively, you can hardcode test configuration in the test files, but using environment variables is recommended.
