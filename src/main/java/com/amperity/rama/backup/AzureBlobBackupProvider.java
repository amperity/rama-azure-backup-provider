package com.amperity.rama.backup;

import com.rpl.rama.backup.BackupProvider;
import com.rpl.rama.backup.BackupProvider.KeysPage;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.rest.PagedResponse;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakePathClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.DataLakeStorageException;

/***
 * An implementation of a Rama BackupProvider for Azure Blob.
 */
public class AzureBlobBackupProvider implements BackupProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobBackupProvider.class);


  private final DataLakeFileSystemClient fsClient;
  private final String rootPrefix;
  private final ExecutorService executor;

  private static final Duration LIST_PATHS_TIMEOUT = Duration.ofSeconds(30);
  private static final int DEFAULT_PAGE_SIZE = 1000;
  private static final String PATH_NOT_FOUND = "PathNotFound";

  private static void logInfo(String fmt, String... args) {
      LOGGER.info("INFO: " + String.format(fmt, args));
  }

  public AzureBlobBackupProvider(final String location) throws IllegalArgumentException {
      String[] parts = location.split(":");
      if (parts.length != 2) {
          throw new IllegalArgumentException("Invalid argument to construct a backup provider: expected a string in the format <storage-account-name>:<blob-container-name>");
      }
      String storageAccountName = parts[0];
      String containerName = parts[1];
      int pathIndex = containerName.indexOf("/");
      if (pathIndex == -1) {
          rootPrefix = "";
      } else {
          String path = containerName.substring(pathIndex + 1);
          if (path.endsWith("/")) {
              rootPrefix = path;
          } else {
              rootPrefix = path + "/";
          }
          containerName = containerName.substring(0, pathIndex);
      }
      String endpoint = String.format("https://%s.dfs.core.windows.net/", storageAccountName);
      TokenCredential credential = new DefaultAzureCredentialBuilder().build();
      DataLakeServiceClient serviceClient = new DataLakeServiceClientBuilder()
          .endpoint(endpoint)
          .credential(credential)
          .buildClient();
      fsClient = serviceClient.getFileSystemClient(containerName);
      executor = Executors.newCachedThreadPool();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends InputStream> CompletableFuture<T> getObject(final String key) {
      logInfo("get '%s'", rootPrefix + key);
      return CompletableFuture.<T>supplyAsync(() -> {
          try {
              return (T) fsClient.getFileClient(rootPrefix + key).openInputStream().getInputStream();
          } catch (BlobStorageException e) {
              if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
                  return null;
              } else {
                  throw e;
              }
          } catch (DataLakeStorageException e) {
              if (e.getErrorCode().equals("PathNotFound")) {
                  return null;
              } else {
                  throw e;
              }
          }
      });
  }

  @Override
  public CompletableFuture<Void> putObject(final String key, final InputStream inputStream, final Long contentLength) {
      logInfo("put '%s'", rootPrefix + key);
      return CompletableFuture.runAsync(() -> {
          // Check for cancellation before starting
          if (Thread.currentThread().isInterrupted()) {
              logInfo("put '%s' - cancelled before upload started", rootPrefix + key);
              throw new CompletionException(new InterruptedException("Upload was cancelled"));
          }

          DataLakeFileClient fileClient = fsClient.getFileClient(rootPrefix + key);

          try {
              // Always upload, overwriting if file exists (matching S3 behavior)
              fileClient.upload(inputStream, contentLength, true);
          } catch (Exception e) {
              // Check if this was due to interruption/cancellation
              if (Thread.currentThread().isInterrupted() || e instanceof InterruptedException) {
                  logInfo("put '%s' - upload cancelled", rootPrefix + key);
                  throw new CompletionException(new InterruptedException("Upload was cancelled"));
              }
              throw e;
          }
      }, executor);
  }

  @Override
  public CompletableFuture<Void> deleteObject(final String key) {
      logInfo("delete '%s'", rootPrefix + key);
      return CompletableFuture.runAsync(() -> {
          fsClient.getFileClient(rootPrefix + key).delete();
      });
  }

  @Override
  public CompletableFuture<Boolean> hasKey(final String key) {
      logInfo("exists? '%s'", rootPrefix + key);
      return CompletableFuture.supplyAsync(() -> {
          return fsClient.getFileClient(rootPrefix + key).exists();
      });
  }

  @Override
  public CompletableFuture<BackupProvider.KeysPage> listKeysRecursive(final String prefix, final String paginationKey) {
      return CompletableFuture.supplyAsync(() -> {
          String finalPrefix = rootPrefix + prefix;
          logInfo("listRecursive '%s'", finalPrefix);
          ListPathsOptions options = new ListPathsOptions();
          options.setPath(finalPrefix);
          options.setRecursive(true);
          PagedResponse<PathItem> response;
          try {
              response = fsClient
                  .listPaths(options, LIST_PATHS_TIMEOUT)
                  .iterableByPage(paginationKey, DEFAULT_PAGE_SIZE)
                  .iterator()
                  .next();
          } catch (DataLakeStorageException e) {
              if (e.getErrorCode().equals(PATH_NOT_FOUND)) {
                  return new BackupProvider.KeysPage(Collections.emptyList(), null);
              } else {
                  throw e;
              }
          }
          List<String> keys = response
              .getElements()
              .stream()
              .filter(item -> !item.isDirectory())
              // modify paths to be relative to the backup provider's root
              .map(item -> item.getName().replaceFirst(rootPrefix, ""))
              .collect(Collectors.toList());
          return new BackupProvider.KeysPage(keys, response.getContinuationToken());
      });
  }

  @Override
  public CompletableFuture<BackupProvider.KeysPage> listKeysNonRecursive(final String prefix, final String paginationKey, final int pageSize) {
      return CompletableFuture.supplyAsync(() -> {
          String finalPrefix = rootPrefix + prefix;
          logInfo("listNonRecursive '%s'", finalPrefix);

          // Special case: if prefix doesn't end with "/" and is itself a directory,
          // S3 would return it in commonPrefixes. Match this behavior.
          if (!prefix.isEmpty() && !prefix.endsWith("/")) {
              try {
                  DataLakePathClient pathClient = fsClient.getFileClient(finalPrefix);
                  if (pathClient.exists() && pathClient.getProperties().isDirectory()) {
                      // Return the directory itself, not its contents
                      return new BackupProvider.KeysPage(Collections.singletonList(prefix), null);
                  }
              } catch (Exception e) {
                  // If we can't determine, fall through to normal listing
              }
          }

          ListPathsOptions options = new ListPathsOptions();
          options.setPath(finalPrefix);
          options.setRecursive(false);
          PagedResponse<PathItem> response;
          try {
              response = fsClient
                  .listPaths(options, LIST_PATHS_TIMEOUT)
                  .iterableByPage(paginationKey, pageSize > 0 ? pageSize : DEFAULT_PAGE_SIZE)
                  .iterator()
                  .next();
          } catch (DataLakeStorageException e) {
              if (e.getErrorCode().equals(PATH_NOT_FOUND)) {
                  return new BackupProvider.KeysPage(Collections.emptyList(), null);
              } else {
                  throw e;
              }
          }
          // Map items to their names following S3 pattern:
          // - Files: just the filename
          // - Directories: filename if prefix ends with "/", otherwise full relative path
          List<String> keys = response
              .getElements()
              .stream()
              .map(item -> {
                  if (item.isDirectory()) {
                      String dirPath = item.getName().replaceFirst(rootPrefix, "");
                      // If prefix ends with "/", return just the directory name
                      // Otherwise return the full path relative to root
                      return prefix.endsWith("/")
                          ? Paths.get(dirPath).getFileName().toString()
                          : dirPath;
                  } else {
                      // For files, always return just the filename
                      return Paths.get(item.getName()).getFileName().toString();
                  }
              })
              .collect(Collectors.toList());
          return new BackupProvider.KeysPage(keys, response.getContinuationToken());
      });
  }

  @Override
  public void close() {
      executor.shutdown();
  }
}
