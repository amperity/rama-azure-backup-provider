package com.amperity.rama.backup;

import com.rpl.rama.backup.BackupProvider;
import com.rpl.rama.backup.BackupProvider.KeysPage;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.rest.PagedResponse;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
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
  private static final int MAX_THREADS = 100;
  private static final int QUEUE_SIZE = 1000;

  /**
   * Creates a bounded thread pool executor with named daemon threads.
   * Similar to Executors.newCachedThreadPool() but with upper bounds to prevent
   * resource exhaustion under high load.
   *
   * @param namePrefix prefix for thread names (e.g., "AzureBlobBackupProvider")
   * @param maxThreads maximum number of threads (sized for Rama cluster workloads)
   * @return configured ExecutorService with bounded thread pool
   */
  private static ExecutorService createBoundedExecutor(String namePrefix, int maxThreads) {
      AtomicInteger threadNumber = new AtomicInteger(1);
      ThreadFactory threadFactory = runnable -> {
          Thread thread = new Thread(runnable, namePrefix + "-" + threadNumber.getAndIncrement());
          thread.setDaemon(true);
          return thread;
      };

      return new ThreadPoolExecutor(
          0,                                      // corePoolSize (same as cached pool)
          maxThreads,                             // maximumPoolSize (bounded)
          60L, TimeUnit.SECONDS,                  // keepAliveTime (same as cached pool)
          new LinkedBlockingQueue<>(QUEUE_SIZE),  // bounded work queue
          threadFactory,
          new ThreadPoolExecutor.CallerRunsPolicy() // backpressure when queue is full
      );
  }

  /**
   * Checks if an exception represents a "not found" error from Azure storage.
   * Handles both BlobStorageException and DataLakeStorageException.
   *
   * @param e the exception to check
   * @return true if this is a "not found" error, false otherwise
   */
  private static boolean isNotFoundException(Exception e) {
      if (e instanceof BlobStorageException) {
          return ((BlobStorageException) e).getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND);
      }
      if (e instanceof DataLakeStorageException) {
          return ((DataLakeStorageException) e).getErrorCode().equals(PATH_NOT_FOUND);
      }
      return false;
  }

  /**
   * Converts a relative key to a full path by prepending the root prefix.
   *
   * @param key the relative key
   * @return the full path including root prefix
   */
  private String toFullPath(String key) {
      return rootPrefix + key;
  }

  public AzureBlobBackupProvider(final String location) throws IllegalArgumentException {
      if (location == null || location.trim().isEmpty()) {
          throw new IllegalArgumentException("Location cannot be null or empty");
      }

      String[] parts = location.split(":", 2);
      if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
          throw new IllegalArgumentException(
              "Invalid location format. Expected: <storage-account-name>:<container-name>[/path], got: " + location);
      }

      String storageAccountName = parts[0].trim();

      // Validate Azure storage account name format
      // Rules: 3-24 characters, lowercase letters and numbers only
      if (storageAccountName.length() < 3 || storageAccountName.length() > 24) {
          throw new IllegalArgumentException(
              "Invalid storage account name '" + storageAccountName + "': must be between 3 and 24 characters");
      }
      if (!storageAccountName.matches("^[a-z0-9]+$")) {
          throw new IllegalArgumentException(
              "Invalid storage account name '" + storageAccountName + "': must contain only lowercase letters and numbers");
      }
      String containerName = parts[1].trim();
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
      // Use ManagedIdentityCredential directly for faster authentication on Azure VMs
      // Falls back to DefaultAzureCredential if AZURE_USE_DEFAULT_CREDENTIAL=true
      TokenCredential credential;
      if ("true".equalsIgnoreCase(System.getenv("AZURE_USE_DEFAULT_CREDENTIAL"))) {
          credential = new DefaultAzureCredentialBuilder().build();
      } else {
          credential = new ManagedIdentityCredentialBuilder().build();
      }
      DataLakeServiceClient serviceClient = new DataLakeServiceClientBuilder()
          .endpoint(endpoint)
          .credential(credential)
          .buildClient();
      fsClient = serviceClient.getFileSystemClient(containerName);
      executor = createBoundedExecutor("AzureBlobBackupProvider", MAX_THREADS);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends InputStream> CompletableFuture<T> getObject(final String key) {
      LOGGER.info("get '{}'", toFullPath(key));
      return CompletableFuture.<T>supplyAsync(() -> {
          try {
              return (T) fsClient.getFileClient(toFullPath(key)).openInputStream().getInputStream();
          } catch (Exception e) {
              if (isNotFoundException(e)) {
                  return null;
              }
              throw e;
          }
      }, executor);
  }

  @Override
  public CompletableFuture<Void> putObject(final String key, final InputStream inputStream, final Long contentLength) {
      LOGGER.info("put '{}'", toFullPath(key));
      return CompletableFuture.runAsync(() -> {
          // Check for cancellation before starting
          if (Thread.currentThread().isInterrupted()) {
              LOGGER.info("put '{}' - cancelled before upload started", toFullPath(key));
              throw new CompletionException(new InterruptedException("Upload was cancelled"));
          }

          if (contentLength == null) {
              throw new IllegalArgumentException("contentLength cannot be null");
          }

          DataLakeFileClient fileClient = fsClient.getFileClient(toFullPath(key));

          try {
              if (contentLength == 0) {
                  // Azure Data Lake Storage Gen2 limitation: upload() rejects Content-Length: 0
                  //
                  // Azure returns HTTP 400 - InvalidHeaderValue error:
                  //   {
                  //     "error": {
                  //       "code": "InvalidHeaderValue",
                  //       "message": "The value for one of the HTTP headers is not in the correct format",
                  //       "detail": {
                  //         "HeaderName": "Content-Length",
                  //         "HeaderValue": "0"
                  //       }
                  //     }
                  //   }
                  //
                  // This is a validation error thrown before upload attempt. Azure API validates that
                  // Content-Length header cannot be "0" even though it's a valid HTTP header value.
                  //
                  // Workaround: Use create() instead of upload() for zero-byte files.
                  // This matches S3 behavior which accepts empty objects.
                  //
                  // References:
                  // - https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
                  // - https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
                  fileClient.create(true);  // overwrite=true
              } else {
                  // Always upload, overwriting if file exists (matching S3 behavior)
                  fileClient.upload(inputStream, contentLength, true);
              }
          } catch (Exception e) {
              // Check if this was due to interruption/cancellation
              if (Thread.currentThread().isInterrupted() || e instanceof InterruptedException) {
                  LOGGER.info("put '{}' - upload cancelled", toFullPath(key));
                  throw new CompletionException(new InterruptedException("Upload was cancelled"));
              }
              throw e;
          }
      }, executor);
  }

  @Override
  public CompletableFuture<Void> deleteObject(final String key) {
      LOGGER.info("delete '{}'", toFullPath(key));
      return CompletableFuture.runAsync(() -> {
          fsClient.getFileClient(toFullPath(key)).delete();
      }, executor);
  }

  @Override
  public CompletableFuture<Boolean> hasKey(final String key) {
      LOGGER.info("exists? '{}'", toFullPath(key));
      return CompletableFuture.supplyAsync(() -> {
          return fsClient.getFileClient(toFullPath(key)).exists();
      }, executor);
  }

  @Override
  public CompletableFuture<BackupProvider.KeysPage> listKeysRecursive(final String prefix, final String paginationKey) {
      return CompletableFuture.supplyAsync(() -> {
          String finalPrefix = toFullPath(prefix);
          LOGGER.info("listRecursive '{}'", finalPrefix);
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
              .map(item -> {
                  String name = item.getName();
                  // Remove rootPrefix using substring to avoid regex interpretation
                  if (!rootPrefix.isEmpty() && name.startsWith(rootPrefix)) {
                      return name.substring(rootPrefix.length());
                  }
                  return name;
              })
              .collect(Collectors.toList());
          return new BackupProvider.KeysPage(keys, response.getContinuationToken());
      }, executor);
  }

  @Override
  public CompletableFuture<BackupProvider.KeysPage> listKeysNonRecursive(final String prefix, final String paginationKey, final int pageSize) {
      return CompletableFuture.supplyAsync(() -> {
          String finalPrefix = toFullPath(prefix);
          LOGGER.info("listNonRecursive '{}'", finalPrefix);

          // Special case: if prefix doesn't end with "/" and is itself a directory,
          // S3 would return it in commonPrefixes. Match this behavior.
          if (!prefix.isEmpty() && !prefix.endsWith("/")) {
              try {
                  DataLakePathClient pathClient = fsClient.getFileClient(finalPrefix);
                  // Check properties directly - no need for separate exists() check
                  // which creates a race condition (file could be deleted between calls)
                  if (pathClient.getProperties().isDirectory()) {
                      // Return the directory itself, not its contents
                      return new BackupProvider.KeysPage(Collections.singletonList(prefix), null);
                  }
              } catch (DataLakeStorageException e) {
                  // Path doesn't exist or isn't accessible - fall through to normal listing
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
                  String name = item.getName();
                  // Remove rootPrefix using substring to avoid regex interpretation
                  String relativePath;
                  if (!rootPrefix.isEmpty() && name.startsWith(rootPrefix)) {
                      relativePath = name.substring(rootPrefix.length());
                  } else {
                      relativePath = name;
                  }

                  if (item.isDirectory()) {
                      // If prefix ends with "/", return just the directory name
                      // Otherwise return the full path relative to root
                      return prefix.endsWith("/")
                          ? Paths.get(relativePath).getFileName().toString()
                          : relativePath;
                  } else {
                      // For files, always return just the filename
                      return Paths.get(name).getFileName().toString();
                  }
              })
              .collect(Collectors.toList());
          return new BackupProvider.KeysPage(keys, response.getContinuationToken());
      }, executor);
  }

  @Override
  public void close() {
      executor.shutdown();
      try {
          // Wait for existing tasks to complete
          if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
              // Force shutdown if tasks didn't complete
              executor.shutdownNow();
              // Wait again for forced shutdown to complete
              if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                  LOGGER.warn("Executor did not terminate after forced shutdown");
              }
          }
      } catch (InterruptedException e) {
          // Current thread was interrupted, force shutdown immediately
          executor.shutdownNow();
          // Preserve interrupt status
          Thread.currentThread().interrupt();
      }
  }
}
