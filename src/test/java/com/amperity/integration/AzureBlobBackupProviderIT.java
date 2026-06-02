package com.amperity.integration;

import com.rpl.rama.backup.BackupProvider;
import com.rpl.rama.backup.BackupProviderTester;
import com.amperity.rama.backup.AzureBlobBackupProvider;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AzureBlobBackupProviderIT {

  private static void assertEquals(Object expected, Object val) {
    if (expected == null && val != null || expected != null && !expected.equals(val)) {
      throw new RuntimeException("assertEquals failure: expected " + expected + ", actual " + val);
    }
  }

  private static void testing(String s) {
    System.err.println(s);
  }

  public static String zeroPad(int num, int length) {
    return String.format("%0" + length + "d", num);
  }

  /**
   * Gets Azure storage configuration with unique test path per run.
   * Uses timestamp to ensure test isolation and prevent interference from previous runs.
   * Returns format: <storage-account>:<container>/test-<timestamp>
   */
  private static String getAzureLocation() {
    String storageAccount = System.getenv("AZURE_STORAGE_ACCOUNT");
    String container = System.getenv("AZURE_CONTAINER");

    if (storageAccount == null || container == null) {
      System.err.println("WARNING: AZURE_STORAGE_ACCOUNT and AZURE_CONTAINER not set, using test defaults");
      storageAccount = "amperityaztest";
      container = "test";
    }

    // Generate unique path per test run to avoid interference
    String uniquePath = "test-" + System.currentTimeMillis();
    return storageAccount + ":" + container + "/" + uniquePath;
  }

  /**
   * Recursively deletes all files in the provider's root path for cleanup.
   * Logs any errors during cleanup for debugging.
   */
  private static void cleanupTestData(BackupProvider provider) {
    try {
      BackupProvider.KeysPage page = provider.listKeysRecursive("", null).get();
      while (page != null && !page.keys.isEmpty()) {
        for (String key : page.keys) {
          try {
            provider.deleteObject(key).get();
          } catch (Exception e) {
            System.err.println("Failed to delete key '" + key + "' during cleanup:");
            e.printStackTrace();
          }
        }
        if (page.nextPageMarker != null) {
          page = provider.listKeysRecursive("", page.nextPageMarker).get();
        } else {
          break;
        }
      }
    } catch (Exception e) {
      System.err.println("Cleanup failed during listing:");
      e.printStackTrace();
    }
  }

  /**
   * Cleans up both Azure storage and local temporary directory.
   * Handles null provider and directory gracefully.
   */
  private static void cleanupResources(BackupProvider provider, java.nio.file.Path dir) {
    // Cleanup Azure test data
    if (provider != null) {
      cleanupTestData(provider);
    }
    // Cleanup local temp directory
    if (dir != null) {
      try (Stream<java.nio.file.Path> pathStream = Files.walk(dir)) {
        pathStream
            .sorted(Comparator.reverseOrder())
            .map(java.nio.file.Path::toFile)
            .forEach(File::delete);
      } catch (Exception e) {
        System.err.println("Local cleanup failed for directory " + dir + ":");
        e.printStackTrace();
      }
    }
  }

  public void testAzureBlobProvider() throws Exception {
    final String k = "a/b/c";
    final java.nio.file.Path dir = Files.createTempDirectory("testAzureBlobProvider");

    BackupProvider provider = null;
    try {
      testing("An Azure Blob provider");

      provider = new AzureBlobBackupProvider(getAzureLocation());

      testing("  when empty");

      testing("    lists no keys,");
      {
        BackupProvider.KeysPage page = provider.listKeysNonRecursive("", null, 1000).get();
        assertEquals(Collections.emptyList(), page.keys);
        assertEquals(null, page.nextPageMarker);
      }
      {
        BackupProvider.KeysPage page = provider.listKeysRecursive("", null).get();
        assertEquals(Collections.emptyList(), page.keys);
        assertEquals(null, page.nextPageMarker);
      }

      testing("    returns false for hasKey on a non-existing key");
      assertEquals(false, provider.hasKey(k).get());

      testing("  when a key is added,");
      provider.putObject(k, new ByteArrayInputStream("abc".getBytes()), 3L).get();

      testing("    returns true for hasKey on the added key");
      assertEquals(true, provider.hasKey(k).get());

      testing("    getObject returns an InputStream for the contents");
      {
        InputStream inputStream = provider.getObject(k).get();
        String text =
            new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        assertEquals("abc", text);
      }
      testing("    lists the added key");
      {
        BackupProvider.KeysPage page = provider.listKeysNonRecursive("a/b/", null, 1000).get();
        assertEquals(Arrays.asList("c"), page.keys);
        assertEquals(null, page.nextPageMarker);
      }
      {
        BackupProvider.KeysPage page = provider.listKeysRecursive("", null).get();
        assertEquals(Arrays.asList("a/b/c"), page.keys);
        assertEquals(null, page.nextPageMarker);
      }
      testing("    lists the root directory elements");
      {
        BackupProvider.KeysPage page = provider.listKeysNonRecursive("a/", null, 1000).get();
        assertEquals(Arrays.asList("b"), page.keys);
        assertEquals(null, page.nextPageMarker);
      }
      {
        BackupProvider.KeysPage page = provider.listKeysRecursive("a/", null).get();
        assertEquals(Arrays.asList("a/b/c"), page.keys);
        assertEquals(null, page.nextPageMarker);
      }
      {
        BackupProvider.KeysPage page = provider.listKeysRecursive("a", null).get();
        assertEquals(Arrays.asList("a/b/c"), page.keys);
        assertEquals(null, page.nextPageMarker);
      }
      {
        BackupProvider.KeysPage page = provider.listKeysRecursive("a/b", null).get();
        assertEquals(Arrays.asList("a/b/c"), page.keys);
        assertEquals(null, page.nextPageMarker);
      }
      {
        BackupProvider.KeysPage page = provider.listKeysRecursive("a/b/", null).get();
        assertEquals(Arrays.asList("a/b/c"), page.keys);
        assertEquals(null, page.nextPageMarker);
      }
      testing("  paginated list keys");
      {
            byte[] content = "abc".getBytes();
            provider.putObject("x/k2", new ByteArrayInputStream(content), 3L).get();
            provider.putObject("x/k1", new ByteArrayInputStream(content), 3L).get();
            provider.putObject("x/k5", new ByteArrayInputStream(content), 3L).get();
            provider.putObject("x/k3", new ByteArrayInputStream(content), 3L).get();
            provider.putObject("x/k4", new ByteArrayInputStream(content), 3L).get();
            BackupProvider.KeysPage page = provider.listKeysNonRecursive("x/", null, 2).get();
            assertEquals(Arrays.asList("k1", "k2"), page.keys);
            page = provider.listKeysNonRecursive("x/", page.nextPageMarker, 2).get();
            assertEquals(Arrays.asList("k3", "k4"), page.keys);
            page = provider.listKeysNonRecursive("x/", page.nextPageMarker, 2).get();
            assertEquals(Arrays.asList("k5"), page.keys);
            assertEquals(null, page.nextPageMarker);
      }
      testing("  large number of keys");
      {
            byte[] content = "abc".getBytes();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for(int i=0; i<1100; i++) {
              futures.add(provider.putObject("z/" + zeroPad(i, 4), new ByteArrayInputStream(content), 3L));
            }
            testing("  beginning derefs");
            futures.forEach(f -> {
                try {
                    f.get();
                } catch (Exception e) {
                    throw new RuntimeException("Failed", e);
                }
            });
            testing("  end derefs");
            BackupProvider.KeysPage page = provider.listKeysNonRecursive("z/", null, -1).get();
            List expected = new ArrayList();
            for(int i=0; i<1000; i++) expected.add(zeroPad(i, 4));
            assertEquals(expected, page.keys);
            page = provider.listKeysNonRecursive("z/", page.nextPageMarker, -1).get();
            expected = new ArrayList();
            for(int i=1000; i<1100; i++) expected.add(zeroPad(i, 4));
            assertEquals(expected, page.keys);
            assertEquals(null, page.nextPageMarker);

            page = provider.listKeysRecursive("z/", null).get();
            expected = new ArrayList();
            for(int i=0; i<1000; i++) expected.add("z/" + zeroPad(i, 4));
            assertEquals(expected, page.keys);
            page = provider.listKeysRecursive("z/", page.nextPageMarker).get();
            expected = new ArrayList();
            for(int i=1000; i<1100; i++) expected.add("z/" + zeroPad(i, 4));
            assertEquals(expected, page.keys);
            assertEquals(null, page.nextPageMarker);
      }
      System.err.println("done");
    } finally {
      cleanupResources(provider, dir);
    }
  }

  public void testAzureBlobProviderTester() throws Exception {
    final java.nio.file.Path dir = Files.createTempDirectory("testAzureBlobProvider");
    BackupProvider provider = null;

    try {
      testing("An Azure Blob provider");

      provider = new AzureBlobBackupProvider(getAzureLocation());

      BackupProviderTester.testProvider(provider);

    } finally {
      cleanupResources(provider, dir);
    }
  }
}
