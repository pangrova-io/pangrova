package org.pangrova.testing.integration.e2e.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.pangrova.ai.worker.blob.TenantCapabilitySigner;
import org.pangrova.worker.jni.NativeRuntime;

/** Real persistent filesystem namespace selected by production FILE factory configuration. */
public final class LocalFileDeployment implements FileStorageDeployment {
    private Path directory;

    @Override
    public Map<String, String> prepareConfiguredDeployment() throws IOException {
        if (NativeRuntime.isInitialized()) {
            throw new IllegalStateException("LOCAL FILE deployment must precede shared Native startup");
        }
        String encoded = System.getenv(CAPABILITY_KEY_ENV);
        if (encoded == null || encoded.isBlank()) {
            throw new IllegalStateException("LOCAL FILE SQL requires " + CAPABILITY_KEY_ENV);
        }
        byte[] key = TenantCapabilitySigner.decodeBase64Secret(encoded);
        Arrays.fill(key, (byte) 0);
        if (directory == null) directory = Files.createTempDirectory("pangrova-local-file-sql-");
        return Map.of("file.storage-backend", "local",
                "file.storage.local.root", objectRoot().toString(),
                "file.storage-staging-root", directory.resolve("staging").toString(),
                "file.capability-secret-reference", "env://" + CAPABILITY_KEY_ENV);
    }

    /** Returns the actual namespace passed to the production provider. */
    public Path objectRoot() {
        if (directory == null) throw new IllegalStateException("LOCAL FILE deployment is not prepared");
        return directory.resolve("objects");
    }

    /** Requires that the production factory, rather than the oracle, created the object root. */
    public void requireConfiguredDeployment() {
        if (!Files.isDirectory(objectRoot())) {
            throw new IllegalStateException("The production LOCAL FILE factory has not opened its namespace");
        }
    }

    /** Lists actual published files; private incomplete copies are not published identities. */
    public List<Path> publishedObjects() throws IOException {
        requireConfiguredDeployment();
        try (var files = Files.walk(objectRoot())) {
            return files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".blob")).sorted().toList();
        }
    }

    @Override
    public void close() throws IOException {
        if (NativeRuntime.isInitialized()) {
            throw new IllegalStateException("Native callback drain must precede LOCAL FILE cleanup");
        }
        if (directory == null) return;
        try (var files = Files.walk(directory)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(file);
        }
        directory = null;
    }
}
