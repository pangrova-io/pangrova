package org.pangrova.filesystem.api.blob;

import org.pangrova.common.api.annotation.Experimental;

import java.util.HashMap;
import java.util.Map;

/** Creates one governed FILE backend for a Worker. Discovery must not allocate backend resources. */
@Experimental
public interface BlobStorageFactory {
    int API_VERSION = 1;

    /** Stable deployment ID selected by the host's storage-backend configuration. */
    String providerId();

    int apiVersion();

    /** Looks up a compatible factory in the installed inventory before any backend client is opened. */
    static BlobStorageFactory select(Map<String, String> configuration, Iterable<BlobStorageFactory> factories) {
        String selected = configuration.get("file.storage-backend");
        if (selected == null || selected.isBlank()) {
            throw new IllegalArgumentException("file.storage-backend is required");
        }
        var installed = new HashMap<String, BlobStorageFactory>();
        for (var factory : factories) {
            String id = factory.providerId();
            if (id == null || id.isBlank() || factory.apiVersion() != API_VERSION) {
                throw new IllegalArgumentException("Invalid FILE storage factory: " + id);
            }
            if (installed.putIfAbsent(id, factory) != null) {
                throw new IllegalArgumentException("Duplicate FILE storage backend: " + id);
            }
        }
        var factory = installed.get(selected.trim());
        if (factory == null) {
            throw new IllegalArgumentException("Unsupported FILE storage backend: " + selected.trim());
        }
        return factory;
    }

    /** Validates the selected backend's settings and creates an instance owned by the caller. */
    BlobStorage create(Map<String, String> configuration);
}
