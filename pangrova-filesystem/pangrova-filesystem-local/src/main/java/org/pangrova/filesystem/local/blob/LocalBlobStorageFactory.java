package org.pangrova.filesystem.local.blob;

import org.pangrova.filesystem.api.blob.BlobStorage;
import org.pangrova.filesystem.api.blob.BlobStorageFactory;

import java.nio.file.Path;
import java.util.Map;

public final class LocalBlobStorageFactory implements BlobStorageFactory {
    @Override
    public String providerId() {
        return "local";
    }

    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public BlobStorage create(Map<String, String> configuration) {
        String root = configuration.get("file.storage.local.root");
        if (root == null || root.isBlank()) {
            throw new IllegalArgumentException("file.storage.local.root is required");
        }
        return new FileSystemBlobStorage(Path.of(root.trim()));
    }
}
