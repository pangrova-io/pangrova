package org.pangrova.worker.ai.blob;

import java.nio.file.Path;
import java.util.Set;

/** Test fixture for the filesystem implementation of the durable blob contract. */
public final class BlobTestStores {

    private BlobTestStores() {}

    public static GovernedBlobStore create(
            Path root, long maximumBytes, Set<String> mimeTypes) {
        return new GovernedBlobStore(
                new FileSystemBlobStorage(root.resolve("objects")),
                root.resolve("staging"),
                capabilityKey(), maximumBytes, mimeTypes);
    }

    public static byte[] capabilityKey() {
        byte[] key = new byte[32];
        java.util.Arrays.fill(key, (byte) 0x5a);
        return key;
    }
}
