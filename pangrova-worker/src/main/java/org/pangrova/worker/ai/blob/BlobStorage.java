package org.pangrova.worker.ai.blob;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

/** Durable content-addressed storage used by the governed FILE layer. */
public interface BlobStorage extends AutoCloseable {

    /**
     * Atomically publishes an immutable object if it is not already present.
     *
     * @param tenantScope opaque tenant scope
     * @param checksum SHA-256 content identity
     * @param size exact content length
     * @param stagedContent fully written local staging file
     * @throws IOException when atomic publication or integrity verification fails
     */
    void publish(String tenantScope, String checksum, long size, Path stagedContent)
            throws IOException;

    /** Opens the exact object without enumerating the tenant namespace. */
    InputStream open(String tenantScope, String checksum, long size) throws IOException;

    @Override
    default void close() throws Exception {}
}
