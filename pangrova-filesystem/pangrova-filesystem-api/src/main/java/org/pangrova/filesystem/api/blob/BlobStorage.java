package org.pangrova.filesystem.api.blob;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import org.pangrova.common.api.annotation.Stable;

/** Content-addressed publication port; authorization and caller staging remain with the caller. */
@Stable(since = "0.1")
public interface BlobStorage extends AutoCloseable {
    /**
     * Publishes the exact immutable content without replacing an existing object. The provider
     * must not retain a mutable alias of the caller's fully written staging file.
     *
     * @throws IOException when publication or integrity verification fails
     */
    void publish(BlobObjectId object, Path stagedContent) throws IOException;

    /** Opens the exact object without enumerating its tenant namespace; the caller owns the stream. */
    InputStream open(BlobObjectId object) throws IOException;

    /** Resource-free providers require no cleanup; others release only resources they own. */
    @Override
    default void close() throws IOException {}
}
