package org.pangrova.storage.api.blob;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.LongConsumer;

import org.pangrova.common.api.annotation.Internal;

/** Backend identity and admitted I/O for durable preparation, independent of any source protocol. */
@Internal
public interface ManagedBlobStorage extends BlobStorage {
    /** Admits each payload request before I/O; hidden payload retries must not reuse its allowance. */
    void publish(BlobObjectId object, Path stagedContent, LongConsumer writeAdmission) throws IOException;

    /** Opens exactly one admitted complete positive-length range, with explicit cancellation. */
    BlobInputStream open(BlobObjectId object, LongConsumer readAdmission) throws IOException;

    /** Verifies an empty object without attempting an empty payload range. */
    void verifyEmptyPublication(BlobObjectId object) throws IOException;

    /** Returns the physical location of this content identity. */
    String objectUri(BlobObjectId object);

    /** Returns the logical namespace selected by process assembly. */
    String rootUri();

    /** Returns a non-secret physical backend fingerprint, rechecked before managed I/O. */
    String backendIdentitySha256();
}
