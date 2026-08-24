package org.pangrova.worker.ai.blob;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads blob content from external object storage given a {@link BlobReference}.
 *
 * <p>Implements the read side of the Claim-Check pattern: resolves the URI from
 * the reference, fetches the content from storage, and returns the raw bytes.</p>
 *
 * <p>Thread safety: instances are safe for concurrent use from multiple worker threads,
 * provided the underlying {@link FileSystemAccessor} is thread-safe.</p>
 */
public final class BlobContentReader {

    private static final Logger LOG = LoggerFactory.getLogger(BlobContentReader.class);

    private final FileSystemAccessor storage;

    /**
     * Creates a reader backed by the given storage accessor.
     *
     * @param storage file system accessor for the table's storage backend
     */
    public BlobContentReader(FileSystemAccessor storage) {
        if (storage == null) {
            throw new IllegalArgumentException("storage must not be null");
        }
        this.storage = storage;
    }

    /**
     * Reads the full blob content referenced by the given {@link BlobReference}.
     *
     * <p>Verifies that the actual content size matches the declared size in the reference.
     * Returns the raw bytes for downstream processing (e.g., embedding computation).</p>
     *
     * @param reference blob reference containing the storage URI and metadata
     * @return raw blob content bytes
     * @throws PangrovaException with {@link PangrovaErrorCode#CONNECTOR_READ_ERROR}
     *                         if the read from external storage fails or size mismatch detected
     */
    public byte[] read(BlobReference reference) {
        if (reference == null) {
            throw new IllegalArgumentException("reference must not be null");
        }

        String uri = reference.uri();
        byte[] content = readFromStorage(uri);

        // Size verification: skip when size=0 (deferred resolution from FILE_FROM_URL)
        if (reference.size() > 0 && content.length != reference.size()) {
            throw new PangrovaException(
                    PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "BLOB size mismatch: expected=" + reference.size()
                            + " actual=" + content.length + " uri=" + uri);
        }

        LOG.debug("event=blob_read uri={} size={}", uri, content.length);

        return content;
    }

    /**
     * Opens an input stream for streaming blob content without loading into memory.
     *
     * <p>Callers are responsible for closing the returned stream. Preferred for large
     * blobs where full materialization would exceed memory constraints.</p>
     *
     * @param reference blob reference containing the storage URI and metadata
     * @return input stream for reading blob content (caller must close)
     * @throws PangrovaException with {@link PangrovaErrorCode#CONNECTOR_READ_ERROR}
     *                         if the stream cannot be opened
     */
    public InputStream openStream(BlobReference reference) {
        if (reference == null) {
            throw new IllegalArgumentException("reference must not be null");
        }

        String uri = reference.uri();
        try {
            InputStream stream = storage.openInputStream(uri);
            LOG.debug("event=blob_stream_opened uri={} declaredSize={}", uri, reference.size());
            return stream;
        } catch (IOException e) {
            throw new PangrovaException(
                    PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Failed to open BLOB stream: uri=" + uri + ": " + e.getMessage(),
                    e);
        }
    }

    private byte[] readFromStorage(String uri) {
        try (InputStream in = storage.openInputStream(uri)) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new PangrovaException(
                    PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Failed to read BLOB from external storage: uri=" + uri
                            + ": " + e.getMessage(),
                    e);
        }
    }
}
