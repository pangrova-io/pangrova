package org.pangrova.indexing.core.storage;

import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

/**
 * Manages reading and writing serialized index files via {@link FileSystemAccessor}.
 *
 * <p>Data integrity is guaranteed by the CRC32 trailer embedded in the
 * {@link org.pangrova.indexing.core.serde.IndexSerializer} wire format. This class
 * is a thin I/O layer — no redundant checksum computation.</p>
 */
public final class IndexStorageManager {

    private static final Logger LOG = LoggerFactory.getLogger(IndexStorageManager.class);

    /** Default maximum index file size: 50 MB. Files larger than this are skipped. */
    public static final long DEFAULT_MAX_INDEX_FILE_BYTES = 50L * 1024 * 1024;

    /** Buffer size used when reading index files. */
    private static final int READ_BUFFER_SIZE_BYTES = 65536;

    private final long maxIndexFileBytes;

    /** Creates a manager with the default maximum index file size. */
    public IndexStorageManager() {
        this(DEFAULT_MAX_INDEX_FILE_BYTES);
    }

    /** Creates a manager with a custom maximum index file size in bytes. */
    public IndexStorageManager(long maxIndexFileBytes) {
        if (maxIndexFileBytes <= 0) {
            throw new IllegalArgumentException("maxIndexFileBytes must be positive, got " + maxIndexFileBytes);
        }
        this.maxIndexFileBytes = maxIndexFileBytes;
    }

    /**
     * Writes serialized index data to the given path.
     *
     * @param fsAccessor    file system accessor
     * @param indexFilePath path to write the index file
     * @param data          serialized index data (already contains CRC32 trailer)
     * @throws IOException if writing fails
     */
    public void writeIndex(FileSystemAccessor fsAccessor, String indexFilePath, byte[] data)
            throws IOException {
        try (OutputStream out = fsAccessor.openOutputStream(indexFilePath)) {
            out.write(data);
        }
    }

    /**
     * Reads serialized index data from the given path, distinguishing a normal
     * "no index built yet" outcome ({@link IndexFileReadResult.Status#MISSING})
     * from a genuine read failure ({@link IndexFileReadResult.Status#ERROR}).
     *
     * <p>Files exceeding {@code maxIndexFileBytes} are reported as {@code ERROR}
     * to prevent OOM from loading oversized index files into the JVM heap.
     * Callers must count only {@code ERROR} toward the per-index circuit breaker;
     * treating {@code MISSING} as a failure trips the breaker during the normal
     * write → asynchronous-build window (finding F-INDEX-03).</p>
     *
     * @param fsAccessor    file system accessor
     * @param indexFilePath path to read the index file
     * @return present bytes, or a missing/error status
     */
    public IndexFileReadResult readIndexResult(FileSystemAccessor fsAccessor, String indexFilePath) {
        try {
            if (!fsAccessor.exists(indexFilePath)) {
                return IndexFileReadResult.missing();
            }
            long fileSize = fsAccessor.fileSize(indexFilePath);
            if (fileSize > maxIndexFileBytes) {
                LOG.warn("event=index_file_too_large file={} sizeBytes={} maxBytes={} action=skip",
                        indexFilePath, fileSize, maxIndexFileBytes);
                return IndexFileReadResult.error();
            }
            try (InputStream in = new BufferedInputStream(
                    fsAccessor.openInputStream(indexFilePath), READ_BUFFER_SIZE_BYTES)) {
                return IndexFileReadResult.present(in.readAllBytes());
            }
        } catch (IOException e) {
            LOG.warn("event=read_index_failed file={} error={}", indexFilePath, e.getMessage());
            return IndexFileReadResult.error();
        }
    }

    /**
     * Reads serialized index data from the given path.
     *
     * <p>Returns the data bytes, or empty if the file is missing, too large, or
     * failed to read. Callers that must distinguish a missing index from a read
     * error should use {@link #readIndexResult} instead.</p>
     *
     * @param fsAccessor    file system accessor
     * @param indexFilePath path to read the index file
     * @return the data bytes, or empty if file missing, too large, or I/O error
     */
    public Optional<byte[]> readIndex(FileSystemAccessor fsAccessor, String indexFilePath) {
        IndexFileReadResult result = readIndexResult(fsAccessor, indexFilePath);
        return result.isPresent() ? Optional.of(result.bytes()) : Optional.empty();
    }
}
