package org.pangrova.connector.spi.index;

import org.pangrova.common.api.annotation.Experimental;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * SPI contract for accessing the underlying file system of a connector.
 *
 * <p>Implementations bridge the indexing module to connector-specific storage
 * (e.g. HDFS, S3, local FS). Each method operates on absolute file paths
 * within the connector's storage namespace.
 */
@Experimental
public interface FileSystemAccessor extends AutoCloseable {

    /**
     * Opens an input stream for reading the specified file.
     *
     * @param path absolute file path
     * @return input stream (caller must close)
     * @throws IOException if the file cannot be opened
     */
    InputStream openInputStream(String path) throws IOException;

    /**
     * Opens an output stream for writing to the specified file.
     * Creates parent directories if they do not exist.
     *
     * @param path absolute file path
     * @return output stream (caller must close)
     * @throws IOException if the file cannot be created or opened
     */
    OutputStream openOutputStream(String path) throws IOException;

    /**
     * Deletes the specified file.
     *
     * @param path absolute file path
     * @throws IOException if deletion fails
     */
    void deleteFile(String path) throws IOException;

    /**
     * Checks whether the specified file exists.
     *
     * @param path absolute file path
     * @return true if the file exists
     * @throws IOException if the check fails
     */
    boolean exists(String path) throws IOException;

    /**
     * Returns the size of the specified file in bytes.
     *
     * @param path absolute file path
     * @return file size in bytes
     * @throws IOException if the file does not exist or size cannot be determined
     */
    long fileSize(String path) throws IOException;

    /**
     * Lists all files under the specified directory path.
     *
     * @param directoryPath absolute directory path
     * @return list of absolute file paths (non-recursive)
     * @throws IOException if the directory cannot be listed
     */
    List<String> listFiles(String directoryPath) throws IOException;

    // ---- Multimodal FILE type extensions (default methods for backward compatibility) ----

    /**
     * Opens a multipart output stream for writing large files (>64MB).
     * Implementations should use S3 multipart upload or equivalent.
     * Default falls back to single-part {@link #openOutputStream(String)}.
     *
     * @param path absolute file path
     * @param partSizeBytes suggested part size in bytes
     * @return output stream that handles multipart upload internally
     * @throws IOException if the stream cannot be opened
     */
    default OutputStream openMultipartOutputStream(String path, long partSizeBytes) throws IOException {
        return openOutputStream(path);
    }

    /**
     * Copies an object from one path to another within the same storage backend.
     * Implementations should use server-side copy when available (e.g. S3 CopyObject).
     * Default falls back to streaming copy via input/output streams.
     *
     * @param sourcePath source file path
     * @param destinationPath destination file path
     * @throws IOException if the copy fails
     */
    default void copyObject(String sourcePath, String destinationPath) throws IOException {
        try (InputStream in = openInputStream(sourcePath);
                OutputStream out = openOutputStream(destinationPath)) {
            in.transferTo(out);
        }
    }

    /**
     * Generates a presigned URL for temporary direct access to the specified file.
     * Used for passing file references to external AI services.
     * Default returns empty (presigning not supported).
     *
     * @param path absolute file path
     * @param expiration duration for which the URL remains valid
     * @return presigned URL if supported, empty otherwise
     * @throws IOException if URL generation fails
     */
    default Optional<URI> generatePresignedUrl(String path, Duration expiration) throws IOException {
        return Optional.empty();
    }

    /**
     * Returns the content type (MIME type) of the specified file based on
     * storage metadata or file extension heuristics.
     * Default returns empty (content type detection not supported).
     *
     * @param path absolute file path
     * @return detected MIME type if available
     * @throws IOException if metadata cannot be read
     */
    default Optional<String> getContentType(String path) throws IOException {
        return Optional.empty();
    }

    /**
     * Computes or retrieves the checksum (e.g. MD5 or SHA-256) of the specified file.
     * Used for content-addressable deduplication in FILE storage.
     * Default returns empty (checksum not available).
     *
     * @param path absolute file path
     * @return hex-encoded checksum string if available
     * @throws IOException if checksum computation fails
     */
    default Optional<String> getChecksum(String path) throws IOException {
        return Optional.empty();
    }
}
