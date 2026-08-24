package org.pangrova.worker.ai.blob;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Writes blob content to external object storage and returns a {@link BlobReference}.
 *
 * <p>Implements the write side of the Claim-Check pattern: accepts raw bytes,
 * computes a SHA-256 checksum, enforces maximum size, persists to the configured
 * storage path, and returns an immutable reference for storage in the data file.</p>
 *
 * <p>Storage path convention: {@code {tableLocation}/blob/{column}/{uuid}.{ext}}</p>
 *
 * <p>Thread safety: instances are safe for concurrent use from multiple worker threads,
 * provided the underlying {@link FileSystemAccessor} is thread-safe.</p>
 */
public final class BlobExternalWriter {

    private static final Logger LOG = LoggerFactory.getLogger(BlobExternalWriter.class);

    /** Default maximum blob size: 5 GB. */
    static final long DEFAULT_MAX_BLOB_SIZE = 5L * 1024 * 1024 * 1024;

    private static final String BLOB_PATH_SEGMENT = "/blob/";

    private final FileSystemAccessor storage;
    private final long maxBlobSize;

    /**
     * Creates a writer with the default maximum blob size (5 GB).
     *
     * @param storage file system accessor for the table's storage backend
     */
    public BlobExternalWriter(FileSystemAccessor storage) {
        this(storage, DEFAULT_MAX_BLOB_SIZE);
    }

    /**
     * Creates a writer with a custom maximum blob size.
     *
     * @param storage     file system accessor for the table's storage backend
     * @param maxBlobSize maximum allowed blob size in bytes (must be positive)
     */
    public BlobExternalWriter(FileSystemAccessor storage, long maxBlobSize) {
        if (storage == null) {
            throw new IllegalArgumentException("storage must not be null");
        }
        if (maxBlobSize <= 0) {
            throw new IllegalArgumentException("maxBlobSize must be positive, got: " + maxBlobSize);
        }
        this.storage = storage;
        this.maxBlobSize = maxBlobSize;
    }

    /**
     * Writes blob content to external storage and returns a reference.
     *
     * <p>Computes SHA-256 checksum during write. Enforces maximum size before
     * writing. The blob is stored at {@code {tableLocation}/blob/{column}/{uuid}.{ext}}.</p>
     *
     * @param tableLocation base location URI of the table
     * @param column        column name for path partitioning
     * @param content       raw blob bytes (must not be null or empty)
     * @param mimeType      MIME type of the content (e.g., "image/jpeg")
     * @return immutable blob reference for storage in the data file
     * @throws PangrovaException with {@link PangrovaErrorCode#AI_BLOB_SIZE_EXCEEDED}
     *                         if content exceeds the configured maximum size
     * @throws PangrovaException with {@link PangrovaErrorCode#CONNECTOR_WRITE_ERROR}
     *                         if the write to external storage fails
     */
    public BlobReference write(String tableLocation, String column, byte[] content, String mimeType) {
        validatePathArguments(tableLocation, column);
        validateContent(content);
        validateMimeType(mimeType);

        long size = content.length;
        if (size > maxBlobSize) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_BLOB_SIZE_EXCEEDED,
                    "BLOB size " + size + " bytes exceeds maximum " + maxBlobSize + " bytes"
                            + " for column=" + column);
        }

        String checksum = computeSha256(content);
        String extension = resolveExtension(mimeType);
        String blobId = UUID.randomUUID().toString();
        String blobPath = buildBlobPath(tableLocation, column, blobId, extension);

        writeToStorage(blobPath, content, column);

        LOG.debug("event=blob_written column={} size={} path={}", column, size, blobPath);

        return new BlobReference(blobPath, mimeType, size, checksum);
    }

    private static void validatePathArguments(String tableLocation, String column) {
        if (tableLocation == null || tableLocation.isBlank()) {
            throw new IllegalArgumentException("tableLocation must not be blank");
        }
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
    }

    private static void validateMimeType(String mimeType) {
        if (mimeType == null || mimeType.isBlank()) {
            throw new IllegalArgumentException("mimeType must not be blank");
        }
    }

    private static void validateContent(byte[] content) {
        if (content == null || content.length == 0) {
            throw new IllegalArgumentException("content must not be null or empty");
        }
    }

    private String buildBlobPath(String tableLocation, String column, String blobId, String extension) {
        String base = tableLocation.endsWith("/")
                ? tableLocation.substring(0, tableLocation.length() - 1)
                : tableLocation;
        return base + BLOB_PATH_SEGMENT + column + "/" + blobId + "." + extension;
    }

    private void writeToStorage(String path, byte[] content, String column) {
        try (OutputStream out = storage.openOutputStream(path)) {
            out.write(content);
        } catch (IOException e) {
            throw new PangrovaException(
                    PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to write BLOB to external storage: path=" + path
                            + " column=" + column + ": " + e.getMessage(),
                    e);
        }
    }

    static String computeSha256(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    static String resolveExtension(String mimeType) {
        int slashIndex = mimeType.indexOf('/');
        if (slashIndex < 0 || slashIndex == mimeType.length() - 1) {
            return "bin";
        }
        String subtype = mimeType.substring(slashIndex + 1);
        return switch (subtype) {
            case "jpeg" -> "jpg";
            case "plain" -> "txt";
            case "svg+xml" -> "svg";
            case "x-wav" -> "wav";
            case "mpeg" -> "mp3";
            default -> subtype;
        };
    }
}
