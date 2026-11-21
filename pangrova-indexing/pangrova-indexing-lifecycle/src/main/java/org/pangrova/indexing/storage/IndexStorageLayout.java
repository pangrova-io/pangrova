package org.pangrova.indexing.storage;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;

import java.util.Objects;
import java.util.UUID;

/**
 * Computes deterministic file paths for index data files.
 *
 * <p>Pattern:
 * {@code {baseIndexPath}/v3/{catalog}/{schema}/{table}/{indexId}/g{generation}/{attemptId}/{dataFileHash}.idx}
 * where {@code dataFileHash} is the first 32 hex characters (128 bits) of the SHA-256 hash
 * of the data file path. 128 bits provides collision resistance up to ~2^64 files
 * (birthday bound), far exceeding practical table sizes.</p>
 */
public final class IndexStorageLayout {

    private static final int HASH_PREFIX_LENGTH = 32;
    private static final String INDEX_FILE_EXTENSION = ".idx";

    /** Storage layout version. Embedded in the path to allow future path scheme migration. */
    static final int LAYOUT_VERSION = 3;

    private final String baseIndexPath;

    /** Creates the layout rooted at the given base index path. */
    public IndexStorageLayout(String baseIndexPath) {
        if (baseIndexPath == null || baseIndexPath.isBlank()) {
            throw new IllegalArgumentException("baseIndexPath must not be blank");
        }
        this.baseIndexPath = stripTrailingSlash(baseIndexPath);
    }

    /**
     * Computes the index file path for the given descriptor and data file.
     *
     * @param descriptor   the index descriptor
     * @param attemptId    identity of the build attempt writing the file
     * @param dataFilePath the data file path to hash
     * @return the deterministic index file path
     */
    public String indexFilePath(IndexDescriptor descriptor, UUID attemptId, String dataFilePath) {
        String hash = sha256Prefix(dataFilePath);
        return baseIndexPath + "/v" + LAYOUT_VERSION + "/"
                + sanitizePathComponent(descriptor.catalogName()) + "/"
                + sanitizePathComponent(descriptor.schemaName()) + "/"
                + sanitizePathComponent(descriptor.tableName()) + "/"
                + sanitizePathComponent(descriptor.indexId()) + "/"
                + "g" + descriptor.generation() + "/"
                + Objects.requireNonNull(attemptId, "attemptId") + "/" + hash + INDEX_FILE_EXTENSION;
    }

    static String sanitizePathComponent(String component) {
        if (component == null || component.isEmpty()) {
            throw new IllegalArgumentException("Path component must not be null or empty");
        }
        if (component.contains("..") || component.contains("/") || component.contains("\\")
                || component.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "Path component contains illegal characters: " + component);
        }
        return component;
    }

    static String sha256Prefix(String input) {
        return DigestUtils.sha256Hex(input)
                .substring(0, HASH_PREFIX_LENGTH);
    }

    private static String stripTrailingSlash(String path) {
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }
}
