package org.pangrova.indexing.storage;

import org.pangrova.common.crypto.Sha256Digests;
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
    private static final int RETIRED_PATH_COMPONENT_COUNT = 3;

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
        return indexScopePrefix(descriptor) + "g" + descriptor.generation() + "/"
                + Objects.requireNonNull(attemptId, "attemptId") + "/" + hash + INDEX_FILE_EXTENSION;
    }

    /**
     * Verifies the original physical generation of a retired mapping, including paths copied forward
     * into newer generations. Unsupported legacy layouts or changed storage roots fail closed.
     */
    public boolean isRetiredIndexFilePath(IndexDescriptor descriptor, String dataFilePath,
                                          int lastRetiredGeneration, String artifactPath) {
        String prefix = indexScopePrefix(descriptor);
        if (!artifactPath.startsWith(prefix)) return false;
        String[] parts = artifactPath.substring(prefix.length()).split("/", -1);
        if (parts.length != RETIRED_PATH_COMPONENT_COUNT || !parts[0].startsWith("g")) return false;
        try {
            int physicalGeneration = Integer.parseInt(parts[0].substring(1));
            UUID attempt = UUID.fromString(parts[1]);
            return physicalGeneration >= 0 && physicalGeneration <= lastRetiredGeneration
                    && indexFilePath(descriptor.withGeneration(physicalGeneration, descriptor.updatedAt()),
                            attempt, dataFilePath).equals(artifactPath);
        } catch (IllegalArgumentException invalidPath) {
            return false;
        }
    }

    private String indexScopePrefix(IndexDescriptor descriptor) {
        return baseIndexPath + "/v" + LAYOUT_VERSION + "/"
                + sanitizePathComponent(descriptor.catalogName()) + "/"
                + sanitizePathComponent(descriptor.schemaName()) + "/"
                + sanitizePathComponent(descriptor.tableName()) + "/"
                + sanitizePathComponent(descriptor.indexId()) + "/";
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
        return Sha256Digests.sha256Hex(input)
                .substring(0, HASH_PREFIX_LENGTH);
    }

    private static String stripTrailingSlash(String path) {
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }
}
