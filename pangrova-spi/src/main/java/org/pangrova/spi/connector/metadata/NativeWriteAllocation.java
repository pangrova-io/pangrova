package org.pangrova.spi.connector.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Objects;

/** Immutable connector allocation for a Native dataset sink; contains no rows or credentials. */
@Experimental
public record NativeWriteAllocation(
        String datasetUri, Operation operation, long readVersion,
        byte[] schemaFenceSha256, String writeId, String stagingPrefix,
        String generation, List<String> writeColumnNames) {

    private static final int MAX_STAGING_PREFIX_LENGTH = 2048;
    private static final int SHA256_LENGTH = 32;

    /** Dataset transaction operation owned by the connector. */
    public enum Operation { CREATE, APPEND, OVERWRITE }

    /** Validates a stable allocation before it can cross the execution boundary. */
    public NativeWriteAllocation {
        requireText(datasetUri, "datasetUri");
        Objects.requireNonNull(operation, "operation");
        requireVersion(operation, readVersion);
        schemaFenceSha256 = copySchemaFence(schemaFenceSha256);
        requireText(writeId, "writeId");
        requireText(generation, "generation");
        requireStagingPrefix(stagingPrefix);
        writeColumnNames = copyWriteColumns(writeColumnNames);
    }

    private static void requireVersion(Operation operation, long version) {
        if (version < 0 || (operation == Operation.CREATE) != (version == 0)) {
            throw new IllegalArgumentException(
                    "CREATE requires version 0; other operations require a positive version");
        }
    }

    private static byte[] copySchemaFence(byte[] fence) {
        byte[] copy = Objects.requireNonNull(fence, "schemaFenceSha256").clone();
        if (copy.length != SHA256_LENGTH) {
            throw new IllegalArgumentException("schemaFenceSha256 must contain 32 bytes");
        }
        return copy;
    }

    private static void requireStagingPrefix(String stagingPrefix) {
        requireText(stagingPrefix, "stagingPrefix");
        if (stagingPrefix.length() > MAX_STAGING_PREFIX_LENGTH) {
            throw new IllegalArgumentException("stagingPrefix must be a normalized dataset-relative prefix");
        }
        for (String part : stagingPrefix.split("/", -1)) {
            requirePathComponent(part);
        }
    }

    private static void requirePathComponent(String part) {
        if (part.isEmpty() || part.equals(".") || part.equals("..")) {
            throw new IllegalArgumentException("stagingPrefix must be a normalized dataset-relative prefix");
        }
        for (int index = 0; index < part.length(); index++) {
            if (!isPathCharacter(part.charAt(index))) {
                throw new IllegalArgumentException("stagingPrefix must be a normalized dataset-relative prefix");
            }
        }
    }

    private static boolean isPathCharacter(char value) {
        return value >= 'A' && value <= 'Z'
                || value >= 'a' && value <= 'z'
                || value >= '0' && value <= '9'
                || "_.=-".indexOf(value) >= 0;
    }

    private static List<String> copyWriteColumns(List<String> columns) {
        List<String> copy = List.copyOf(columns);
        if (copy.isEmpty() || copy.stream().anyMatch(String::isBlank)
                || copy.stream().distinct().count() != copy.size()) {
            throw new IllegalArgumentException("writeColumnNames must be nonempty, unique and nonblank");
        }
        return copy;
    }

    @Override
    public byte[] schemaFenceSha256() { return schemaFenceSha256.clone(); }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    }
}
