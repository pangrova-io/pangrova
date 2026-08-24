package org.pangrova.connector.iceberg.handle;

import java.util.List;
import java.util.Set;

/**
 * Describes a delete file associated with an Iceberg split.
 *
 * @param filePath             path to the delete file
 * @param deleteType           "POSITION", "EQUALITY", or "DELETION_VECTOR"
 * @param equalityFields       field names for equality deletes (empty for position/DV deletes)
 * @param recordCount          number of delete records in the file
 * @param dataSequenceNumber   data sequence number of the delete file
 * @param dvFilePath           deletion vector file path (only for DELETION_VECTOR type)
 */
public record IcebergDeleteFile(
        String filePath,
        String deleteType,
        List<String> equalityFields,
        long recordCount,
        long dataSequenceNumber,
        String dvFilePath) {

    private static final Set<String> VALID_DELETE_TYPES =
            Set.of("POSITION", "EQUALITY", "DELETION_VECTOR");

    public IcebergDeleteFile {
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException("filePath must not be null or blank");
        }
        if (deleteType == null || deleteType.isBlank()) {
            throw new IllegalArgumentException("deleteType must not be null or blank");
        }
        if (!VALID_DELETE_TYPES.contains(deleteType)) {
            throw new IllegalArgumentException(
                    "deleteType must be one of " + VALID_DELETE_TYPES + ", got: " + deleteType);
        }
        equalityFields = equalityFields == null ? List.of() : List.copyOf(equalityFields);
        dvFilePath = dvFilePath == null ? "" : dvFilePath;
    }

    /**
     * Backward-compatible constructor for callers that do not supply dvFilePath.
     */
    public IcebergDeleteFile(
            String filePath,
            String deleteType,
            List<String> equalityFields,
            long recordCount,
            long dataSequenceNumber) {
        this(filePath, deleteType, equalityFields, recordCount, dataSequenceNumber, "");
    }
}
