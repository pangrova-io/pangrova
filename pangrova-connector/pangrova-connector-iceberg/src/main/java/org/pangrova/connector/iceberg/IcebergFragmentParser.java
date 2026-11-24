package org.pangrova.connector.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses and deserializes worker-written commit fragments for the Iceberg
 * distributed write path. Fragments are JSON payloads: a JSON array denotes
 * APPEND DataFiles, a JSON object carries an {@code operation} discriminator
 * (DELETE, MERGE, or OVERWRITE) with {@code dataFiles} / {@code deleteFiles}
 * arrays.
 */
final class IcebergFragmentParser {
    static final String CDC_MERGE = "CDC_MERGE";

    private static final ObjectMapper SHARED_MAPPER = new ObjectMapper(
            JsonFactory.builder().disable(JsonFactory.Feature.CHARSET_DETECTION).build());

    private IcebergFragmentParser() {}

    /**
     * Collects DataFiles and DeleteFiles from all fragments into a single
     * aggregation, validating that all fragments share the same operation type.
     */
    static IcebergCollectedFragments collectAllFragments(Table icebergTable,
                                                         IcebergOutputTableHandle handle,
                                                         List<byte[]> fragments) {
        List<DataFile> allDataFiles = new ArrayList<>();
        List<DeleteFile> allDeleteFiles = new ArrayList<>();
        String resolvedOperation = null;

        for (byte[] fragment : fragments) {
            if (fragment == null || fragment.length == 0) {
                continue;
            }
            JsonNode root = parseFragment(fragment, handle);
            String operation = classifyFragment(root);
            resolvedOperation = mergeOperationType(resolvedOperation, operation, handle);
            collectFilesFromFragment(icebergTable, handle, root, operation,
                    allDataFiles, allDeleteFiles);
        }
        return new IcebergCollectedFragments(
                resolvedOperation, List.copyOf(allDataFiles), List.copyOf(allDeleteFiles));
    }

    private static JsonNode parseFragment(byte[] fragment, IcebergOutputTableHandle handle) {
        if (fragment[0] != '[' && fragment[0] != '{') {
            throw new IllegalStateException(
                    "Unrecognized fragment format for table %s.%s: expected JSON array or object"
                            .formatted(handle.schemaName(), handle.tableName()));
        }
        try {
            return SHARED_MAPPER.readTree(fragment);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to parse fragment JSON for table %s.%s: %s"
                            .formatted(handle.schemaName(), handle.tableName(),
                                    e.getMessage()),
                    e);
        }
    }

    private static String classifyFragment(JsonNode root) {
        if (root.isArray()) {
            return "APPEND";
        }
        return switch (root.path("operation").asText()) {
            case "DELETE" -> "DELETE";
            case "MERGE" -> "MERGE";
            case CDC_MERGE -> CDC_MERGE;
            default -> "OVERWRITE";
        };
    }

    /**
     * Validates that all fragments share the same operation type.
     * Mixed operations within a single distributed write are not supported.
     */
    private static String mergeOperationType(String current, String incoming,
                                             IcebergOutputTableHandle handle) {
        if (current == null) {
            return incoming;
        }
        if (!current.equals(incoming)) {
            throw new IllegalStateException(
                    "Mixed operation types in distributed write for table %s.%s: %s vs %s"
                            .formatted(handle.schemaName(), handle.tableName(),
                                    current, incoming));
        }
        return current;
    }

    /**
     * Deserializes DataFiles and DeleteFiles from a single fragment and appends
     * them to the provided accumulator lists.
     */
    private static void collectFilesFromFragment(
            Table icebergTable, IcebergOutputTableHandle handle,
            JsonNode root, String operation,
            List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
        switch (operation) {
            case "APPEND" -> collectDataFiles(icebergTable.spec(), root, dataFiles);
            case "OVERWRITE" -> collectDataFiles(icebergTable.spec(), root.path("dataFiles"), dataFiles);
            case "DELETE" -> collectDeleteFiles(icebergTable, handle, root.path("deleteFiles"), deleteFiles);
            case "MERGE" -> {
                collectDataFiles(icebergTable.spec(), root.path("dataFiles"), dataFiles);
                collectDeleteFiles(icebergTable, handle, root.path("deleteFiles"), deleteFiles);
            }
            case CDC_MERGE -> collectCdcFiles(icebergTable.spec(), root, dataFiles, deleteFiles);
            default -> throw new IllegalStateException("Unknown operation: " + operation);
        }
    }

    private static void collectCdcFiles(PartitionSpec spec, JsonNode root,
                                        List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
        JsonNode data = root.path("dataFiles");
        JsonNode deletes = root.path("deleteFiles");
        if (!data.isArray() || !deletes.isArray()) {
            throw new IllegalArgumentException("Iceberg CDC fragment requires exact data and delete file lists");
        }
        collectDataFiles(spec, data, dataFiles);
        for (JsonNode element : deletes) {
            var file = (DeleteFile) ContentFileParser.fromJson(element, spec);
            if (file.content() != FileContent.POSITION_DELETES || file.format() != FileFormat.PARQUET) {
                throw new IllegalArgumentException("Iceberg CDC requires Parquet position delete files");
            }
            deleteFiles.add(file);
        }
    }

    /**
     * Deserializes DeleteFile descriptors from the {@code "deleteFiles"} JSON array
     * within a DELETE or MERGE fragment using Jackson for correct un-escaping.
     *
     * @throws IllegalStateException if a DeleteFile entry cannot be deserialized,
     *         including the file path and error details in the message
     */
    private static void collectDeleteFiles(
            Table table, IcebergOutputTableHandle handle, JsonNode array, List<DeleteFile> files) {
        if (!array.isArray()) {
            return;
        }
        for (JsonNode element : array) {
            files.add(parseDeleteFileEntry(table, handle, element));
        }
    }

    private static DeleteFile parseDeleteFileEntry(
            Table table, IcebergOutputTableHandle handle, JsonNode node) {
        String path = node.has("path") ? node.get("path").asText() : "";
        String format = node.has("format") ? node.get("format").asText() : "PARQUET";
        long recordCount = node.has("recordCount") ? node.get("recordCount").asLong() : 0L;
        long fileSizeInBytes = node.has("fileSizeInBytes") ? node.get("fileSizeInBytes").asLong() : 0L;
        String contentType = node.has("content") ? node.get("content").asText() : "POSITION_DELETES";
        try {
            FileFormat fileFormat = FileFormat.fromString(format);
            var builder = org.apache.iceberg.FileMetadata.deleteFileBuilder(table.spec());
            if ("EQUALITY_DELETES".equals(contentType)) {
                builder.ofEqualityDeletes();
            } else {
                builder.ofPositionDeletes();
            }
            return builder
                    .withPath(path)
                    .withFormat(fileFormat)
                    .withRecordCount(recordCount)
                    .withFileSizeInBytes(fileSizeInBytes)
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to deserialize DeleteFile for table %s.%s, path=%s: %s"
                            .formatted(handle.schemaName(), handle.tableName(), path,
                                    e.getMessage()),
                    e);
        }
    }

    /**
     * Deserializes DataFile descriptors from a JSON array using Iceberg's
     * {@link ContentFileParser} for correct handling of escaped characters
     * and full round-trip fidelity with the serialization side.
     */
    private static void collectDataFiles(PartitionSpec spec, JsonNode array, List<DataFile> files) {
        if (!array.isArray()) {
            return;
        }
        try {
            for (JsonNode element : array) {
                files.add((DataFile) ContentFileParser.fromJson(element, spec));
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to deserialize DataFile from JSON: " + e.getMessage(), e);
        }
    }
}
