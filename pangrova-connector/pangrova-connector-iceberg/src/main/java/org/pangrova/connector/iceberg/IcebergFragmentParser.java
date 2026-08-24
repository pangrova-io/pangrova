package org.pangrova.connector.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

import java.nio.charset.StandardCharsets;
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

    /**
     * F-CONNECTOR-06: Shared {@link ObjectMapper} for all fragment JSON parsing.
     * Each {@code new ObjectMapper()} initializes the full Jackson module system,
     * serializer cache, and introspection infrastructure — a 4-way per-call
     * allocation in {@code finishDistributedWrite} could allocate tens of
     * thousands of mappers per commit. {@code ObjectMapper} is documented as
     * thread-safe for read-only configurations.
     */
    private static final ObjectMapper SHARED_MAPPER = new ObjectMapper();

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
            String json = new String(fragment, StandardCharsets.UTF_8);
            String operation = classifyFragment(json, handle);
            resolvedOperation = mergeOperationType(resolvedOperation, operation, handle);
            collectFilesFromFragment(icebergTable, handle, json, operation,
                    allDataFiles, allDeleteFiles);
        }
        return new IcebergCollectedFragments(
                resolvedOperation, List.copyOf(allDataFiles), List.copyOf(allDeleteFiles));
    }

    /**
     * Classifies a JSON fragment as APPEND, OVERWRITE, DELETE, or MERGE
     * based on its structure and operation discriminator field.
     */
    private static String classifyFragment(String json, IcebergOutputTableHandle handle) {
        if (json.startsWith("[")) {
            return "APPEND";
        }
        if (json.startsWith("{")) {
            return classifyObjectFragment(json, handle);
        }
        throw new IllegalStateException(
                "Unrecognized fragment format for table %s.%s: expected JSON array or object"
                        .formatted(handle.schemaName(), handle.tableName()));
    }

    private static String classifyObjectFragment(String json, IcebergOutputTableHandle handle) {
        try {
            JsonNode root = SHARED_MAPPER.readTree(json);
            String operation = root.has("operation")
                    ? root.get("operation").asText() : "";
            return switch (operation) {
                case "DELETE" -> "DELETE";
                case "MERGE" -> "MERGE";
                default -> "OVERWRITE";
            };
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to parse fragment JSON for table %s.%s: %s"
                            .formatted(handle.schemaName(), handle.tableName(),
                                    e.getMessage()),
                    e);
        }
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
            String json, String operation,
            List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
        switch (operation) {
            case "APPEND" ->
                    dataFiles.addAll(deserializeDataFilesViaParser(icebergTable.spec(), json));
            case "OVERWRITE" ->
                    dataFiles.addAll(
                            deserializeOverwriteDataFilesViaParser(icebergTable.spec(), json));
            case "DELETE" ->
                    deleteFiles.addAll(deserializeDeleteFiles(icebergTable, handle, json));
            case "MERGE" -> {
                dataFiles.addAll(
                        deserializeOverwriteDataFilesViaParser(icebergTable.spec(), json));
                deleteFiles.addAll(deserializeDeleteFiles(icebergTable, handle, json));
            }
            default -> throw new IllegalStateException("Unknown operation: " + operation);
        }
    }

    /**
     * Deserializes DeleteFile descriptors from the {@code "deleteFiles"} JSON array
     * within a DELETE or MERGE fragment using Jackson for correct un-escaping.
     *
     * @throws IllegalStateException if a DeleteFile entry cannot be deserialized,
     *         including the file path and error details in the message
     */
    private static List<DeleteFile> deserializeDeleteFiles(
            Table table, IcebergOutputTableHandle handle, String json) {
        try {
            JsonNode root = SHARED_MAPPER.readTree(json);
            JsonNode deleteFilesNode = root.get("deleteFiles");
            if (deleteFilesNode == null || !deleteFilesNode.isArray()) {
                return List.of();
            }
            List<DeleteFile> files = new ArrayList<>();
            for (JsonNode element : deleteFilesNode) {
                files.add(parseDeleteFileEntry(table, handle, element));
            }
            return files;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to deserialize DeleteFiles for table %s.%s: %s"
                            .formatted(handle.schemaName(), handle.tableName(),
                                    e.getMessage()),
                    e);
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
            org.apache.iceberg.FileFormat fileFormat =
                    org.apache.iceberg.FileFormat.fromString(format);
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
    private static List<DataFile> deserializeDataFilesViaParser(PartitionSpec spec, String json) {
        String trimmed = json.trim();
        if (!trimmed.startsWith("[") || trimmed.equals("[]")) {
            return List.of();
        }
        try {
            JsonNode root = SHARED_MAPPER.readTree(trimmed);
            if (!root.isArray()) {
                throw new IllegalStateException(
                        "Expected JSON array for APPEND DataFiles, got: " + root.getNodeType());
            }
            List<DataFile> files = new ArrayList<>();
            for (JsonNode element : root) {
                files.add((DataFile) ContentFileParser.fromJson(element, spec));
            }
            return files;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to deserialize DataFile from JSON: " + e.getMessage(), e);
        }
    }

    /**
     * Deserializes DataFile descriptors from the {@code "dataFiles"} JSON array
     * within an OVERWRITE or MERGE fragment, using Iceberg's {@link ContentFileParser}.
     */
    private static List<DataFile> deserializeOverwriteDataFilesViaParser(PartitionSpec spec, String json) {
        try {
            JsonNode root = SHARED_MAPPER.readTree(json);
            JsonNode dataFilesNode = root.get("dataFiles");
            if (dataFilesNode == null || !dataFilesNode.isArray()) {
                return List.of();
            }
            List<DataFile> files = new ArrayList<>();
            for (JsonNode element : dataFilesNode) {
                files.add((DataFile) ContentFileParser.fromJson(element, spec));
            }
            return files;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to deserialize DataFile from JSON: " + e.getMessage(), e);
        }
    }
}
