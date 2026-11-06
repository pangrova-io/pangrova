package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.page.GenerationSplit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Batched JDBC queries for immutable artifact outputs and canonical splits. */
final class PostgresGenerationOutputQueries {

    private static final int LOOKUP_BATCH_SIZE = 500;

    private PostgresGenerationOutputQueries() {}

    static List<String> requireDependencies(List<String> dependencyPageIds) {
        List<String> dependencies = List.copyOf(dependencyPageIds);
        if (dependencies.isEmpty()
                || dependencies.stream().anyMatch(id -> id == null || id.isBlank())
                || new HashSet<>(dependencies).size() != dependencies.size()) {
            throw new IllegalArgumentException(
                    "dependencyPageIds must be non-empty and unique");
        }
        return dependencies;
    }

    static List<String> requireSourceIds(Set<String> sourceObjectIds) {
        List<String> requested = sourceObjectIds.stream().sorted().toList();
        if (requested.stream().anyMatch(id -> id == null || id.isBlank())) {
            throw new IllegalArgumentException("sourceObjectIds must not contain blanks");
        }
        return requested;
    }

    static List<String> requireSplitChecksums(Map<String, String> sourceSplitSha256) {
        List<String> sourceIds = sourceSplitSha256.keySet().stream().sorted().toList();
        if (sourceIds.stream().anyMatch(id -> id == null || id.isBlank())
                || sourceSplitSha256.values().stream().anyMatch(
                        sha -> sha == null || !sha.matches("[0-9a-fA-F]{64}"))) {
            throw new IllegalArgumentException(
                    "source split identities and SHA-256 values must be complete");
        }
        return sourceIds;
    }

    static List<GenerationOutput> loadDependencies(
            Connection connection, String tenantId,
            String jobId, List<String> dependencies) throws SQLException {
        String placeholders = placeholders(dependencies.size());
        List<GenerationOutput> outputs = loadOutputsForPages(
                connection, tenantId, jobId, dependencies, placeholders);
        Set<String> succeeded = loadSucceededPages(
                connection, tenantId, jobId, dependencies, placeholders);
        if (!succeeded.equals(Set.copyOf(dependencies)) || outputs.isEmpty()) {
            throw new IllegalStateException(
                    "artifact dependency pages are incomplete or have no outputs");
        }
        return List.copyOf(outputs);
    }

    private static List<GenerationOutput> loadOutputsForPages(
            Connection connection, String tenantId, String jobId, List<String> dependencies,
            String placeholders) throws SQLException {
        List<GenerationOutput> outputs = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT o.object_id,o.opaque_uri,o.object_role,o.source_object_id,"
                        + "o.object_ordinal,o.object_scope,o.size_bytes,o.row_count,o.checksum_sha256,"
                        + "o.encryption_key_ref FROM artifact_build_outputs o "
                        + "JOIN artifact_build_pages p ON p.tenant_id=o.tenant_id "
                        + "AND p.job_id=o.job_id AND p.page_id=o.page_id "
                        + "WHERE o.tenant_id=? AND o.job_id=? "
                        + "AND o.page_id IN (" + placeholders + ") "
                        + "AND p.state='SUCCEEDED' ORDER BY o.page_id,o.object_ordinal")) {
            bindList(statement, tenantId, jobId, dependencies);
            appendOutputs(statement, outputs);
        }
        return outputs;
    }

    private static Set<String> loadSucceededPages(
            Connection connection, String tenantId, String jobId, List<String> dependencies,
            String placeholders) throws SQLException {
        Set<String> succeeded = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT page_id FROM artifact_build_pages WHERE tenant_id=? AND job_id=? "
                        + "AND page_id IN (" + placeholders + ") AND state='SUCCEEDED'")) {
            bindList(statement, tenantId, jobId, dependencies);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    succeeded.add(result.getString(1));
                }
            }
        }
        return succeeded;
    }

    static List<GenerationOutput> loadPublished(
            Connection connection, String tenantId,
            String jobId, List<String> requested) throws SQLException {
        requirePublishedJob(connection, tenantId, jobId);
        List<GenerationOutput> outputs = new ArrayList<>();
        for (int offset = 0; offset < requested.size(); offset += LOOKUP_BATCH_SIZE) {
            List<String> batch = requested.subList(
                    offset, Math.min(requested.size(), offset + LOOKUP_BATCH_SIZE));
            loadSourceOutputs(connection, tenantId, jobId, batch, outputs);
        }
        loadGenerationOutputs(connection, tenantId, jobId, outputs);
        return List.copyOf(outputs);
    }

    private static void loadSourceOutputs(
            Connection connection, String tenantId, String jobId, List<String> batch,
            List<GenerationOutput> outputs) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT object_id,opaque_uri,object_role,source_object_id,"
                        + "object_ordinal,object_scope,size_bytes,row_count,checksum_sha256,"
                        + "encryption_key_ref FROM artifact_build_outputs "
                        + "WHERE tenant_id=? AND job_id=? AND object_scope='SOURCE_OBJECT' "
                        + "AND source_object_id IN (" + placeholders(batch.size()) + ") "
                        + "ORDER BY source_object_id,object_ordinal,object_id")) {
            bindList(statement, tenantId, jobId, batch);
            appendOutputs(statement, outputs);
        }
    }

    private static void loadGenerationOutputs(
            Connection connection, String tenantId,
            String jobId, List<GenerationOutput> outputs)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT object_id,opaque_uri,object_role,source_object_id,"
                        + "object_ordinal,object_scope,size_bytes,row_count,checksum_sha256,"
                        + "encryption_key_ref FROM artifact_build_outputs "
                        + "WHERE tenant_id=? AND job_id=? AND object_scope='GENERATION' "
                        + "ORDER BY object_ordinal,object_id")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            appendOutputs(statement, outputs);
        }
    }

    static boolean matchesPublishedSplits(
            Connection connection, String tenantId, String jobId, List<String> sourceIds,
            Map<String, String> requested) throws SQLException {
        requirePublishedJob(connection, tenantId, jobId);
        Map<String, String> stored = new HashMap<>();
        for (int offset = 0; offset < sourceIds.size(); offset += LOOKUP_BATCH_SIZE) {
            List<String> batch = sourceIds.subList(
                    offset, Math.min(sourceIds.size(), offset + LOOKUP_BATCH_SIZE));
            loadStoredSplits(connection, tenantId, jobId, batch, stored);
        }
        return stored.keySet().equals(requested.keySet())
                && stored.entrySet().stream().allMatch(entry ->
                        entry.getValue() != null && entry.getValue().equalsIgnoreCase(
                                requested.get(entry.getKey())));
    }

    static Map<String, GenerationSplit> loadPublishedSourceSplits(
            Connection connection, String tenantId, String jobId, List<String> sourceIds)
            throws SQLException {
        requirePublishedJob(connection, tenantId, jobId);
        Map<String, GenerationSplit> stored = new HashMap<>();
        for (int offset = 0; offset < sourceIds.size(); offset += LOOKUP_BATCH_SIZE) {
            List<String> batch = sourceIds.subList(
                    offset, Math.min(sourceIds.size(), offset + LOOKUP_BATCH_SIZE));
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT source_object_id,split_bytes,split_sha256 "
                            + "FROM artifact_build_page_sources WHERE tenant_id=? AND job_id=? "
                            + "AND source_object_id IN (" + placeholders(batch.size()) + ")")) {
                bindList(statement, tenantId, jobId, batch);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        String sourceId = result.getString(1);
                        byte[] bytes = result.getBytes(2);
                        String checksum = result.getString(3);
                        if (bytes == null || checksum == null) {
                            throw new IllegalStateException("Published canonical source split is missing");
                        }
                        GenerationSplit split = GenerationSplit.of(sourceId, bytes);
                        if (!split.splitSha256().equalsIgnoreCase(checksum)) {
                            throw new IllegalStateException("Published canonical source split checksum differs");
                        }
                        if (stored.putIfAbsent(sourceId, split) != null) {
                            throw new IllegalStateException("Published source has duplicate canonical splits");
                        }
                    }
                }
            }
        }
        return Map.copyOf(stored);
    }

    private static void loadStoredSplits(
            Connection connection, String tenantId, String jobId, List<String> batch,
            Map<String, String> stored) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT source_object_id,split_sha256 "
                        + "FROM artifact_build_page_sources WHERE tenant_id=? AND job_id=? "
                        + "AND source_object_id IN (" + placeholders(batch.size()) + ")")) {
            bindList(statement, tenantId, jobId, batch);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    String previous = stored.put(result.getString(1), result.getString(2));
                    if (previous != null) {
                        throw new IllegalStateException(
                                "published source object has duplicate canonical splits");
                    }
                }
            }
        }
    }

    private static void requirePublishedJob(
            Connection connection, String tenantId, String jobId)
            throws SQLException {
        GenerationJob job = PostgresGenerationJobStoreMappings.findJob(
                connection, tenantId, jobId, true).orElseThrow(
                        () -> new IllegalArgumentException(
                                "unknown artifact build job: " + jobId));
        if (job.state() != GenerationJobState.PUBLISHED) {
            throw new IllegalStateException("artifact generation is not atomically published");
        }
    }

    private static void appendOutputs(
            PreparedStatement statement, List<GenerationOutput> outputs) throws SQLException {
        try (ResultSet result = statement.executeQuery()) {
            while (result.next()) {
                outputs.add(PostgresGenerationJobStoreMappings.mapOutput(result));
            }
        }
    }

    private static void bindList(
            PreparedStatement statement, String tenantId,
            String jobId, List<String> values) throws SQLException {
        statement.setString(1, tenantId);
        statement.setString(2, jobId);
        for (int index = 0; index < values.size(); index++) {
            statement.setString(index + 3, values.get(index));
        }
    }

    private static String placeholders(int size) {
        return String.join(",", java.util.Collections.nCopies(size, "?"));
    }
}
