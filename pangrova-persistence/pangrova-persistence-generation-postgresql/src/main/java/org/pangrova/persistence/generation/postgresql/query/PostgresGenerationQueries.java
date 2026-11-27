package org.pangrova.persistence.generation.postgresql.query;

import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionInputs;
import org.pangrova.generation.api.dependency.GenerationDependency;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.codec.execution.GenerationStageGraphCodec;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Reads immutable scoped build inputs using the caller's connection; never opens or commits a transaction. */
@org.pangrova.common.api.annotation.Internal
public final class PostgresGenerationQueries implements GenerationTransactionInputs {
    public PostgresGenerationQueries() {}

    /** Reads the immutable scoped dispatch contract without synthesizing missing state. */
    @Override
    public Optional<GenerationDispatchSpec> findDispatchSpec(
            Connection connection, String tenantId, String jobId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM artifact_build_dispatch_specs "
                        + "WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(new GenerationDispatchSpec(
                        jobId, GenerationStageGraphCodec.decode(
                                result.getBytes("stage_graph_bytes"), result.getString("stage_graph_sha256")),
                        result.getString("tenant_id"), result.getString("principal"),
                        org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec.decode(
                                result.getBytes("authorization_scope_bytes")),
                        result.getLong("max_memory_bytes"), result.getInt("max_threads"),
                        result.getLong("max_spill_bytes"),
                        result.getLong("task_timeout_millis")));
            }
        }
    }

    /** Loads the exact version-pinned input dependencies in their canonical ordinal order. */
    @Override
    public List<GenerationDependency> loadJobDependencies(
            Connection connection, String tenantId, String jobId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT dependency_kind,dependency_id,dependency_version "
                        + "FROM artifact_build_dependencies "
                        + "WHERE tenant_id=? AND job_id=? "
                        + "ORDER BY dependency_ordinal")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                java.util.ArrayList<GenerationDependency> dependencies =
                        new java.util.ArrayList<>();
                while (result.next()) {
                    dependencies.add(new GenerationDependency(
                            result.getString(1), result.getString(2), result.getString(3)));
                }
                return List.copyOf(dependencies);
            }
        }
    }

    /** Loads the exact scoped page cohort in its persisted ordinal order. */
    @Override
    public List<GenerationPage> loadPages(
            Connection connection, String tenantId, String jobId)
            throws SQLException {
        List<GenerationPage> pages = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT job_id,page_id,stage_id,partition_id,page_ordinal,page_kind,max_attempts "
                        + "FROM artifact_build_pages WHERE tenant_id=? AND job_id=? "
                        + "ORDER BY page_ordinal")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    pages.add(mapPage(connection, tenantId, jobId, result));
                }
            }
        }
        return List.copyOf(pages);
    }

    private GenerationPage mapPage(
            Connection connection, String tenantId, String jobId, ResultSet result)
            throws SQLException {
        String pageId = result.getString("page_id");
        return new GenerationPage(
                result.getString("job_id"), pageId,
                result.getInt("stage_id"), result.getInt("partition_id"),
                result.getInt("page_ordinal"),
                GenerationPageKind.valueOf(result.getString("page_kind")),
                loadSplits(
                        connection, tenantId, jobId, pageId),
                loadDependencies(
                        connection, tenantId, jobId, pageId),
                result.getInt("max_attempts"));
    }

    /** Loads replayable canonical splits in their persisted source order, rejecting legacy missing bytes. */
    public List<GenerationSplit> loadSplits(
            Connection connection, String tenantId, String jobId, String pageId)
            throws SQLException {
        List<GenerationSplit> splits = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT source_object_id,split_bytes,split_sha256 "
                        + "FROM artifact_build_page_sources "
                        + "WHERE tenant_id=? AND job_id=? AND page_id=? "
                        + "ORDER BY source_ordinal")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setString(3, pageId);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    splits.add(readSplit(result));
                }
            }
        }
        return List.copyOf(splits);
    }

    /** Loads the exact producer-page barrier in its persisted dependency order. */
    public List<String> loadDependencies(
            Connection connection, String tenantId, String jobId, String pageId)
            throws SQLException {
        List<String> dependencies = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT dependency_page_id FROM artifact_build_page_dependencies "
                        + "WHERE tenant_id=? AND job_id=? AND page_id=? "
                        + "ORDER BY dependency_ordinal")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setString(3, pageId);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    dependencies.add(result.getString(1));
                }
            }
        }
        return List.copyOf(dependencies);
    }

    /** Immutable inputs in the exact source and dependency ordinals of one page. */
    public record PageInputs(List<GenerationSplit> splits, List<String> dependencies) {
        public PageInputs {
            splits = List.copyOf(splits);
            dependencies = List.copyOf(dependencies);
        }
    }

    /** Loads the supplied locked page IDs with two bounded-cursor reads on the caller's connection. */
    public List<PageInputs> loadPageInputs(
            Connection connection, String tenantId, String jobId, List<String> pageIds) throws SQLException {
        if (pageIds.isEmpty()) return List.of();
        var ids = connection.createArrayOf("varchar", pageIds.toArray(String[]::new));
        Map<String, List<GenerationSplit>> splits = new HashMap<>();
        Map<String, List<String>> dependencies = new HashMap<>();
        try {
            try (var statement = connection.prepareStatement("""
                    SELECT page_id,source_object_id,split_bytes,split_sha256
                    FROM artifact_build_page_sources
                    WHERE tenant_id=? AND job_id=? AND page_id=ANY(?)
                    ORDER BY page_id,source_ordinal
                    """)) {
                bindPageInputs(statement, tenantId, jobId, ids);
                try (var rows = statement.executeQuery()) {
                    while (rows.next()) {
                        splits.computeIfAbsent(rows.getString("page_id"), key -> new ArrayList<>())
                                .add(readSplit(rows));
                    }
                }
            }
            try (var statement = connection.prepareStatement("""
                    SELECT page_id,dependency_page_id
                    FROM artifact_build_page_dependencies
                    WHERE tenant_id=? AND job_id=? AND page_id=ANY(?)
                    ORDER BY page_id,dependency_ordinal
                    """)) {
                bindPageInputs(statement, tenantId, jobId, ids);
                try (var rows = statement.executeQuery()) {
                    while (rows.next()) {
                        dependencies.computeIfAbsent(rows.getString("page_id"), key -> new ArrayList<>())
                                .add(rows.getString("dependency_page_id"));
                    }
                }
            }
        } finally {
            ids.free();
        }
        return pageIds.stream().map(id -> new PageInputs(
                splits.getOrDefault(id, List.of()), dependencies.getOrDefault(id, List.of()))).toList();
    }

    private static void bindPageInputs(PreparedStatement statement, String tenantId, String jobId, java.sql.Array ids)
            throws SQLException {
        statement.setFetchSize(16);
        statement.setString(1, tenantId);
        statement.setString(2, jobId);
        statement.setArray(3, ids);
    }

    private static GenerationSplit readSplit(ResultSet row) throws SQLException {
        byte[] bytes = row.getBytes("split_bytes");
        String checksum = row.getString("split_sha256");
        if (bytes == null || bytes.length == 0 || checksum == null) {
            throw new IllegalStateException("artifact page predates replayable canonical split storage");
        }
        return new GenerationSplit(row.getString("source_object_id"), Base64.getEncoder().encodeToString(bytes),
                checksum);
    }

}
