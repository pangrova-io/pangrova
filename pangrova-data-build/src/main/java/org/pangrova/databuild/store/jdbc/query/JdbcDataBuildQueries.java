package org.pangrova.databuild.store.jdbc.query;

import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.spi.jdbc.DataBuildTransactionInputs;
import org.pangrova.databuild.api.DataBuildDependency;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.protocol.DataBuildStageGraphCodec;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

/** Reads immutable scoped build inputs using the caller's connection; never opens or commits a transaction. */
@org.pangrova.common.api.annotation.Internal
public final class JdbcDataBuildQueries implements DataBuildTransactionInputs {
    public JdbcDataBuildQueries() {}

    /** Reads the immutable scoped dispatch contract without synthesizing missing state. */
    @Override
    public Optional<DataBuildDispatchSpec> findDispatchSpec(
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
                return Optional.of(new DataBuildDispatchSpec(
                        jobId, DataBuildStageGraphCodec.decode(
                                result.getBytes("stage_graph_bytes"), result.getString("stage_graph_sha256")),
                        result.getString("tenant_id"), result.getString("principal"),
                        org.pangrova.security.protocol.DeferredAuthorizationScopeCodec.decode(
                                result.getBytes("authorization_scope_bytes")),
                        result.getLong("max_memory_bytes"), result.getInt("max_threads"),
                        result.getLong("max_spill_bytes"),
                        result.getLong("task_timeout_millis")));
            }
        }
    }

    /** Loads the exact version-pinned input dependencies in their canonical ordinal order. */
    @Override
    public List<DataBuildDependency> loadJobDependencies(
            Connection connection, String tenantId, String jobId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT dependency_kind,dependency_id,dependency_version "
                        + "FROM artifact_build_dependencies "
                        + "WHERE tenant_id=? AND job_id=? "
                        + "ORDER BY dependency_ordinal")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                java.util.ArrayList<DataBuildDependency> dependencies =
                        new java.util.ArrayList<>();
                while (result.next()) {
                    dependencies.add(new DataBuildDependency(
                            result.getString(1), result.getString(2), result.getString(3)));
                }
                return List.copyOf(dependencies);
            }
        }
    }

    /** Loads the exact scoped page cohort in its persisted ordinal order. */
    @Override
    public List<DataBuildPage> loadPages(
            Connection connection, String tenantId, String jobId)
            throws SQLException {
        List<DataBuildPage> pages = new ArrayList<>();
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

    private DataBuildPage mapPage(
            Connection connection, String tenantId, String jobId, ResultSet result)
            throws SQLException {
        String pageId = result.getString("page_id");
        return new DataBuildPage(
                result.getString("job_id"), pageId,
                result.getInt("stage_id"), result.getInt("partition_id"),
                result.getInt("page_ordinal"),
                DataBuildPageKind.valueOf(result.getString("page_kind")),
                loadSplits(
                        connection, tenantId, jobId, pageId),
                loadDependencies(
                        connection, tenantId, jobId, pageId),
                result.getInt("max_attempts"));
    }

    /** Loads replayable canonical splits in their persisted source order, rejecting legacy missing bytes. */
    public List<DataBuildSplit> loadSplits(
            Connection connection, String tenantId, String jobId, String pageId)
            throws SQLException {
        List<DataBuildSplit> splits = new ArrayList<>();
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
                    byte[] bytes = result.getBytes(2);
                    String checksum = result.getString(3);
                    if (bytes == null || bytes.length == 0 || checksum == null) {
                        throw new IllegalStateException(
                                "artifact page predates replayable canonical split storage");
                    }
                    splits.add(new DataBuildSplit(
                            result.getString(1), Base64.getEncoder().encodeToString(bytes),
                            checksum));
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

}
