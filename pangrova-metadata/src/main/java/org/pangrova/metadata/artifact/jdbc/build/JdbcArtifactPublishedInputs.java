package org.pangrova.metadata.artifact.jdbc.build;

import org.pangrova.metadata.artifact.ArtifactGenerationManifest;
import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.build.ArtifactBuildSplit;
import org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate.PublicationRead;
import org.pangrova.metadata.artifact.contract.ArtifactBuildOutputScope;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStoreMappings.loadArtifactDependencies;
import static org.pangrova.metadata.store.JdbcSupport.getInstant;

/** Same-connection immutable publication reads, shared by query resolution and durable claim preparation. */
final class JdbcArtifactPublishedInputs {
    private JdbcArtifactPublishedInputs() {}

    static Optional<ArtifactGenerationManifest> findManifest(Connection connection, String tenantId,
            String artifactId, String generation, String sourceSnapshot) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM artifact_build_jobs WHERE tenant_id=? AND artifact_id=? "
                        + "AND staging_generation=? AND source_snapshot=? "
                        + "AND state='PUBLISHED'")) {
            statement.setString(1, tenantId);
            statement.setString(2, artifactId);
            statement.setString(3, generation);
            statement.setString(4, sourceSnapshot);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                String manifestSha = result.getString("manifest_sha256");
                Instant publishedAt = getInstant(result, "published_at");
                if (manifestSha == null || publishedAt == null) {
                    throw new IllegalStateException(
                            "published artifact is missing its immutable manifest identity");
                }
                return Optional.of(new ArtifactGenerationManifest(
                        result.getString("tenant_id"), result.getString("job_id"),
                        result.getString("artifact_id"),
                        result.getString("artifact_type"),
                        result.getString("staging_generation"),
                        result.getString("source_id"), result.getString("source_snapshot"),
                        ArtifactExecutionKind.valueOf(
                                result.getString("execution_kind")),
                        new ArtifactDependencySet(
                                loadArtifactDependencies(
                                        connection, tenantId, result.getString("job_id")),
                                result.getString("dependency_set_sha256")),
                        ArtifactEmptyPartitionPolicy.valueOf(
                                result.getString("empty_partition_policy")),
                        result.getString("format_id"),
                        result.getInt("format_major"), result.getInt("format_minor"),
                        result.getString("schema_sha256"), result.getString("writer_build_id"),
                        "metadata://artifact-generation/" + manifestSha, manifestSha,
                        result.getLong("published_object_count"),
                        result.getLong("published_row_count"),
                        result.getLong("published_total_bytes"), publishedAt));
            }
        }
    }

    static Map<PublicationRead, JdbcArtifactStageExecutionStore.PublishedInput> load(Connection connection,
            String tenantId, List<PublicationRead> reads, List<ArtifactBuildSplit> splits) throws SQLException {
        Map<PublicationRead, JdbcArtifactStageExecutionStore.PublishedInput> result = new LinkedHashMap<>();
        var requested = splits.stream().map(ArtifactBuildSplit::dataObjectId).sorted().distinct().toList();
        for (var read : reads) {
            var manifest = findManifest(connection, tenantId, read.artifactId(), read.generation(),
                    read.sourceSnapshot()).orElseThrow(() -> new IllegalStateException("Pinned publication is absent"));
            if (!manifest.sourceId().equals(read.sourceId()) || !manifest.manifestSha256().equals(read.manifestSha256())) {
                throw new IllegalStateException("Pinned stage publication identity or digest differs");
            }
            var outputs = JdbcArtifactBuildOutputQueries.loadPublished(connection, tenantId, manifest.jobId(), requested);
            Map<String, String> covered = splitChecksums(splits, outputs);
            if (!JdbcArtifactBuildOutputQueries.matchesPublishedSplits(connection, tenantId, manifest.jobId(),
                    List.copyOf(covered.keySet()), covered)) {
                throw new IllegalStateException("Pinned artifact source allocation differs from durable task inputs");
            }
            result.put(read, new JdbcArtifactStageExecutionStore.PublishedInput(manifest, outputs, covered.keySet()));
        }
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, String> splitChecksums(List<ArtifactBuildSplit> splits, List<ArtifactBuildOutput> outputs) {
        var coveredIds = outputs.stream()
                .filter(output -> output.scope() == ArtifactBuildOutputScope.SOURCE_OBJECT)
                .map(ArtifactBuildOutput::sourceObjectId).collect(java.util.stream.Collectors.toSet());
        Map<String, String> checksums = new HashMap<>();
        for (var split : splits) {
            if (coveredIds.contains(split.dataObjectId())) {
                if (checksums.put(split.dataObjectId(), split.splitSha256()) != null) {
                    throw new IllegalStateException("Durable publication inputs repeat a physical source object");
                }
            }
        }
        return checksums;
    }
}
