package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries;

import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.PublishedInput;

import org.pangrova.generation.api.publication.GenerationManifest;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate.PublicationRead;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.pangrova.generation.api.constants.GenerationOutputTypeIds.TEXT_INDEX;
import static org.pangrova.generation.api.constants.GenerationOutputTypeIds.VECTOR_INDEX;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;

/** Same-connection immutable publication reads, shared by query resolution and durable claim preparation. */
final class PostgresGenerationPublishedInputs {
    private static final PostgresGenerationQueries INPUTS = new PostgresGenerationQueries();
    private static final String PUBLISHED_MANIFEST =
            "SELECT * FROM artifact_build_jobs WHERE tenant_id=? AND artifact_id=? "
                    + "AND staging_generation=? AND source_snapshot=? AND state='PUBLISHED'";
    private static final String LOCKED_PUBLISHED_MANIFEST = PUBLISHED_MANIFEST + " FOR SHARE";

    private PostgresGenerationPublishedInputs() {}

    static Optional<GenerationManifest> findManifest(Connection connection, String tenantId,
            String artifactId, String generation, String sourceSnapshot, boolean lock) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                lock ? LOCKED_PUBLISHED_MANIFEST : PUBLISHED_MANIFEST)) {
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
                return Optional.of(new GenerationManifest(
                        result.getString("tenant_id"), result.getString("job_id"),
                        result.getString("artifact_id"),
                        result.getString("artifact_type"),
                        result.getString("staging_generation"),
                        result.getString("source_id"), result.getString("source_snapshot"),
                        GenerationExecutionKind.valueOf(
                                result.getString("execution_kind")),
                        new GenerationDependencySet(
                                INPUTS.loadJobDependencies(
                                        connection, tenantId, result.getString("job_id")),
                                result.getString("dependency_set_sha256")),
                        GenerationEmptyPartitionPolicy.valueOf(
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

    /** Reads publication evidence once for the union of requested sources, then verifies each page separately. */
    static List<Map<PublicationRead, PublishedInput>> load(Connection connection, String tenantId,
            String executionId, List<PublicationRead> reads, List<List<GenerationSplit>> partitionSplits)
            throws SQLException {
        if (reads.isEmpty()) return Collections.nCopies(partitionSplits.size(), Map.of());
        var requested = partitionSplits.stream().flatMap(List::stream).map(GenerationSplit::dataObjectId)
                .distinct().sorted().toList();
        var result = new ArrayList<Map<PublicationRead, PublishedInput>>(partitionSplits.size());
        for (int i = 0; i < partitionSplits.size(); i++) result.add(new HashMap<>());
        for (var read : reads) {
            var manifest = findManifest(connection, tenantId, read.artifactId(), read.generation(),
                    read.sourceSnapshot(), true).orElseThrow(
                            () -> new IllegalStateException("Pinned publication is absent"));
            if (!manifest.sourceId().equals(read.sourceId())
                    || !manifest.manifestSha256().equals(read.manifestSha256())) {
                throw new IllegalStateException("Pinned stage publication identity or digest differs");
            }
            if (VECTOR_INDEX.equals(manifest.artifactType()) || TEXT_INDEX.equals(manifest.artifactType())) {
                PostgresSnapshotPinLedger.retainForExecution(connection, tenantId, read.artifactId(),
                        read.generation(), read.sourceSnapshot(), executionId);
            }
            loadPublicationWindow(connection, tenantId, manifest, read, requested, partitionSplits, result);
        }
        return result.stream().map(Map::copyOf).toList();
    }

    private static void loadPublicationWindow(Connection connection, String tenantId, GenerationManifest manifest,
            PublicationRead read, List<String> requested, List<List<GenerationSplit>> partitionSplits,
            List<Map<PublicationRead, PublishedInput>> result) throws SQLException {
        Map<String, List<GenerationOutput>> bySource = new HashMap<>();
        var generationOutputs = new ArrayList<GenerationOutput>();
        for (var output : PostgresGenerationOutputQueries.loadLockedPublishedOutputs(
                connection, tenantId, manifest.jobId(), requested)) {
            if (output.scope() == GenerationOutputScope.SOURCE_OBJECT) {
                bySource.computeIfAbsent(output.sourceObjectId(), ignored -> new ArrayList<>()).add(output);
            } else {
                generationOutputs.add(output);
            }
        }
        var checksums = PostgresGenerationOutputQueries.loadLockedPublishedSplitChecksums(
                connection, tenantId, manifest.jobId(), bySource.keySet().stream().sorted().toList());
        for (int i = 0; i < partitionSplits.size(); i++) {
            var covered = new LinkedHashSet<String>();
            var outputs = new ArrayList<>(generationOutputs);
            for (var split : partitionSplits.get(i)) {
                var scoped = bySource.get(split.dataObjectId());
                if (scoped == null) continue;
                if (!covered.add(split.dataObjectId())) {
                    throw new IllegalStateException("Durable publication inputs repeat a physical source object");
                }
                if (!split.splitSha256().equalsIgnoreCase(checksums.get(split.dataObjectId()))) {
                    throw new IllegalStateException(
                            "Pinned artifact source allocation differs from durable task inputs");
                }
                outputs.addAll(scoped);
            }
            result.get(i).put(read, new PublishedInput(manifest, outputs, covered));
        }
    }
}
