package org.pangrova.metadata.artifact.jdbc.build;

import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildPage;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Loads durable artifact pages and compares immutable job identities. */
@org.pangrova.spi.annotation.Internal
public final class JdbcArtifactBuildPageLoader {

    private static final int MAX_DEPENDENCY_FAN_IN = 4096;

    private JdbcArtifactBuildPageLoader() {}

    static List<ArtifactBuildPage> validateNewJob(
            ArtifactBuildJob job,
            ArtifactBuildDispatchSpec dispatchSpec,
            List<ArtifactBuildPage> pages) {
        Objects.requireNonNull(job, "job must not be null");
        Objects.requireNonNull(dispatchSpec, "dispatchSpec must not be null");
        if (!job.jobId().equals(dispatchSpec.jobId())) {
            throw new IllegalArgumentException("dispatch spec belongs to a different job");
        }
        if (job.artifactType().equals(org.pangrova.metadata.artifact.contract.ArtifactTypeIds.TABULAR_EXPORT)
                && (dispatchSpec.stageGraph().purpose()
                        != org.pangrova.metadata.artifact.build.ArtifactStageGraph.Purpose.RELATIONAL_EXPORT
                        || pages.isEmpty())) {
            throw new IllegalArgumentException("Tabular export requires a real relational terminal partition cohort");
        }
        if (job.expectedPages() == 0
                && (job.emptyPartitionPolicy()
                        != org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy
                                .ALLOW_ZERO_ROWS
                        || !org.pangrova.metadata.artifact.contract.ArtifactFormatContracts.require(
                                job.formatId(), job.artifactType())
                                .supportsEmptyPartitions())) {
            throw new IllegalArgumentException(
                    "empty artifact generation is forbidden by job or format policy");
        }
        ArtifactBuildJobState requiredState = job.expectedPages() == 0
                ? ArtifactBuildJobState.READY_TO_PUBLISH
                : ArtifactBuildJobState.QUEUED;
        if (job.state() != requiredState
                || job.completedPages() != 0 || job.publicationFencingToken() != 0) {
            throw new IllegalArgumentException(
                    "a new artifact job must start empty in " + requiredState);
        }
        List<ArtifactBuildPage> validated = validatePages(job, pages);
        validateStageCohorts(dispatchSpec.stageGraph(), validated);
        return validated;
    }

    private static void validateStageCohorts(
            org.pangrova.metadata.artifact.build.ArtifactStageGraph graph, List<ArtifactBuildPage> pages) {
        for (ArtifactBuildPage page : pages) {
            var stage = graph.requireStage(page.stageId());
            if (stage.pageKind() != page.kind() || page.partitionId() >= stage.parallelism()) {
                throw new IllegalArgumentException("artifact page changes its persisted stage contract");
            }
        }
        for (var stage : graph.stages()) {
            Set<Integer> partitions = pages.stream().filter(page -> page.stageId() == stage.stageId())
                    .map(ArtifactBuildPage::partitionId).collect(java.util.stream.Collectors.toSet());
            if (partitions.size() != stage.parallelism()) {
                throw new IllegalArgumentException("artifact stage partition cohort is incomplete");
            }
        }
        for (var edge : graph.exchanges()) {
            Set<String> producers = pages.stream().filter(page -> page.stageId() == edge.upstreamStageId())
                    .map(ArtifactBuildPage::pageId).collect(java.util.stream.Collectors.toSet());
            if (pages.stream().filter(page -> page.stageId() == edge.downstreamStageId())
                    .anyMatch(page -> !page.dependencyPageIds().containsAll(producers))) {
                throw new IllegalArgumentException("durable exchange is missing a complete producer barrier");
            }
        }
    }

    static List<ArtifactBuildPage> validatePages(
            ArtifactBuildJob job, List<ArtifactBuildPage> pages) {
        List<ArtifactBuildPage> result = List.copyOf(pages);
        if (result.size() != job.expectedPages()) {
            throw new IllegalArgumentException("expectedPages differs from supplied pages");
        }
        validatePageIdentities(job, result);
        Map<String, ArtifactBuildPage> byId = result.stream().collect(
                java.util.stream.Collectors.toUnmodifiableMap(
                        ArtifactBuildPage::pageId, page -> page));
        long finalizers = result.stream()
                .filter(page -> page.kind() == ArtifactBuildPageKind.GENERATION_FINALIZE)
                .count();
        if (finalizers > 1) {
            throw new IllegalArgumentException("artifact DAG has multiple generation finalizers");
        }
        result.forEach(page -> validateDependencies(page, byId));
        return result.stream().sorted(
                Comparator.comparingInt(ArtifactBuildPage::pageOrdinal)).toList();
    }

    private static void validatePageIdentities(
            ArtifactBuildJob job, List<ArtifactBuildPage> pages) {
        Set<String> pageIds = new HashSet<>();
        Set<Integer> ordinals = new HashSet<>();
        Set<String> partitions = new HashSet<>();
        for (ArtifactBuildPage page : pages) {
            if (!job.jobId().equals(page.jobId()) || !pageIds.add(page.pageId())
                    || !ordinals.add(page.pageOrdinal())
                    || !partitions.add(page.stageId() + ":" + page.partitionId())) {
                throw new IllegalArgumentException(
                        "artifact pages have duplicate or foreign identity");
            }
        }
    }

    private static void validateDependencies(
            ArtifactBuildPage page, Map<String, ArtifactBuildPage> byId) {
        if (page.dependencyPageIds().size() > MAX_DEPENDENCY_FAN_IN) {
            throw new IllegalArgumentException("artifact dependency fan-in exceeds 4096");
        }
        for (String dependencyId : page.dependencyPageIds()) {
            ArtifactBuildPage dependency = byId.get(dependencyId);
            if (dependency == null || dependency.pageOrdinal() >= page.pageOrdinal()
                    || dependency.stageId() == page.stageId()
                    || dependency.kind() == ArtifactBuildPageKind.GENERATION_FINALIZE) {
                throw new IllegalArgumentException(
                        "artifact page dependency violates DAG ordering");
            }
        }
    }

    static boolean sameIdentity(ArtifactBuildJob left, ArtifactBuildJob right) {
        return sameCoreIdentity(left, right) && sameFormatIdentity(left, right)
                && left.tenantId().equals(right.tenantId())
                && left.executionKind() == right.executionKind()
                && left.dependencySet().equals(right.dependencySet())
                && left.emptyPartitionPolicy() == right.emptyPartitionPolicy()
                && left.encryptionKeyRef().equals(right.encryptionKeyRef())
                && left.idempotencyKey().equals(right.idempotencyKey())
                && left.expectedPages() == right.expectedPages();
    }

    private static boolean sameCoreIdentity(ArtifactBuildJob left, ArtifactBuildJob right) {
        return left.jobId().equals(right.jobId())
                && left.artifactId().equals(right.artifactId())
                && left.artifactType().equals(right.artifactType())
                && left.stagingGeneration().equals(right.stagingGeneration())
                && left.sourceId().equals(right.sourceId())
                && left.sourceSnapshot().equals(right.sourceSnapshot())
                && left.stagingUri().equals(right.stagingUri());
    }

    private static boolean sameFormatIdentity(ArtifactBuildJob left, ArtifactBuildJob right) {
        return left.formatId().equals(right.formatId())
                && left.formatMajor() == right.formatMajor()
                && left.formatMinor() == right.formatMinor()
                && left.schemaSha256().equalsIgnoreCase(right.schemaSha256())
                && left.writerBuildId().equals(right.writerBuildId());
    }

    public static List<ArtifactBuildPage> loadPages(
            Connection connection, String tenantId, String jobId)
            throws SQLException {
        List<ArtifactBuildPage> pages = new ArrayList<>();
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

    private static ArtifactBuildPage mapPage(
            Connection connection, String tenantId, String jobId, ResultSet result)
            throws SQLException {
        String pageId = result.getString("page_id");
        return new ArtifactBuildPage(
                result.getString("job_id"), pageId,
                result.getInt("stage_id"), result.getInt("partition_id"),
                result.getInt("page_ordinal"),
                ArtifactBuildPageKind.valueOf(result.getString("page_kind")),
                JdbcArtifactBuildStoreSupport.loadSplits(
                        connection, tenantId, jobId, pageId),
                JdbcArtifactBuildStoreSupport.loadDependencies(
                        connection, tenantId, jobId, pageId),
                result.getInt("max_attempts"));
    }
}
