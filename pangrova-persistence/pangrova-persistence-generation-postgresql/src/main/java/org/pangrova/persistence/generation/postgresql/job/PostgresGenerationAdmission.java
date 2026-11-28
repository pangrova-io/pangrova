package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.generation.codec.execution.GenerationStageGraphCodec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.enums.GenerationJobState;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Validates new durable build cohorts and compares immutable identities on replay. */
final class PostgresGenerationAdmission {

    private static final int MAX_DEPENDENCY_FAN_IN = 4096;

    private PostgresGenerationAdmission() {}

    static List<GenerationPage> validateNewJob(
            GenerationJob job,
            GenerationDispatchSpec dispatchSpec,
            List<GenerationPage> pages) {
        Objects.requireNonNull(job, "job must not be null");
        Objects.requireNonNull(dispatchSpec, "dispatchSpec must not be null");
        // Raw contracts may be supplied without the submission factory; reject before any SQL mutation.
        GenerationStageGraphCodec.encode(dispatchSpec.stageGraph());
        if (!job.jobId().equals(dispatchSpec.jobId())) {
            throw new IllegalArgumentException("dispatch spec belongs to a different job");
        }
        if (job.artifactType().equals(org.pangrova.generation.api.constants.GenerationOutputTypeIds.TABULAR_EXPORT)
                && (dispatchSpec.stageGraph().purpose()
                        != GenerationGraphPurpose.RELATIONAL_EXPORT
                        || pages.isEmpty())) {
            throw new IllegalArgumentException("Tabular export requires a real relational terminal partition cohort");
        }
        if (job.expectedPages() == 0
                && (job.emptyPartitionPolicy()
                        != org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy
                                .ALLOW_ZERO_ROWS
                        || !org.pangrova.generation.codec.format.BuiltInGenerationOutputFormats.require(
                                job.formatId(), job.artifactType())
                                .supportsEmptyPartitions())) {
            throw new IllegalArgumentException(
                    "empty artifact generation is forbidden by job or format policy");
        }
        GenerationJobState requiredState = job.expectedPages() == 0
                ? GenerationJobState.READY_TO_PUBLISH
                : GenerationJobState.QUEUED;
        if (job.state() != requiredState
                || job.completedPages() != 0 || job.publicationFencingToken() != 0) {
            throw new IllegalArgumentException(
                    "a new artifact job must start empty in " + requiredState);
        }
        List<GenerationPage> validated = validatePages(job, pages);
        validateStageCohorts(dispatchSpec.stageGraph(), validated);
        return validated;
    }

    private static void validateStageCohorts(
            org.pangrova.generation.api.graph.GenerationStageGraph graph, List<GenerationPage> pages) {
        var stages = graph.stages().stream().collect(Collectors.toMap(stage -> stage.stageId(), stage -> stage));
        var consumers = graph.exchanges().stream().map(edge -> edge.downstreamStageId())
                .collect(Collectors.toSet());
        var counts = new HashMap<Integer, Integer>();
        for (GenerationPage page : pages) {
            var stage = stages.get(page.stageId());
            if (stage == null || stage.pageKind() != page.kind() || page.partitionId() >= stage.parallelism()) {
                throw new IllegalArgumentException("artifact page changes its persisted stage contract");
            }
            if (page.kind().isRelational() && consumers.contains(page.stageId()) && !page.splits().isEmpty()) {
                throw new IllegalArgumentException("relational exchange consumers cannot enumerate source splits");
            }
            counts.merge(page.stageId(), 1, Integer::sum);
        }
        for (var stage : graph.stages()) {
            if (counts.getOrDefault(stage.stageId(), 0) != stage.parallelism()) {
                throw new IllegalArgumentException("artifact stage partition cohort is incomplete");
            }
        }
        if (graph.purpose() == GenerationGraphPurpose.RELATIONAL_EXPORT) {
            return;
        }
        for (var edge : graph.exchanges()) {
            Set<String> producers = pages.stream().filter(page -> page.stageId() == edge.upstreamStageId())
                    .map(GenerationPage::pageId).collect(java.util.stream.Collectors.toSet());
            if (pages.stream().filter(page -> page.stageId() == edge.downstreamStageId())
                    .anyMatch(page -> !page.dependencyPageIds().containsAll(producers))) {
                throw new IllegalArgumentException("durable exchange is missing a complete producer barrier");
            }
        }
    }

    static List<GenerationPage> validatePages(
            GenerationJob job, List<GenerationPage> pages) {
        List<GenerationPage> result = List.copyOf(pages);
        if (result.size() != job.expectedPages()) {
            throw new IllegalArgumentException("expectedPages differs from supplied pages");
        }
        validatePageIdentities(job, result);
        Map<String, GenerationPage> byId = result.stream().collect(
                java.util.stream.Collectors.toUnmodifiableMap(
                        GenerationPage::pageId, page -> page));
        long finalizers = result.stream()
                .filter(page -> page.kind() == GenerationPageKind.GENERATION_FINALIZE)
                .count();
        if (finalizers > 1) {
            throw new IllegalArgumentException("artifact DAG has multiple generation finalizers");
        }
        result.forEach(page -> validateDependencies(page, byId));
        return result.stream().sorted(
                Comparator.comparingInt(GenerationPage::pageOrdinal)).toList();
    }

    private static void validatePageIdentities(
            GenerationJob job, List<GenerationPage> pages) {
        Set<String> pageIds = new HashSet<>();
        Set<Integer> ordinals = new HashSet<>();
        Set<String> partitions = new HashSet<>();
        for (GenerationPage page : pages) {
            if (!job.jobId().equals(page.jobId()) || !pageIds.add(page.pageId())
                    || !ordinals.add(page.pageOrdinal())
                    || !partitions.add(page.stageId() + ":" + page.partitionId())) {
                throw new IllegalArgumentException(
                        "artifact pages have duplicate or foreign identity");
            }
        }
    }

    private static void validateDependencies(
            GenerationPage page, Map<String, GenerationPage> byId) {
        if (page.dependencyPageIds().size() > MAX_DEPENDENCY_FAN_IN) {
            throw new IllegalArgumentException("artifact dependency fan-in exceeds 4096");
        }
        for (String dependencyId : page.dependencyPageIds()) {
            GenerationPage dependency = byId.get(dependencyId);
            if (dependency == null || dependency.pageOrdinal() >= page.pageOrdinal()
                    || dependency.stageId() == page.stageId()
                    || dependency.kind() == GenerationPageKind.GENERATION_FINALIZE) {
                throw new IllegalArgumentException(
                        "artifact page dependency violates DAG ordering");
            }
        }
    }

    static boolean sameIdentity(GenerationJob left, GenerationJob right) {
        return sameCoreIdentity(left, right) && sameFormatIdentity(left, right)
                && left.tenantId().equals(right.tenantId())
                && left.executionKind() == right.executionKind()
                && left.dependencySet().equals(right.dependencySet())
                && left.emptyPartitionPolicy() == right.emptyPartitionPolicy()
                && left.encryptionKeyRef().equals(right.encryptionKeyRef())
                && left.idempotencyKey().equals(right.idempotencyKey());
    }

    private static boolean sameCoreIdentity(GenerationJob left, GenerationJob right) {
        return left.jobId().equals(right.jobId())
                && left.artifactId().equals(right.artifactId())
                && left.artifactType().equals(right.artifactType())
                && left.stagingGeneration().equals(right.stagingGeneration())
                && left.sourceId().equals(right.sourceId())
                && left.sourceSnapshot().equals(right.sourceSnapshot())
                && left.stagingUri().equals(right.stagingUri());
    }

    private static boolean sameFormatIdentity(GenerationJob left, GenerationJob right) {
        return left.formatId().equals(right.formatId())
                && left.formatMajor() == right.formatMajor()
                && left.formatMinor() == right.formatMinor()
                && left.schemaSha256().equalsIgnoreCase(right.schemaSha256())
                && left.writerBuildId().equals(right.writerBuildId());
    }

}
