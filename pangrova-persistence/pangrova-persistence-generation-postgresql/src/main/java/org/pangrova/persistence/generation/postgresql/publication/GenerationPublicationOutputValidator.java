package org.pangrova.persistence.generation.postgresql.publication;

import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.codec.format.BuiltInGenerationOutputFormats;
import org.pangrova.generation.api.output.GenerationOutputFormat;
import org.pangrova.generation.api.output.GenerationOutputRole;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Validates complete artifact output bundles before atomic publication. */
final class GenerationPublicationOutputValidator {

    private GenerationPublicationOutputValidator() {}

    static List<GenerationOutput> loadAndValidate(
            Connection connection, GenerationPublicationJob job, GenerationStageGraph graph)
            throws SQLException {
        if (job.completedPages() != job.expectedPages()) {
            throw new IllegalStateException("Artifact job has not crossed the all-pages barrier");
        }
        boolean relational = job.artifactType().equals(
                org.pangrova.generation.api.constants.GenerationOutputTypeIds.TABULAR_EXPORT);
        Map<Integer, StagePlan> stages = relational ? relationalStages(graph) : Map.of();
        List<PageOutput> outputs = loadCompletedOutputs(connection, job, stages);
        if (job.expectedPages() == 0) {
            if (!outputs.isEmpty()) {
                throw new IllegalStateException("Empty artifact generation unexpectedly has outputs");
            }
            if (job.emptyPartitionPolicy()
                    != org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS
                    || !BuiltInGenerationOutputFormats.require(job.formatId(), job.artifactType())
                            .supportsEmptyPartitions()) {
                throw new IllegalStateException(
                        "Empty artifact generation is forbidden by job or format policy");
            }
            return List.of();
        }
        if (outputs.isEmpty()) {
            throw new IllegalStateException("Complete artifact generation has no objects");
        }
        validateWorkPages(connection, job, outputs, relational);
        return outputs.stream().map(PageOutput::output)
                .filter(output -> relational || output.scope() != GenerationOutputScope.WORK_UNIT)
                .toList();
    }

    private static Map<Integer, StagePlan> relationalStages(GenerationStageGraph graph) {
        if (graph.purpose() != GenerationGraphPurpose.RELATIONAL_EXPORT) {
            throw new IllegalStateException("Tabular export requires the relational terminal purpose");
        }
        return graph.stages().stream().collect(Collectors.toUnmodifiableMap(StagePlan::stageId, Function.identity()));
    }

    private static List<PageOutput> loadCompletedOutputs(
            Connection connection, GenerationPublicationJob job, Map<Integer, StagePlan> stages)
            throws SQLException {
        List<PageOutput> outputs = new ArrayList<>();
        int completed = 0;
        String previousPage = null;
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT p.page_id,p.page_kind,o.object_id,o.opaque_uri,o.object_role,
                       o.source_object_id,o.object_ordinal,o.object_scope,o.size_bytes,
                       o.row_count,o.checksum_sha256,o.encryption_key_ref,p.state,
                       p.stage_id,p.partition_id
                FROM artifact_build_pages p LEFT JOIN artifact_build_outputs o
                  ON p.tenant_id=o.tenant_id AND p.job_id=o.job_id AND p.page_id=o.page_id
                WHERE p.tenant_id=? AND p.job_id=?
                ORDER BY p.page_id,o.object_ordinal,o.object_id
                """)) {
            statement.setString(1, job.tenantId());
            statement.setString(2, job.jobId());
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    String pageId = rows.getString("page_id");
                    boolean hasOutput = rows.getString("object_id") != null;
                    if (!pageId.equals(previousPage)) {
                        validateCompletedPage(rows, stages, hasOutput);
                        completed++;
                        previousPage = pageId;
                    }
                    if (hasOutput) {
                        outputs.add(mapOutput(rows));
                    }
                }
            }
        }
        if (completed != job.expectedPages()) {
            throw new IllegalStateException("Artifact page states differ from the completion barrier");
        }
        return outputs;
    }

    private static void validateCompletedPage(
            ResultSet page, Map<Integer, StagePlan> stages, boolean hasOutput) throws SQLException {
        if (!"SUCCEEDED".equals(page.getString("state"))) {
            throw new IllegalStateException("Artifact page states differ from the completion barrier");
        }
        if (stages.isEmpty()) {
            return;
        }
        StagePlan stage = stages.get(page.getInt("stage_id"));
        int partition = page.getInt("partition_id");
        if (stage == null || !stage.pageKind().name().equals(page.getString("page_kind"))
                || partition < 0 || partition >= stage.parallelism()) {
            throw new IllegalStateException("Export page differs from its persisted stage allocation");
        }
        if (stage.publicationOutput() != hasOutput) {
            throw new IllegalStateException("Export output coverage differs from its terminal partition cohort");
        }
    }

    private static PageOutput mapOutput(ResultSet result) throws SQLException {
        int column = 1;
        String pageId = result.getString(column++);
        GenerationPageKind kind = GenerationPageKind.valueOf(result.getString(column++));
        GenerationOutput output = new GenerationOutput(
                result.getString(column++), result.getString(column++),
                result.getString(column++), result.getString(column++),
                result.getInt(column++), GenerationOutputScope.valueOf(result.getString(column++)),
                result.getLong(column++), result.getLong(column++),
                result.getString(column++), result.getString(column));
        return new PageOutput(pageId, kind, output);
    }

    private static void validateWorkPages(
            Connection connection, GenerationPublicationJob job,
            List<PageOutput> outputs, boolean relational) throws SQLException {
        GenerationOutputFormat contract = BuiltInGenerationOutputFormats.require(
                job.formatId(), job.artifactType());
        Map<String, List<PageOutput>> pages = outputs.stream().collect(
                Collectors.groupingBy(PageOutput::pageId, LinkedHashMap::new, Collectors.toList()));
        if (relational) {
            for (List<PageOutput> pageOutputs : pages.values()) {
                validatePageOutputs(job, pageOutputs.get(0), null, pageOutputs, contract);
            }
            return;
        }
        if (pages.size() != job.expectedPages()) {
            throw new IllegalStateException("Artifact work pages have missing outputs");
        }
        Map<String, String> sourcePages = loadSourcePages(
                connection, job.tenantId(), job.jobId());
        long finalizers = pages.values().stream()
                .filter(page -> page.get(0).kind() == GenerationPageKind.GENERATION_FINALIZE)
                .count();
        for (List<PageOutput> pageOutputs : pages.values()) {
            PageOutput first = pageOutputs.get(0);
            validatePageOutputs(job, first, sourcePages.get(first.pageId()), pageOutputs, contract);
        }
        long requiredFinalizers = contract.requiresGenerationFinalize() ? 1 : 0;
        if (finalizers != requiredFinalizers) {
            throw new IllegalStateException(
                    "Artifact generation finalizer count differs from its format contract");
        }
        validatePublishedBundles(job, sourcePages, outputs, contract);
    }

    private static void validatePageOutputs(
            GenerationPublicationJob job, PageOutput page,
            String sourceObjectId, List<PageOutput> outputs, GenerationOutputFormat contract) {
        Map<String, GenerationOutputRole> expectedRoles = contract.rolesFor(page.kind());
        Map<String, GenerationOutput> actualRoles = new HashMap<>();
        for (PageOutput pageOutput : outputs) {
            validatePageOutput(job, page, sourceObjectId, pageOutput, expectedRoles, actualRoles);
        }
        if (!actualRoles.keySet().equals(expectedRoles.keySet())) {
            throw new IllegalStateException("Artifact work-page roles are incomplete");
        }
    }

    private static void validatePageOutput(
            GenerationPublicationJob job, PageOutput page,
            String sourceObjectId, PageOutput pageOutput,
            Map<String, GenerationOutputRole> expectedRoles,
            Map<String, GenerationOutput> actualRoles) {
        GenerationOutput output = pageOutput.output();
        GenerationOutputRole role = expectedRoles.get(output.objectRole());
        if (!matchesRole(page, pageOutput, output, role)
                || actualRoles.put(output.objectRole(), output) != null) {
            throw new IllegalStateException("Artifact work-page roles are invalid");
        }
        if (!output.encryptionKeyRef().equals(job.encryptionKeyRef())) {
            throw new IllegalStateException("Artifact encryption key differs within a generation");
        }
        if (output.rowCount() == 0
                && (job.emptyPartitionPolicy()
                        != org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy
                                .ALLOW_ZERO_ROWS
                        || !BuiltInGenerationOutputFormats.require(
                                job.formatId(), job.artifactType())
                                .supportsEmptyPartitions())) {
            throw new IllegalStateException(
                    "Zero-row artifact output is forbidden by job or format policy");
        }
        String expectedScopeId = switch (output.scope()) {
            case SOURCE_OBJECT -> sourceObjectId;
            case WORK_UNIT -> page.pageId();
            case GENERATION -> job.generation();
        };
        if (expectedScopeId == null || !expectedScopeId.equals(output.sourceObjectId())) {
            throw new IllegalStateException("Artifact output scope identity is invalid");
        }
    }

    private static boolean matchesRole(
            PageOutput page, PageOutput pageOutput,
            GenerationOutput output, GenerationOutputRole role) {
        return pageOutput.kind() == page.kind() && role != null
                && output.ordinal() == role.ordinal() && output.scope() == role.scope();
    }

    private static Map<String, String> loadSourcePages(
            Connection connection, String tenantId, String jobId)
            throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        Set<String> sourceIds = new java.util.HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT page_id,source_object_id FROM artifact_build_page_sources "
                        + "WHERE tenant_id=? AND job_id=? ORDER BY page_id,source_ordinal")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    String pageId = rows.getString(1);
                    String sourceId = rows.getString(2);
                    if (result.put(pageId, sourceId) != null || !sourceIds.add(sourceId)) {
                        throw new IllegalStateException(
                                "Artifact source object is assigned to multiple work pages");
                    }
                }
            }
        }
        return Map.copyOf(result);
    }

    private static void validatePublishedBundles(
            GenerationPublicationJob job, Map<String, String> sourcePages,
            List<PageOutput> outputs, GenerationOutputFormat contract) {
        Map<String, GenerationOutputRole> sourceRoles = contract.sourceMapRoles().entrySet().stream()
                .filter(entry -> entry.getValue().scope() == GenerationOutputScope.SOURCE_OBJECT)
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, Map.Entry::getValue));
        PublishedBundles published = collectBundles(outputs);
        if (!published.sources().keySet().equals(Set.copyOf(sourcePages.values()))) {
            throw new IllegalStateException("Artifact source coverage differs from its plan");
        }
        published.sources().values().forEach(bundle -> validateSourceBundle(bundle, sourceRoles));
        if (!published.generation().keySet().equals(contract.generationFinalizeRoles().keySet())) {
            throw new IllegalStateException("Published generation roles are incomplete");
        }
        long rows = published.sources().values().stream()
                .mapToLong(bundle -> bundle.values().iterator().next().rowCount())
                .reduce(0L, Math::addExact);
        if (published.generation().values().stream().anyMatch(output -> output.rowCount() != rows)) {
            throw new IllegalStateException(
                    "Generation artifact row count differs from source coverage");
        }
    }

    private static PublishedBundles collectBundles(List<PageOutput> outputs) {
        Map<String, Map<String, GenerationOutput>> sources = new LinkedHashMap<>();
        Map<String, GenerationOutput> generation = new HashMap<>();
        for (PageOutput pageOutput : outputs) {
            GenerationOutput output = pageOutput.output();
            if (output.scope() == GenerationOutputScope.SOURCE_OBJECT) {
                GenerationOutput previous = sources.computeIfAbsent(
                        output.sourceObjectId(), ignored -> new HashMap<>())
                        .put(output.objectRole(), output);
                if (previous != null) {
                    throw new IllegalStateException("Duplicate published source role");
                }
            } else if (output.scope() == GenerationOutputScope.GENERATION
                    && generation.put(output.objectRole(), output) != null) {
                throw new IllegalStateException("Duplicate generation artifact role");
            }
        }
        return new PublishedBundles(sources, generation);
    }

    private static void validateSourceBundle(
            Map<String, GenerationOutput> bundle, Map<String, GenerationOutputRole> sourceRoles) {
        if (!bundle.keySet().equals(sourceRoles.keySet())) {
            throw new IllegalStateException("Published source roles are incomplete");
        }
        long rows = bundle.values().iterator().next().rowCount();
        for (Map.Entry<String, GenerationOutput> entry : bundle.entrySet()) {
            if (entry.getValue().ordinal() != sourceRoles.get(entry.getKey()).ordinal()
                    || entry.getValue().rowCount() != rows) {
                throw new IllegalStateException(
                        "Published source role ordinal or row count mismatch");
            }
        }
    }

    private record PageOutput(
            String pageId, GenerationPageKind kind, GenerationOutput output) {}

    private record PublishedBundles(
            Map<String, Map<String, GenerationOutput>> sources,
            Map<String, GenerationOutput> generation) {}
}
