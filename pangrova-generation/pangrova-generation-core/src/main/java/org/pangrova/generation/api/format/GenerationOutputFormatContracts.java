package org.pangrova.generation.api.format;

import org.pangrova.generation.api.GenerationOutputScope;
import org.pangrova.generation.api.GenerationPageKind;
import org.pangrova.generation.api.GenerationOutputTypeIds;

import org.pangrova.common.api.annotation.Internal;

import java.util.Map;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;

/** Executable work-page, scope and role contracts for immutable artifacts. */
@Internal
public final class GenerationOutputFormatContracts {

    private static final Map<String, FormatContract> CONTRACTS = Map.of(
            "pangrova.diskann.segment", new FormatContract(
                    GenerationOutputTypeIds.VECTOR_INDEX,
                    Map.of(
                            "DISKANN_GRAPH", source(0),
                            "RAW_VECTORS", source(1),
                            "ROW_LOCATORS", source(2)),
                    Map.of(),
                    Map.of(), Map.of(), true),
            "pangrova.hnsw.segment", vectorContract("HNSW_GRAPH"),
            "pangrova.ivfpq.segment", vectorContract("IVF_PQ_INDEX"),
            "pangrova.bm25.segment", new FormatContract(
                    GenerationOutputTypeIds.TEXT_INDEX,
                    Map.of(
                            "BM25_POSTINGS", source(0),
                            "ROW_LOCATORS", source(1),
                            "BM25_STATS_SHARD", workUnit(2)),
                    Map.of("BM25_STATS_SHARD", workUnit(0)),
                    Map.of("BM25_CORPUS_STATS", generation(0)), Map.of(), true),
            "pangrova.generated-values.segment", new FormatContract(
                    GenerationOutputTypeIds.GENERATED_COLUMN_VALUES,
                    Map.of(
                            "GENERATED_VALUES", source(0),
                            "ROW_LOCATORS", source(1)),
                    Map.of(),
                    Map.of(), Map.of(), false),
            "pangrova.tabular-export.arrow-stream", new FormatContract(
                    GenerationOutputTypeIds.TABULAR_EXPORT,
                    Map.of(),
                    Map.of(),
                    Map.of(), Map.of("TABULAR_DATA", workUnit(0)), true));

    private GenerationOutputFormatContracts() {}

    /** Returns the exact current contract or fails closed for an unknown format. */
    public static FormatContract require(String formatId, String artifactType) {
        FormatContract contract = CONTRACTS.get(formatId);
        if (contract == null || !contract.artifactType().equals(artifactType)) {
            throw new IllegalStateException(
                    "artifact type and format have no executable contract");
        }
        return contract;
    }

    private static RoleContract source(int ordinal) {
        return new RoleContract(ordinal, GenerationOutputScope.SOURCE_OBJECT);
    }

    private static RoleContract workUnit(int ordinal) {
        return new RoleContract(ordinal, GenerationOutputScope.WORK_UNIT);
    }

    private static RoleContract generation(int ordinal) {
        return new RoleContract(ordinal, GenerationOutputScope.GENERATION);
    }

    private static FormatContract vectorContract(String indexRole) {
        return new FormatContract(
                GenerationOutputTypeIds.VECTOR_INDEX,
                Map.of(
                        indexRole, source(0),
                        "RAW_VECTORS", source(1),
                        "ROW_LOCATORS", source(2)),
                Map.of(),
                Map.of(), Map.of(), true);
    }

    /** Immutable role layout for every page kind of one current format. */
    public record FormatContract(
            String artifactType,
            Map<String, RoleContract> sourceMapRoles,
            Map<String, RoleContract> dependencyReduceRoles,
            Map<String, RoleContract> generationFinalizeRoles,
            Map<String, RoleContract> relationalPartitionRoles,
            boolean supportsEmptyPartitions) {

        public FormatContract {
            artifactType = Objects.requireNonNull(
                    artifactType, "artifactType must not be null");
            sourceMapRoles = Map.copyOf(sourceMapRoles);
            dependencyReduceRoles = Map.copyOf(dependencyReduceRoles);
            generationFinalizeRoles = Map.copyOf(generationFinalizeRoles);
            relationalPartitionRoles = Map.copyOf(relationalPartitionRoles);
            if (artifactType.isBlank() || (sourceMapRoles.isEmpty() == relationalPartitionRoles.isEmpty())
                    || dependencyReduceRoles.isEmpty()
                    != generationFinalizeRoles.isEmpty()) {
                throw new IllegalArgumentException(
                        "artifact format work-page contract must be complete");
            }
            validateRoles(sourceMapRoles, Set.of(
                    GenerationOutputScope.SOURCE_OBJECT,
                    GenerationOutputScope.WORK_UNIT));
            validateRoles(dependencyReduceRoles, Set.of(
                    GenerationOutputScope.WORK_UNIT));
            validateRoles(generationFinalizeRoles, Set.of(
                    GenerationOutputScope.GENERATION));
            validateRoles(relationalPartitionRoles, Set.of(GenerationOutputScope.WORK_UNIT));
            if (!sourceMapRoles.isEmpty() && sourceMapRoles.values().stream().noneMatch(
                    role -> role.scope() == GenerationOutputScope.SOURCE_OBJECT)) {
                throw new IllegalArgumentException(
                        "source-map contract must publish source-scoped objects");
            }
        }

        /** Returns the allowed object roles for one page kind. */
        public Map<String, RoleContract> rolesFor(GenerationPageKind kind) {
            return switch (kind) {
                case SOURCE_MAP -> sourceMapRoles;
                case DEPENDENCY_REDUCE -> dependencyReduceRoles;
                case GENERATION_FINALIZE -> generationFinalizeRoles;
                case RELATIONAL_PARTITION -> relationalPartitionRoles;
                case RELATIONAL_COMPUTE -> Map.of();
            };
        }

        public boolean requiresGenerationFinalize() {
            return !generationFinalizeRoles.isEmpty();
        }

        private static void validateRoles(
                Map<String, RoleContract> roles,
                Set<GenerationOutputScope> allowedScopes) {
            Set<Integer> ordinals = new HashSet<>();
            roles.forEach((name, role) -> {
                if (name == null || name.isBlank() || role == null
                        || !allowedScopes.contains(role.scope())
                        || !ordinals.add(role.ordinal())) {
                    throw new IllegalArgumentException(
                            "artifact role contract has an invalid identity or scope");
                }
            });
        }
    }

    /** Required ordinal and distribution scope for one object role. */
    public record RoleContract(int ordinal, GenerationOutputScope scope) {
        public RoleContract {
            Objects.requireNonNull(scope, "scope must not be null");
            if (ordinal < 0) {
                throw new IllegalArgumentException("role ordinal must not be negative");
            }
        }
    }
}
