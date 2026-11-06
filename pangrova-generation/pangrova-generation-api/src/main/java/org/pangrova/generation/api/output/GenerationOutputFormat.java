package org.pangrova.generation.api.output;

import java.util.Objects;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import org.pangrova.generation.api.execution.GenerationPageKind;

/** Immutable role layout for every page kind of one current format. */
public record GenerationOutputFormat(
        String artifactType,
        Map<String, GenerationOutputRole> sourceMapRoles,
        Map<String, GenerationOutputRole> dependencyReduceRoles,
        Map<String, GenerationOutputRole> generationFinalizeRoles,
        Map<String, GenerationOutputRole> relationalPartitionRoles,
        boolean supportsEmptyPartitions) {

    public GenerationOutputFormat {
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
    public Map<String, GenerationOutputRole> rolesFor(GenerationPageKind kind) {
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
            Map<String, GenerationOutputRole> roles,
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

