package org.pangrova.security.api.authorization;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.pangrova.security.api.enums.Privilege;

/** Exact resources and observed transformations authorized for replayable background work. */
@Experimental
public record DeferredAuthorizationScope(
        List<TablePermission> permissions, Optional<DefinitionSnapshot> definitionSnapshot) {
    /** Maximum exact read columns carried by one deferred table permission. */
    public static final int MAX_COLUMNS = 4096;

    /** Rejects unscoped work and defensively copies all requirements. */
    public DeferredAuthorizationScope {
        permissions = List.copyOf(permissions);
        definitionSnapshot = Objects.requireNonNull(definitionSnapshot, "definitionSnapshot");
        if (permissions.isEmpty() || permissions.size() > 256) {
            throw new IllegalArgumentException("Deferred work requires exact resource permissions");
        }
        if (definitionSnapshot.isEmpty() && (permissions.stream()
                .noneMatch(permission -> permission.privilege() == Privilege.SELECT)
                || permissions.stream().noneMatch(permission -> permission.privilege() == Privilege.ALTER))) {
            throw new IllegalArgumentException("Deferred work requires both source SELECT and target ALTER");
        }
        if (definitionSnapshot.isPresent()) {
            validateWritePermissions(permissions, definitionSnapshot.orElseThrow());
        }
    }

    /** Creates exact raw-read and target-alter requirements; applicable policies will reject raw work. */
    public static DeferredAuthorizationScope raw(
            QualifiedTableName source, Set<String> columns, QualifiedTableName target) {
        return new DeferredAuthorizationScope(List.of(
                new TablePermission(source, columns, Privilege.SELECT, ReadMode.RAW_READ, PolicySnapshot.empty()),
                new TablePermission(target, Set.of(), Privilege.ALTER, ReadMode.NONE, PolicySnapshot.empty())),
                Optional.empty());
    }

    private static void validateWritePermissions(List<TablePermission> permissions, DefinitionSnapshot snapshot) {
        boolean writePermission = permissions.stream().anyMatch(permission -> permission.table().equals(
                snapshot.declaration().table())
                && Set.of(Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE).contains(permission.privilege()));
        if (!writePermission) {
            throw new IllegalArgumentException("Write receipt requires the original target write permission");
        }
        if (snapshot.declaration().hasGeneratedColumns() && permissions.stream()
                .noneMatch(permission -> permission.privilege() == Privilege.SELECT)) {
            throw new IllegalArgumentException("Generated write receipt requires explicit source SELECT");
        }
    }

    /** Actual connector definition and exact model deployments admitted before a write starts. */
    public record DefinitionSnapshot(
            WriteDeclarationSnapshot declaration, List<ModelSnapshot> models) {
        /** Validates content digests; no cached revision is treated as external connector authority. */
        public DefinitionSnapshot {
            Objects.requireNonNull(declaration, "declaration");
            models = List.copyOf(models.stream()
                    .sorted(java.util.Comparator.comparing(ModelSnapshot::logicalName)).toList());
            if (models.size() > 256 || (!declaration.hasGeneratedColumns() && !models.isEmpty())) {
                throw new IllegalArgumentException("Model contracts must belong to bounded generated definitions");
            }
            if (declaration.hasGeneratedColumns() && models.isEmpty()) {
                throw new IllegalArgumentException("Generated definitions require exact model contracts");
            }
            if (models.stream().map(ModelSnapshot::logicalName).distinct().count() != models.size()) {
                throw new IllegalArgumentException("Model contracts require unique logical names");
            }
        }
    }

    /** Immutable logical model incarnation, deployment version and implementation signature. */
    public record ModelSnapshot(String logicalName, String incarnation, int configurationVersion,
                                String deploymentVersion, String signature) {
        /** Rejects unspecified model contracts. */
        public ModelSnapshot {
            TablePermission.requireText(logicalName);
            TablePermission.requireText(incarnation);
            TablePermission.requireText(deploymentVersion);
            requireDigest(signature);
            if (configurationVersion < 0) {
                throw new IllegalArgumentException("Model configuration version must be nonnegative");
            }
        }
    }

    private static void requireDigest(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Definition content requires a canonical SHA-256 digest");
        }
    }

    /** Distinguishes raw physical reads from a plan containing observed security transformations. */
    public enum ReadMode { RAW_READ, SECURITY_TRANSFORMED, NONE }

    /** Actual bound expressions observed by secure compilation, never an invented policy epoch. */
    public record PolicySnapshot(Optional<String> rowFilter, Map<String, Optional<String>> columnMasks) {
        /** Copies immutable policy content. An empty row filter means no applicable policy. */
        public PolicySnapshot {
            rowFilter = Objects.requireNonNull(rowFilter, "rowFilter");
            columnMasks = Map.copyOf(columnMasks);
        }

        /** Returns a snapshot with no applicable transformations. */
        public static PolicySnapshot empty() {
            return new PolicySnapshot(Optional.empty(), Map.of());
        }
    }

    /** One exact table action, with the complete columns read including security dependencies. */
    public record TablePermission(
            QualifiedTableName table, Set<String> columns, Privilege privilege,
            ReadMode readMode, PolicySnapshot observedPolicy) {
        /** Validates that every read declares columns and its transformation semantics. */
        public TablePermission {
            Objects.requireNonNull(table, "table");
            requireText(table.catalog());
            requireText(table.schema());
            requireText(table.table());
            columns = Set.copyOf(columns);
            if (columns.size() > MAX_COLUMNS) {
                throw new IllegalArgumentException("Deferred read columns exceed the bounded scope limit");
            }
            columns.forEach(TablePermission::requireText);
            Objects.requireNonNull(privilege, "privilege");
            Objects.requireNonNull(readMode, "readMode");
            Objects.requireNonNull(observedPolicy, "observedPolicy");
            validateReadMode(columns, privilege, readMode, observedPolicy);
        }

        private static void validateReadMode(Set<String> columns, Privilege privilege,
                                             ReadMode readMode, PolicySnapshot observedPolicy) {
            if ((privilege == Privilege.SELECT) != (readMode != ReadMode.NONE)
                    || (readMode != ReadMode.NONE && columns.isEmpty())) {
                throw new IllegalArgumentException("Deferred reads require explicit SELECT columns and read mode");
            }
            validateObservedPolicies(columns, readMode, observedPolicy);
        }

        private static void validateObservedPolicies(Set<String> columns, ReadMode readMode,
                                                     PolicySnapshot observedPolicy) {
            if (readMode != ReadMode.SECURITY_TRANSFORMED
                    && !observedPolicy.equals(PolicySnapshot.empty())) {
                throw new IllegalArgumentException("Only transformed reads may retain observed policy content");
            }
            if (!columns.containsAll(observedPolicy.columnMasks().keySet())) {
                throw new IllegalArgumentException("Observed masks must belong to declared read columns");
            }
            if (readMode == ReadMode.SECURITY_TRANSFORMED
                    && !columns.equals(observedPolicy.columnMasks().keySet())) {
                throw new IllegalArgumentException("Transformed reads require an observation for every read column");
            }
        }

        private static void requireText(String value) {
            if (value == null || value.isBlank() || value.equals("*")) {
                throw new IllegalArgumentException("Deferred authorization requires exact nonblank identifiers");
            }
        }
    }
}
