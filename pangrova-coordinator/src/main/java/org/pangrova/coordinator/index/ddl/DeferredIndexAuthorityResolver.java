package org.pangrova.coordinator.index.ddl;

import org.pangrova.metadata.artifact.build.ArtifactBuildStore;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.security.AccessDeniedException;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Resolves a replacement's real persisted subject under current role, tenant and permission authority. */
final class DeferredIndexAuthorityResolver {
    private DeferredIndexAuthorityResolver() {}

    static Resolved resolve(ArtifactBuildStore store, IndexDescriptor previous, IndexDescriptor replacement) {
        if (!previous.indexId().equals(replacement.indexId())
                || !previous.tenantId().equals(replacement.tenantId())
                || replacement.generation() != Math.addExact(previous.generation(), 1)) {
            throw new AccessDeniedException("Replacement index scope differs from its predecessor");
        }
        var resolved = resolveExisting(store, previous);
        var table = new QualifiedTableName(
                replacement.catalogName(), replacement.schemaName(), replacement.tableName());
        var expected = DeferredAuthorizationScope.raw(table, Set.copyOf(replacement.columnNames()), table);
        var previousScope = DeferredAuthorizationScope.raw(resolved.table(), Set.copyOf(previous.columnNames()),
                resolved.table());
        if (!previousScope.equals(expected)) {
            throw new AccessDeniedException("Replacement cannot expand the original index authorization scope");
        }
        return resolved;
    }

    static Resolved resolveExisting(ArtifactBuildStore store, IndexDescriptor descriptor) {
        String tenant = descriptor.tenantId();
        var authority = store.loadAuthorizedDispatchSpec(tenant, DurableIndexBuildService.jobId(tenant, descriptor));
        var table = new QualifiedTableName(descriptor.catalogName(), descriptor.schemaName(), descriptor.tableName());
        var expected = DeferredAuthorizationScope.raw(table, Set.copyOf(descriptor.columnNames()), table);
        if (!authority.authorizationScope().equals(expected)) {
            throw new AccessDeniedException("Index metadata differs from its persisted exact authorization scope");
        }
        return new Resolved(tenant, table, new DeferredIndexSession(authority.principal(), tenant, table));
    }

    static Resolved resolveScalar(IndexDescriptor descriptor, String principal) {
        var table = new QualifiedTableName(
                descriptor.catalogName(), descriptor.schemaName(), descriptor.tableName());
        return new Resolved(descriptor.tenantId(), table,
                new DeferredIndexSession(principal, descriptor.tenantId(), table));
    }

    record Resolved(String tenantId, QualifiedTableName table, ConnectorSession session) {}

    static String tenantScope(String tenantId) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(tenantId.getBytes(java.nio.charset.StandardCharsets.UTF_8)), 0, 16);
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
    }

    private record DeferredIndexSession(String principal, String tenantId, QualifiedTableName table)
            implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override
        public String getUser() { return principal; }

        @Override
        public Optional<String> getCatalog() { return Optional.of(table.catalog()); }

        @Override
        public Optional<String> getSchema() { return Optional.of(table.schema()); }

        @Override
        public Map<String, String> getProperties() {
            return Map.of("user", principal, "tenant_id", tenantId,
                    "catalog", table.catalog(), "schema", table.schema());
        }
    }
}
