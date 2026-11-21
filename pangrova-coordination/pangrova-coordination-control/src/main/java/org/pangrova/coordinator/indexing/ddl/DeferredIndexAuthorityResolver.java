package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.security.api.exception.AccessDeniedException;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Resolves a replacement's real persisted subject under current role, tenant and permission authority. */
final class DeferredIndexAuthorityResolver {
    private DeferredIndexAuthorityResolver() {}

    static Resolved resolve(GenerationJobStore store, IndexDescriptor previous, IndexDescriptor replacement) {
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

    static Resolved resolveExisting(GenerationJobStore store, IndexDescriptor descriptor) {
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
        return DigestUtils.sha256Hex(tenantId).substring(0, 32);
    }

    private record DeferredIndexSession(String principal, String tenantId, QualifiedTableName table)
            implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
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
