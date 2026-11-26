package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.generation.api.job.GenerationSubmissionAuthority;
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

    static Resolved resolve(GenerationJobStore store, IndexMetadataStore indexes,
                            IndexDescriptor previous, IndexDescriptor replacement) {
        if (!previous.indexId().equals(replacement.indexId())
                || !previous.tenantId().equals(replacement.tenantId())
                || replacement.generation() != Math.addExact(previous.generation(), 1)) {
            throw new AccessDeniedException("Replacement index scope differs from its predecessor");
        }
        var resolved = resolveExisting(store, indexes, previous);
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

    static Resolved resolveExisting(GenerationJobStore store, IndexMetadataStore indexes, IndexDescriptor descriptor) {
        String tenant = descriptor.tenantId();
        // Prefer the exact immutable published attempt, never an arbitrary failed generation sibling.
        String jobId = store.findPublishedGeneration(tenant, descriptor.indexId(),
                Integer.toString(descriptor.generation()), descriptor.buildSnapshotId())
                .map(manifest -> manifest.jobId())
                .orElseGet(() -> indexes.findIndexGenerationSubmissionAttempt(
                                descriptor.indexId(), descriptor.generation(), descriptor.buildSnapshotId())
                        .map(attempt -> DurableIndexBuildService.jobId(tenant, descriptor, attempt))
                        .orElseThrow(() -> new AccessDeniedException("Exact Native submission authority is absent")));
        var authority = store.loadAuthorizedDispatchSpec(tenant, jobId);
        var table = new QualifiedTableName(descriptor.catalogName(), descriptor.schemaName(), descriptor.tableName());
        var expected = DeferredAuthorizationScope.raw(table, Set.copyOf(descriptor.columnNames()), table);
        if (!authority.authorizationScope().equals(expected)) {
            throw new AccessDeniedException("Index metadata differs from its persisted exact authorization scope");
        }
        return new Resolved(tenant, table, new DeferredIndexSession(authority.principal(), tenant, table));
    }

    static Resolved resolveSubmission(GenerationSubmissionAuthority authority) {
        var table = authority.authorizationScope().permissions().stream()
                .filter(permission -> permission.privilege() == org.pangrova.security.api.enums.Privilege.SELECT)
                .map(permission -> permission.table())
                .findFirst().orElseThrow(() -> new AccessDeniedException(
                        "Snapshot maintenance has no exact persisted source scope"));
        return new Resolved(authority.tenantId(), table,
                new DeferredIndexSession(authority.principal(), authority.tenantId(), table));
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
