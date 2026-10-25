package org.pangrova.testing.e2e;

import org.pangrova.coordinator.ai.embedding.DurableGeneratedValueBuildService;
import org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry;
import org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator;
import org.pangrova.coordinator.artifact.SnapshotArtifactSplitPlanner;
import org.pangrova.coordinator.query.execution.dispatch.dag.DefaultWorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.SpillBudget;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.artifact.build.ArtifactBuildStore;
import org.pangrova.metadata.artifact.jdbc.JdbcGeneratedValueArtifactPublicationStore;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.store.PersistentColumnMaskStore;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.store.PersistentRowPolicyStore;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.spi.coordination.WriteCommitJournal;
import org.pangrova.spi.security.AccessControl;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Explicit PostgreSQL authority and production write-receipt wiring for authenticated SQL fixtures. */
final class E2eCurrentAuthority {
    static final String MODEL_ADMIN_ROLE = "e2e-model-admin";
    static final String MODEL_USAGE_ROLE = "e2e-model-invoker";
    private static final String SQL_ROLE = "e2e-sql-fixture";

    private E2eCurrentAuthority() {}

    static JdbcTenantStore tenants(DataSource dataSource, String tenantId, String principal,
                                  ResourceQuota deploymentQuota) {
        java.util.Objects.requireNonNull(deploymentQuota, "deploymentQuota");
        var store = new JdbcTenantStore(dataSource);
        store.initSchema();
        if (store.getTenant(tenantId).isEmpty()) {
            store.createTenant(new Tenant(tenantId, "Authenticated SQL E2E tenant", TenantStatus.ACTIVE,
                    deploymentQuota, List.of(), List.of(), "e2e", Map.of(), Instant.EPOCH, principal));
        }
        if (store.getTenant(tenantId).orElseThrow().status() != TenantStatus.ACTIVE) {
            throw new IllegalStateException("Fixture tenant is not ACTIVE");
        }
        var membership = store.getTenantForUser(principal);
        if (membership.isEmpty()) {
            store.addUserToTenant(tenantId, principal, TenantRole.OWNER);
        } else if (!membership.orElseThrow().tenant().tenantId().equals(tenantId)) {
            throw new IllegalStateException("Fixture principal belongs to a different tenant");
        }
        return store;
    }

    static AccessControl provisionDefault(DataSource dataSource, String principal, CatalogRegistry catalogs) {
        var roles = roles(dataSource);
        roles.createRole(Role.of(SQL_ROLE));
        roles.grantRoleToUser(UserRoleAssignment.of(principal, SQL_ROLE));
        var grants = new PersistentGrantStore(dataSource);
        if (grants.listGrantsForRole(SQL_ROLE).isEmpty()) {
            grants.addGrant(PrivilegeGrant.of(SQL_ROLE, Privilege.ALL, "TABLE", "*", "*", "*", null));
        }
        if (grants.listGrantsForRole(SQL_ROLE).stream()
                .noneMatch(grant -> grant.privilege() == Privilege.MANAGE_STORAGE_PROFILE
                        && grant.coversStorageProfile("*"))) {
            grants.addGrant(PrivilegeGrant.of(SQL_ROLE, Privilege.MANAGE_STORAGE_PROFILE,
                    "STORAGE_PROFILE", "*", null, null, null));
        }
        return new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(new PersistentRowPolicyStore(dataSource)),
                new ColumnMaskingProcessor(new org.pangrova.metadata.service.TableControlColumnMaskStore(
                        catalogs, new PersistentColumnMaskStore(dataSource))));
    }

    static void removeDefault(DataSource dataSource, String principal) {
        roles(dataSource).revokeRoleFromUser(principal, SQL_ROLE);
    }

    static void provisionModels(DataSource dataSource, String principal) {
        var roles = roles(dataSource);
        roles.createRole(Role.of(MODEL_ADMIN_ROLE));
        roles.createRole(Role.of(MODEL_USAGE_ROLE));
        roles.grantRoleToUser(UserRoleAssignment.of(principal, MODEL_ADMIN_ROLE));
        roles.grantRoleToUser(UserRoleAssignment.of(principal, MODEL_USAGE_ROLE));
    }

    static GeneratedValueMaterializationCoordinator generatedValues(GeneratedServices services) {
        var publications = new JdbcGeneratedValueArtifactPublicationStore(services.dataSource());
        var selector = new DefaultWorkerSelector(services.workers(), services.scheduler(),
                new SplitAssignmentManager(), new SpillBudget(1024L * 1024 * 1024, ""), services.models());
        var builds = new DurableGeneratedValueBuildService(services.builds(), publications,
                new SnapshotArtifactSplitPlanner(services.catalogs()), selector, services.artifactUri(), services.catalogs());
        return new GeneratedValueMaterializationCoordinator(services.catalogs(), builds,
                services.dataSource(), services.journal());
    }

    private static PersistentRoleStore roles(DataSource dataSource) {
        var roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        return roles;
    }

    record GeneratedServices(DataSource dataSource, CatalogRegistry catalogs, ArtifactBuildStore builds,
                             WriteCommitJournal journal, WorkerManager workers, TaskScheduler scheduler,
                             EmbeddingModelRegistry models, String artifactUri) {}
}
