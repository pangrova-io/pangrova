package org.pangrova.testing.integration.e2e;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.pangrova.testing.integration.control.ai.ModelAuthorityFixtures;

import org.pangrova.coordinator.ai.embedding.DurableGeneratedValueBuildService;
import org.pangrova.ai.control.embedding.EmbeddingModelRegistry;
import org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator;
import org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner;
import org.pangrova.coordinator.scheduler.placement.DefaultWorkerSelector;
import org.pangrova.scheduler.api.resource.SpillBudget;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.security.MetadataTableControlPolicyProjections;
import org.pangrova.persistence.workflow.postgresql.security.PostgresTableReadPolicyAuthority;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.security.api.authorization.AccessControl;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        var grants = new JdbcGrantStore(dataSource);
        if (grants.listGrantsForRoles(Set.of(SQL_ROLE)).isEmpty()) {
            grants.addGrant(PrivilegeGrant.of(SQL_ROLE, Privilege.ALL, "TABLE", "*", "*", "*", null));
        }
        if (grants.listGrantsForRoles(Set.of(SQL_ROLE)).stream()
                .noneMatch(grant -> grant.privilege() == Privilege.MANAGE_STORAGE_PROFILE
                        && grant.coversStorageProfile("*"))) {
            grants.addGrant(PrivilegeGrant.of(SQL_ROLE, Privilege.MANAGE_STORAGE_PROFILE,
                    "STORAGE_PROFILE", "*", null, null, null));
        }
        return new DynamicRbacAccessControl(grants, roles,
                new PostgresTableReadPolicyAuthority(dataSource,
                        catalogs::requireLocalDeployment,
                        new MetadataTableControlPolicyProjections(),
                        org.pangrova.security.rowfilter.RowLevelSecurityFilter::renderPolicies,
                        org.pangrova.security.mask.ColumnMaskingProcessor::renderMasks), roles);
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

    /** Exercise the production configuration path before building real Native task grants. */
    static SpillBudget configuredSpillBudget(long bytes) {
        return org.pangrova.server.coordinator.bootstrap.PangrovaCoordinatorMain.buildBootstrapConfig(
                org.pangrova.config.PangrovaConfiguration.fromYaml(java.util.Map.of(
                        "coordinator.task.spill.max-bytes", Long.toString(bytes)))).spillBudget();
    }

    static GeneratedValueMaterializationCoordinator generatedValues(GeneratedServices services) {
        var submissions = ModelAuthorityFixtures.artifacts().generatedSubmissions(services.dataSource());
        var selector = new DefaultWorkerSelector(services.workers(), services.scheduler(),
                null, configuredSpillBudget(1024L * 1024 * 1024), services.models());
        var builds = new DurableGeneratedValueBuildService(services.builds(), submissions,
                new SnapshotGenerationSplitPlanner(services.catalogs()), selector, services.artifactUri(), services.catalogs(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
        return new GeneratedValueMaterializationCoordinator(services.catalogs(), builds,
                services.dataSource(), services.journal(), ModelAuthorityFixtures.models(),
                new org.pangrova.persistence.security.postgresql.tenant.PostgresDeferredTenantAuthority(),
                org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate::lockPublication, TableWriteAuthorityFixture.create());
    }

    private static JdbcRoleStore roles(DataSource dataSource) {
        var roles = new JdbcRoleStore(dataSource);
        return roles;
    }

    record GeneratedServices(DataSource dataSource, CatalogRegistry catalogs, GenerationJobStore builds,
                             WriteCommitJournal journal, WorkerManager workers, TaskScheduler scheduler,
                             EmbeddingModelRegistry models, String artifactUri) {}
}
