package org.pangrova.coordinator.query.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.planner.core.analysis.PangrovaPlanner;
import org.pangrova.planner.core.analysis.PlannerContext;
import org.pangrova.planner.core.analysis.PlannedQuery;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.planner.core.security.ColumnMaskApplier;
import org.pangrova.planner.core.security.RowFilterApplier;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;
import org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl;
import org.pangrova.security.api.policy.GroupRoleMapping;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.security.core.secret.SecretResolverRegistry;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.coordinator.dynamictable.DurableDynamicTableResolver;
import org.pangrova.coordinator.testinfra.DynamicTablePublicationFixtures;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.planner.core.mv.DynamicTableRewriter;
import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/** Real catalog and durable authority for protected SQL compilation, not Native execution. */
final class SecuritySqlFixture implements AutoCloseable {
    private static final Set<String> ROLES = Set.of("admin_role", "sales_team", "other_team", "us_team",
            "regional_team", "finance", "analysts", "role1", "admin_team", "no_access");
    final SqlCompiler compiler;
    private final HadoopCatalog catalog;
    private final MetadataBootstrap metadata;
    private final PluginConnectorHandleFactory connectors;
    private final JdbcGrantStore grants;
    private final JdbcRowPolicyStore policies;
    private final JdbcColumnMaskStore masks;
    private final DynamicRbacAccessControl authority;
    private final DataSource dataSource;
    private org.pangrova.connector.api.metadata.table.QualifiedTableName dynamicPhysical;

    SecuritySqlFixture(Path warehouse) throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("security_sql");
        var roles = new JdbcRoleStore(dataSource);
        for (String role : ROLES) {
            roles.createRole(Role.of(role));
            roles.mapGroupToRole(GroupRoleMapping.of(role, role));
        }
        grants = new JdbcGrantStore(dataSource);
        policies = new JdbcRowPolicyStore(dataSource);
        masks = new JdbcColumnMaskStore(dataSource);
        authority = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(policies), new ColumnMaskingProcessor(masks));
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        catalog = new HadoopCatalog(configuration, warehouse.toString());
        catalog.createNamespace(Namespace.of("default"));
        for (String table : List.of("orders", "source_orders")) {
            catalog.createTable(TableIdentifier.of("default", table), ordersSchema());
        }
        var store = new PostgresCatalogDeploymentRepository(dataSource);
        store.saveCatalogDescriptor(new CatalogDescriptor("default", CatalogType.CONNECTOR, "iceberg",
                CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString(), "type", "hadoop"), "", Set.of()));
        connectors = new PluginConnectorHandleFactory(getClass().getClassLoader(), new SecretResolverRegistry(),
                descriptor -> Optional.empty());
        metadata = MetadataBootstrap.initialize(store, new PostgresTableProjectionRepository(dataSource), new PostgresMetadataProjectionTransactions(dataSource), new PostgresMetadataSyncRepository(dataSource), connectors);
        var schema = metadata.buildCalciteSchema(SessionContextConverter.toConnectorSession(
                session("schema_reader", Set.of())));
        compiler = new SqlCompiler(new PangrovaPlanner(schema, List.of("default", "default")),
                new SubstraitConverter(), new SubstraitPlanSerializer());
    }

    PlannedQuery compile(String sql) {
        return compiler.compileForPolicyApplication(sql).toPlannedQuery();
    }

    SecurityPolicyApplier applier() {
        return applier(compiler);
    }

    SecurityPolicyApplier applier(SqlCompiler selectedCompiler) {
        return new SecurityPolicyApplier(selectedCompiler, new AccessControlEnforcer(authority),
                new ColumnMaskApplier(authority), new RowFilterApplier(authority));
    }

    SqlCompiler dynamicTableCompiler() {
        var publications = DynamicTablePublicationFixtures.catalog();
        var published = DynamicTablePublicationFixtures.create(publications, "security-test-tenant",
                "default.default.rollup", "SELECT id FROM default.default.orders");
        dynamicPhysical = new org.pangrova.connector.api.metadata.table.QualifiedTableName(
                "default", published.physicalTable().schema(), published.physicalTable().table());
        catalog.createNamespace(Namespace.of(InternalNamespace.SCHEMA_NAME));
        catalog.createTable(TableIdentifier.of(InternalNamespace.SCHEMA_NAME, dynamicPhysical.table()),
                ordersSchema());
        var schema = metadata.buildCalciteSchema(SessionContextConverter.toConnectorSession(
                session("schema_reader", Set.of())));
        var rewrite = new DynamicTableRewriter(new DurableDynamicTableResolver(publications,
                "security-test-tenant", "default", "default", candidate -> {
                    // Policy-only compilation: no physical scan or generation GC in this fixture.
                    if (!candidate.generation().equals(published.generation())) {
                        throw new AssertionError("Unexpected generation in security compilation");
                    }
                }));
        var context = new PlannerContext(null, null, null, null, null, rewrite, null, null, null);
        return new SqlCompiler(new PangrovaPlanner(schema, List.of("default", "default"), context),
                new SubstraitConverter(), new SubstraitPlanSerializer());
    }

    org.pangrova.connector.api.metadata.table.QualifiedTableName dynamicPhysical() {
        return java.util.Objects.requireNonNull(dynamicPhysical);
    }

    void grantTable(String role, Privilege privilege, String table) {
        String schema = table.startsWith(InternalNamespace.CANDIDATE_PREFIX)
                ? InternalNamespace.SCHEMA_NAME : "default";
        grants.addGrant(PrivilegeGrant.of(role, privilege,
                "TABLE", "default", schema, table, null));
    }

    void savePolicy(RowPolicy policy) {
        policies.savePolicy(policy);
    }

    void saveMask(ColumnMask mask) {
        masks.saveColumnMask(mask);
    }

    SecurityPolicyApplier withoutAccessEnforcer() {
        return new SecurityPolicyApplier(compiler, null, new ColumnMaskApplier(authority), new RowFilterApplier(authority));
    }

    void grant(String role, Privilege privilege) {
        for (String table : List.of("orders", "source_orders")) {
            grants.addGrant(PrivilegeGrant.of(role, privilege, "TABLE", "default", "default", table, null));
        }
    }

    void authorizePolicyReaders() {
        for (String role : ROLES) {
            if (!"no_access".equals(role)) grant(role, Privilege.SELECT);
        }
    }

    void installPolicies(List<RowPolicy> definitions) {
        for (String table : List.of("orders", "source_orders")) {
            for (RowPolicy existing : policies.getPolicies("default", "default", table)) {
                policies.deletePolicy(existing.policyId());
            }
        }
        definitions.forEach(policies::savePolicy);
    }

    void installMasks(List<ColumnMask> definitions) {
        for (ColumnMask existing : masks.getColumnMasks("default", "default", "orders", "department")) {
            masks.deleteMask(existing.maskId());
        }
        definitions.forEach(masks::saveColumnMask);
    }

    static QuerySession session(String principal, Set<String> groups) {
        return new QuerySession("security-query-" + principal,
                Map.of("catalog", "default", "schema", "default"),
                principal == null ? null : new UserIdentity(principal, groups, Map.of(), Instant.EPOCH),
                "security-test-tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }

    private static Schema ordersSchema() {
        return new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "department", Types.StringType.get()),
                Types.NestedField.optional(3, "region", Types.StringType.get()),
                Types.NestedField.optional(4, "status", Types.StringType.get()),
                Types.NestedField.optional(5, "amount", Types.DecimalType.of(10, 2)));
    }

    @Override
    public void close() throws Exception {
        try {
            metadata.shutdown();
        } finally {
            try {
                connectors.shutdown();
            } finally {
                catalog.close();
            }
        }
    }
}
