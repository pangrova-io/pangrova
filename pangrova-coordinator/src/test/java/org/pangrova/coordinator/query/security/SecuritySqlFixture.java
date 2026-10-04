package org.pangrova.coordinator.query.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.store.JdbcMetadataStore;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.GroupRoleMapping;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.security.secret.SecretResolver;
import org.pangrova.security.store.PersistentColumnMaskStore;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.store.PersistentRowPolicyStore;
import org.pangrova.spi.security.UserIdentity;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Real catalog and durable authority for protected SQL compilation, not Native execution. */
final class SecuritySqlFixture implements AutoCloseable {
    private static final Set<String> ROLES = Set.of("admin_role", "sales_team", "other_team", "us_team",
            "regional_team", "finance", "analysts", "role1", "admin_team", "no_access");
    final SqlCompiler compiler;
    private final HadoopCatalog catalog;
    private final MetadataBootstrap metadata;
    private final PluginConnectorHandleFactory connectors;
    private final PersistentGrantStore grants;
    private final PersistentRowPolicyStore policies;
    private final PersistentColumnMaskStore masks;
    private final DynamicRbacAccessControl authority;

    SecuritySqlFixture(Path warehouse) throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("security_sql");
        var roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        for (String role : ROLES) {
            roles.createRole(Role.of(role));
            roles.mapGroupToRole(GroupRoleMapping.of(role, role));
        }
        grants = new PersistentGrantStore(dataSource);
        policies = new PersistentRowPolicyStore(dataSource);
        masks = new PersistentColumnMaskStore(dataSource);
        authority = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(policies), new ColumnMaskingProcessor(masks));
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        catalog = new HadoopCatalog(configuration, warehouse.toString());
        catalog.createNamespace(Namespace.of("default"));
        for (String table : List.of("orders", "source_orders")) {
            catalog.createTable(TableIdentifier.of("default", table), ordersSchema());
        }
        var store = new JdbcMetadataStore(dataSource);
        store.saveCatalogDescriptor(new CatalogDescriptor("default", CatalogType.CONNECTOR, "iceberg",
                CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString(), "type", "hadoop"), "", Set.of()));
        connectors = new PluginConnectorHandleFactory(getClass().getClassLoader(), new SecretResolver(),
                descriptor -> Optional.empty());
        metadata = MetadataBootstrap.initialize(store, connectors);
        var schema = metadata.buildCalciteSchema(SessionContextConverter.toConnectorSession(
                session("schema_reader", Set.of())));
        compiler = new SqlCompiler(new PangrovaPlanner(schema, List.of("default", "default")),
                new SubstraitConverter(), new SubstraitPlanSerializer());
    }

    PlannedQuery compile(String sql) {
        return compiler.compileForPolicyApplication(sql).toPlannedQuery();
    }

    SecurityPolicyApplier applier() {
        return new SecurityPolicyApplier(compiler, new AccessControlEnforcer(authority),
                new ColumnMaskApplier(authority), new RowFilterApplier(authority));
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

    static CoordinatorSessionContext session(String principal, Set<String> groups) {
        return new CoordinatorSessionContext("security-query-" + principal,
                Map.of("catalog", "default", "schema", "default"),
                principal == null ? null : new UserIdentity(principal, groups, Map.of(), Instant.EPOCH),
                "security-test-tenant");
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
