package org.pangrova.testing.integration.control.ai;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.IcebergConnector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.types.PangrovaType;
import org.pangrova.types.VectorType;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator;
import org.pangrova.query.spi.write.WriteAuthorityResolver;
import org.pangrova.query.api.write.WriteAuthorityRequest;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantRole;
import org.pangrova.security.api.tenant.TenantStatus;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Query-only receipt production uses current PostgreSQL authority and actual SDK table declarations. */
class QueryOnlyWriteAuthorityIntegrationTest extends ContainerTestBase {
    private static final String TENANT = "query-only-tenant";
    private static final String PRINCIPAL = "query-only-writer";
    private static final String ROLE = "query_only_writer";
    private static final QualifiedTableName TABLE = new QualifiedTableName("lake", "docs", "documents");
    private static final SchemaTableName SOURCE = new SchemaTableName(TABLE.schema(), TABLE.table());
    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return PRINCIPAL; }
        @Override public Optional<String> getCatalog() { return Optional.of(TABLE.catalog()); }
        @Override public Optional<String> getSchema() { return Optional.of(TABLE.schema()); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", TENANT); }
        @Override public Optional<ConnectorQueryAuthority> getQueryAuthority() { return Optional.empty(); }
    };

    @TempDir Path warehouse;
    private DataSource dataSource;
    private HadoopCatalog sdkCatalog;
    private IcebergConnector connector;
    private CatalogRegistry catalogs;
    private ConnectorMetadata metadata;
    private JdbcGrantStore grants;
    private WriteAuthorityResolver authority;

    @BeforeEach
    void createRealCatalogAndCurrentAuthorityWithoutMaterializationOwner() throws Exception {
        dataSource = MigratedPostgres.create();
        var roles = new JdbcRoleStore(dataSource);
        roles.createRole(Role.of(ROLE));
        roles.grantRoleToUser(UserRoleAssignment.of(PRINCIPAL, ROLE));
        var tenants = new JdbcTenantStore(dataSource);

        tenants.createTenant(new Tenant(TENANT, TENANT, TenantStatus.ACTIVE, null,
                List.of(), List.of(), null, Map.of(), Instant.EPOCH, PRINCIPAL));
        tenants.addUserToTenant(TENANT, PRINCIPAL, TenantRole.MEMBER);
        grants = new JdbcGrantStore(dataSource);
        for (var privilege : List.of(Privilege.SELECT, Privilege.INSERT)) {
            grants.addGrant(PrivilegeGrant.of(ROLE, privilege, "TABLE",
                    TABLE.catalog(), TABLE.schema(), TABLE.table(), null));
        }
        var configuration = new Configuration();
        configuration.setBoolean("fs.file.impl.disable.cache", true);
        sdkCatalog = new HadoopCatalog(configuration, warehouse.toString());
        sdkCatalog.createNamespace(Namespace.of(TABLE.schema()));
        connector = new IcebergConnector(TABLE.catalog(), sdkCatalog);
        catalogs = new CatalogRegistry(new PostgresCatalogDeploymentRepository(dataSource),
                descriptor -> new ConnectorCatalogHandle(descriptor, connector), null, null);
        catalogs.register(new CatalogDescriptor(TABLE.catalog(), CatalogType.CONNECTOR, "iceberg",
                CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString()), "", Set.of(TENANT)));
        metadata = catalogs.getCatalog(TABLE.catalog()).getMetadata(SESSION);
        authority = GeneratedValueMaterializationCoordinator.writeAuthoritySupplier(
                catalogs, dataSource, null, ModelAuthorityFixtures.models());
    }

    @AfterEach
    void closeCatalog() throws Exception {
        try {
            if (catalogs != null) catalogs.shutdown();
        } finally {
            // Registration can fail before the registry owns the connector; shutdown is idempotent.
            if (connector != null) connector.shutdown();
            else if (sdkCatalog != null) sdkCatalog.close();
        }
    }

    @Test
    void ordinaryWriteProducesExactReceiptWithoutMaterializationOwnerAndHonorsCurrentGrant() {
        createTable(false);
        var request = request();

        var receipt = authority.resolve(request);

        assertThat(DeferredAuthorizationScopeCodec.decodeWriteReceipt(receipt.authorizationScope()))
                .isEqualTo(receipt);
        assertThat(receipt.declaration()).isEqualTo(request.compiledDeclaration());
        assertThat(receipt.declaration().hasGeneratedColumns()).isFalse();
        var scope = DeferredAuthorizationScopeCodec.decode(receipt.authorizationScope());
        assertThat(scope.permissions()).containsExactly(new DeferredAuthorizationScope.TablePermission(
                TABLE, Set.of(), Privilege.INSERT, DeferredAuthorizationScope.ReadMode.NONE,
                DeferredAuthorizationScope.PolicySnapshot.empty()));
        assertThat(scope.definitionSnapshot().orElseThrow().models()).isEmpty();
        assertThat(grants.revokeGrants(ROLE, Privilege.INSERT, TABLE.catalog(), TABLE.schema(), TABLE.table()))
                .isEqualTo(1);
        assertThatThrownBy(() -> authority.resolve(request)).isInstanceOf(AuthorizationDeniedException.class);
        assertThat(sdkCatalog.loadTable(TableIdentifier.of(TABLE.schema(), TABLE.table())).currentSnapshot()).isNull();
    }

    @Test
    void actualGeneratedDeclarationRejectsMissingMaterializationOwnerBeforeReceiptProduction() {
        createTable(true);
        var request = request();
        assertThat(request.compiledDeclaration().hasGeneratedColumns()).isTrue();
        var table = sdkCatalog.loadTable(TableIdentifier.of(TABLE.schema(), TABLE.table()));
        assertThat(GeneratedColumnMetadataCodec.decode(table.properties())).isEqualTo(definitions());

        assertThatThrownBy(() -> authority.resolve(request))
                .isInstanceOf(AuthorizationDeniedException.class)
                .hasMessage("Generated-column writes require an active durable materialization owner");

        table.refresh();
        assertThat(table.currentSnapshot()).isNull();
        assertThat(GeneratedColumnMetadataCodec.decode(table.properties())).isEqualTo(definitions());
    }

    private void createTable(boolean generated) {
        var columns = List.of(new ConnectorTableMetadata.Column("title", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR, OptionalInt.empty(),
                        new VectorType(3, VectorType.ElementType.FLOAT)));
        metadata.createTable(SESSION, new ConnectorTableMetadata(SOURCE,
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns,
                generated ? GeneratedColumnMetadataCodec.encode(definitions()) : Map.of()));
    }

    private WriteAuthorityRequest request() {
        var handle = metadata.getTableHandle(SESSION, SOURCE);
        var actual = metadata.getTableMetadata(SESSION, handle);
        var definitions = GeneratedColumnMetadataCodec.decode(actual.properties());
        var declaration = WriteDeclarationSnapshot.capture(TABLE, actual, definitions.toByteArray(),
                definitions.getDefinitionsCount() != 0);
        return new WriteAuthorityRequest(TABLE, SESSION, "INSERT", List.of(), Set.of(Privilege.INSERT),
                declaration, false);
    }

    private static GeneratedColumnSet definitions() {
        return GeneratedColumnSet.newBuilder().setSchemaVersion(GeneratedColumnMetadataCodec.SCHEMA_VERSION)
                .addDefinitions(GeneratedColumnDefinition.newBuilder().setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef()).setLogicalModelName("embed_model")
                        .addSourceColumns("title").setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(VectorLogicalType.create(3))).build();
    }
}
