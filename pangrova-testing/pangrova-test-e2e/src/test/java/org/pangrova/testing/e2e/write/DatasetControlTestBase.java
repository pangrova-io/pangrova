package org.pangrova.testing.e2e.write;

import com.google.protobuf.Any;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.pangrova.coordinator.metadata.CatalogExecutionBindings;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Persisted deployments and the actor admitted by the real SQL authentication chain. */
abstract class DatasetControlTestBase extends NativeWriteLifecycleTestBase {
    protected static final String DATASET = "dataset_control";
    private String restrictedCatalog;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        var configs = new ArrayList<>(super.connectorConfigs());
        configs.add(new ConnectorTestConfig(DATASET, SCHEMA, "lance", Map.of(
                "warehouse", tempDir.resolve("dataset-control").toString(),
                "jdbc.url", pgJdbcUrl(), "jdbc.user", pgUsername(), "jdbc.password", pgPassword())));
        return List.copyOf(configs);
    }

    @BeforeEach
    void persistRestrictedDeployment() {
        String tenant = "dataset-control-allowed-" + UUID.randomUUID();
        var security = authenticatedRequest().getSecurity();
        var tenants = new JdbcTenantStore(stateStoreDataSource());
        tenants.createTenant(new Tenant(tenant,
                "Restricted dataset owner", TenantStatus.ACTIVE, null, List.of(), List.of(),
                "e2e", Map.of(), Instant.now(), security.getPrincipal()));
        assertThat(tenants.getTenant(tenant)).isPresent();
        var descriptor = metadataBootstrap.catalogRegistry().getCatalog(DATASET).getDescriptor();
        restrictedCatalog = "dataset_restricted_" + UUID.randomUUID().toString().replace("-", "");
        metadataBootstrap.catalogRegistry().register(new CatalogDescriptor(restrictedCatalog, descriptor.type(),
                descriptor.provider(), descriptor.state(), descriptor.properties(),
                "Actual authenticated actor is not in this catalog's tenant allowlist", Set.of(tenant)));
    }

    @AfterEach
    void removeRestrictedDeployment() {
        if (restrictedCatalog != null) metadataBootstrap.catalogRegistry().unregister(restrictedCatalog);
    }

    protected final DatasetControlFixture controlFixture() throws Exception {
        return new DatasetControlFixture("http://localhost:" + coordinatorHttpServer.getPort(),
                E2E_INTERNAL_AUTH_SECRET);
    }

    protected final RuntimeCommand schemaCommand(boolean denied) {
        Type primitive = Type.newBuilder().setI64(Type.I64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        Type nested = Type.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(primitive)
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        var schema = NamedStruct.newBuilder().addNames("record").addNames("id")
                .setStruct(Type.Struct.newBuilder().addTypes(nested)).build();
        var descriptor = metadataBootstrap.catalogRegistry()
                .getCatalog(denied ? restrictedCatalog : DATASET).getDescriptor();
        var request = DatasetSchemaFingerprintRequest.newBuilder()
                .setCatalog(CatalogExecutionBindings.from(descriptor)).setSchema(schema)
                .addColumnTypes(LogicalType.newBuilder().setCanonicalSql("ROW(id BIGINT)").setSubstraitType(nested))
                .setSecurity(authenticatedRequest().getSecurity())
                .setDeadlineEpochMs(System.currentTimeMillis() + 60_000).build();
        return RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT)
                .setPayload(Any.pack(request)).build();
    }
}
