package org.pangrova.testing.e2e.connector;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.lance.LanceTableHandle;
import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.security.store.PersistentColumnMaskStore;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.store.PersistentRowPolicyStore;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Authenticated SQL to real Worker/JNI export, persisted publication and policy-filtered artifact bytes. */
@E2ETest
class LanceFeatureViewExportE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "lance_feature_export";
    private PersistentRoleStore roles;
    private PersistentRowPolicyStore policies;
    private PersistentColumnMaskStore masks;
    private String role;
    private String sourceSchema;
    private String tenant;

    @Override protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(new ConnectorTestConfig(CATALOG, "default", "lance", Map.of(
                "warehouse", tempDir.resolve("lance-feature-warehouse").toString())));
    }

    @AfterEach void releasePolicyWiring() {
        if (roles != null) {
            clearFeatureSecurity();
            roles.dropRole(role);
        }
    }

    @Test void consecutiveNativeWritesKeepTheTaskPoolEmptyUntilDriversStart() {
        String schema = "write_start_" + UUID.randomUUID().toString().replace("-", "");
        String source = CATALOG + "." + schema + ".customers";
        executeDdl("CREATE SCHEMA " + CATALOG + "." + schema);
        executeDdl("CREATE TABLE " + source + " (customer_id BIGINT, secret VARCHAR)");
        for (int id = 1; id <= 16; id++) {
            executeDml("INSERT INTO " + source + " VALUES (" + id + ", 'customer-" + id + "')");
        }
        assertThat(executeQuery("SELECT COUNT(*) AS cnt FROM " + source).get(0).getLong("cnt"))
                .isEqualTo(16L);
    }

    @Test void materializationRecompilesCurrentPoliciesAndExportsOnlyTheExactHistoricalSnapshot() throws Exception {
        sourceSchema = "source_" + UUID.randomUUID().toString().replace("-", "");
        String source = CATALOG + "." + sourceSchema + ".customers";
        String view = CATALOG + ".default.features_" + UUID.randomUUID().toString().replace("-", "");
        executeDdl("CREATE SCHEMA " + CATALOG + "." + sourceSchema);
        executeDdl("CREATE TABLE " + source + " (customer_id BIGINT, event_ts TIMESTAMP, secret VARCHAR)");
        executeDml("INSERT INTO " + source + " VALUES "
                + "(1, TIMESTAMP '2025-01-01 00:00:00', 'private-one'),"
                + "(2, TIMESTAMP '2025-01-02 00:00:00', 'private-two'),"
                + "(3, TIMESTAMP '2025-01-03 00:00:00', 'private-three')");
        long snapshot = table().version();
        executeDml("INSERT INTO " + source + " VALUES (4, TIMESTAMP '2025-01-04 00:00:00', 'newer')");
        configurePolicies(2, "'hidden'");
        String definition = definition(source);
        executeDdl("CREATE FEATURE VIEW " + view + " VERSION 1 AS '" + definition + "'");
        TypedRow first = executeQuery("MATERIALIZE FEATURE VIEW " + view + " VERSION 1 SNAPSHOT '" + snapshot + "'")
                .get(0);
        assertThat(readPublished(first, snapshot)).containsExactlyInAnyOrder("2:hidden", "3:hidden");
        corruptAuditPlan(view.substring(view.lastIndexOf('.') + 1));
        replacePolicies(3, "'current-mask'");
        TypedRow second = executeQuery("MATERIALIZE FEATURE VIEW " + view + " VERSION 1 SNAPSHOT '" + snapshot + "'")
                .get(0);
        assertThat(second.getString("generation")).isNotEqualTo(first.getString("generation"));
        assertThat(readPublished(second, snapshot)).containsExactly("3:current-mask");
        replacePolicies(100, "'empty-mask'");
        TypedRow empty = executeQuery("MATERIALIZE FEATURE VIEW " + view + " VERSION 1 SNAPSHOT '" + snapshot + "'")
                .get(0);
        assertThat(readPublished(empty, snapshot)).isEmpty();
        assertThat(executeExpectingError("CREATE FEATURE VIEW " + view + "_forged VERSION 1 AS '"
                + definition.replace("\"source_query\":", "\"source_plan_base64\":\"AQID\",\"source_query\":") + "'"))
                .contains("source_plan_base64");
        executeDdl("CREATE FEATURE VIEW " + view + "_limit VERSION 1 AS '"
                + definition.replace("FROM " + source, "FROM " + source + " LIMIT 1") + "'");
        var limited = org.pangrova.metadata.ai.AiInfrastructureStores.jdbc(stateStoreDataSource()).features()
                .findFeatureView(new org.pangrova.metadata.feature.FeatureCatalogStore.FeatureViewId(
                        tenant, CATALOG, "default", view.substring(view.lastIndexOf('.') + 1) + "_limit"), 1)
                .orElseThrow();
        assertThat(limited.sourceQuery()).contains("LIMIT 1");
        assertThat(limited.definitionJson()).doesNotContain("source_plan_base64");
        corruptHistoricalManifest(snapshot);
        assertThat(executeExpectingError("MATERIALIZE FEATURE VIEW " + view + " VERSION 1 SNAPSHOT '" + snapshot + "'"))
                .contains("Native publication cannot be inferred from the failed response");
    }

    private void configurePolicies(int minimum, String mask) {
        var session = authenticatedCatalogSession(CATALOG);
        tenant = session.getProperties().get("tenant_id");
        var tenants = new JdbcTenantStore(stateStoreDataSource());
        tenants.initSchema();
        assertThat(tenants.getTenant(tenant)).hasValueSatisfying(current ->
                assertThat(current.status()).isEqualTo(TenantStatus.ACTIVE));
        assertThat(tenants.getTenantForUser(session.getUser())).hasValueSatisfying(membership ->
                assertThat(membership.tenant().tenantId()).isEqualTo(tenant));
        roles = new PersistentRoleStore(stateStoreDataSource());
        roles.initSchema();
        role = "feature_" + UUID.randomUUID();
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(session.getUser(), role));
        var grants = new PersistentGrantStore(stateStoreDataSource());
        grants.addGrant(PrivilegeGrant.of(role, Privilege.ALL, "SCHEMA", CATALOG, "default", null, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT, "TABLE", CATALOG, sourceSchema, "customers", null));
        policies = new PersistentRowPolicyStore(stateStoreDataSource());
        masks = new PersistentColumnMaskStore(stateStoreDataSource());
        replacePolicies(minimum, mask);
        configureFeatureSecurity(new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(policies), new ColumnMaskingProcessor(masks)));
    }

    private void replacePolicies(int minimum, String mask) {
        policies.getPolicies(CATALOG, sourceSchema, "customers").forEach(policy -> policies.deletePolicy(policy.policyId()));
        masks.getColumnMasks(CATALOG, sourceSchema, "customers", "secret").forEach(value -> masks.deleteMask(value.maskId()));
        policies.savePolicy(new RowPolicy(0, "feature_rows", CATALOG, sourceSchema, "customers",
                "customer_id >= " + minimum, List.of(role), true, 1));
        masks.saveColumnMask(new ColumnMask(0, "feature_secret", CATALOG, sourceSchema, "customers", "secret",
                mask, List.of(role), List.of(), true));
    }

    private LanceTableHandle table() {
        var session = authenticatedCatalogSession(CATALOG);
        return (LanceTableHandle) metadataBootstrap.catalogRegistry().getCatalog(CATALOG).getMetadata(session)
                .getTableHandle(session, new SchemaTableName(sourceSchema, "customers"));
    }

    private List<String> readPublished(TypedRow result, long snapshot) throws Exception {
        var store = new JdbcArtifactBuildStore(stateStoreDataSource());
        String job = result.getString("job_id");
        await().atMost(Duration.ofMinutes(2)).untilAsserted(() -> assertThat(store.findPublishedGeneration(
                tenant, result.getString("artifact_id"), result.getString("generation"), Long.toString(snapshot))).isPresent());
        List<ArtifactBuildOutput> outputs = store.loadCompleteOutputs(tenant, job);
        assertThat(outputs).isNotEmpty();
        List<String> rows = new ArrayList<>();
        for (var output : outputs) {
            Path path = output.opaqueUri().startsWith("file:") ? Path.of(URI.create(output.opaqueUri())) : Path.of(output.opaqueUri());
            byte[] bytes = Files.readAllBytes(path);
            assertThat(bytes.length).isEqualTo(output.sizeBytes());
            assertThat(org.pangrova.common.util.DigestUtils.sha256Hex(bytes)).isEqualTo(output.checksumSha256());
            try (var allocator = new RootAllocator(); var input = Files.newInputStream(path);
                 var reader = new ArrowStreamReader(input, allocator)) {
                assertThat(reader.getVectorSchemaRoot().getSchema().getFields()).extracting(field -> field.getName())
                        .containsExactly("customer_id", "event_ts", "secret");
                while (reader.loadNextBatch()) {
                    var root = reader.getVectorSchemaRoot();
                    for (int row = 0; row < root.getRowCount(); row++) {
                        rows.add(root.getVector("customer_id").getObject(row) + ":" + root.getVector("secret").getObject(row));
                    }
                }
            }
        }
        return rows;
    }

    private void corruptAuditPlan(String view) throws Exception {
        try (var connection = stateStoreDataSource().getConnection(); var statement = connection.prepareStatement(
                "UPDATE feature_view_versions SET source_plan=? WHERE tenant_id=? AND catalog_name=? AND feature_view_name=?")) {
            statement.setBytes(1, new byte[] {1, 2, 3}); statement.setString(2, tenant);
            statement.setString(3, CATALOG); statement.setString(4, view);
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
    }

    private void corruptHistoricalManifest(long snapshot) throws Exception {
        try (var connection = stateStoreDataSource().getConnection(); var statement = connection.prepareStatement(
                "UPDATE managed_source_publications SET manifest_sha256=? WHERE catalog_name=? AND tenant_id=? "
                        + "AND table_id=? AND published_version=? AND state='ACCEPTED'")) {
            statement.setBytes(1, new byte[32]); statement.setString(2, CATALOG); statement.setString(3, tenant);
            statement.setString(4, table().tableId()); statement.setLong(5, snapshot);
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
    }

    private static String definition(String source) {
        return "{\"entity_keys\":[\"customer_id\"],\"event_time\":\"event_ts\",\"source_table\":\"" + source
                + "\",\"source_query\":\"SELECT customer_id,event_ts,secret FROM " + source + "\","
                + "\"transform_version\":\"v1\",\"schema\":{\"customer_id\":\"BIGINT\",\"event_ts\":\"TIMESTAMP\",\"secret\":\"VARCHAR\"},"
                + "\"freshness_target_ms\":60000,\"lineage\":[\"" + source + "\"]}";
    }
}
