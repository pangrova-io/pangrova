package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.testing.integration.control.ai.ModelAuthorityFixtures;

import org.pangrova.metadata.service.RegistryTablePolicyAuthority;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.lance.LanceTableHandle;
import org.pangrova.spi.generation.job.GenerationOutput;
import org.pangrova.ai.control.evaluation.EvaluationDatasetId;
import org.pangrova.ai.control.evaluation.EvaluationDatasetVersion;
import org.pangrova.ai.control.evaluation.EvaluationMetric;
import org.pangrova.ai.control.evaluation.EvaluationMetricDirection;
import org.pangrova.ai.control.evaluation.EvaluationMetricPolicy;
import org.pangrova.ai.control.evaluation.EvaluationMetricRule;
import org.pangrova.ai.control.evaluation.EvaluationReleaseDecision;
import org.pangrova.ai.control.evaluation.EvaluationRun;
import org.pangrova.ai.control.evaluation.EvaluationRunState;
import org.pangrova.ai.control.evaluation.EvaluationSubjectType;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.policy.PrivilegeGrant;
import org.pangrova.spi.security.policy.Role;
import org.pangrova.spi.security.policy.UserRoleAssignment;
import org.pangrova.spi.security.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.rowfilter.RowPolicy;
import org.pangrova.persistence.jdbc.security.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.jdbc.security.policy.JdbcGrantStore;
import org.pangrova.persistence.jdbc.security.policy.JdbcRoleStore;
import org.pangrova.persistence.jdbc.security.rowfilter.JdbcRowPolicyStore;
import org.pangrova.spi.security.tenant.TenantStatus;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Authenticated SQL to real Worker/JNI export, persisted publication and policy-filtered artifact bytes. */
@E2ETest
class LanceFeatureViewExportE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "lance_feature_export";
    private JdbcRoleStore roles;
    private org.pangrova.persistence.api.security.rowfilter.RowPolicyStore policies;
    private org.pangrova.persistence.api.security.mask.EnrichedColumnMaskStore masks;
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
        assertPublishedRowCountGatesRelease(first, snapshot, 2, true);
        corruptAuditPlan(view.substring(view.lastIndexOf('.') + 1));
        replacePolicies(3, "'current-mask'");
        TypedRow second = executeQuery("MATERIALIZE FEATURE VIEW " + view + " VERSION 1 SNAPSHOT '" + snapshot + "'")
                .get(0);
        assertThat(second.getString("generation")).isNotEqualTo(first.getString("generation"));
        assertThat(readPublished(second, snapshot)).containsExactly("3:current-mask");
        assertPublishedRowCountGatesRelease(second, snapshot, 1, false);
        replacePolicies(100, "'empty-mask'");
        TypedRow empty = executeQuery("MATERIALIZE FEATURE VIEW " + view + " VERSION 1 SNAPSHOT '" + snapshot + "'")
                .get(0);
        assertThat(readPublished(empty, snapshot)).isEmpty();
        assertThat(executeExpectingError("CREATE FEATURE VIEW " + view + "_forged VERSION 1 AS '"
                + definition.replace("\"source_query\":", "\"source_plan_base64\":\"AQID\",\"source_query\":") + "'"))
                .contains("source_plan_base64");
        executeDdl("CREATE FEATURE VIEW " + view + "_limit VERSION 1 AS '"
                + definition.replace("FROM " + source, "FROM " + source + " LIMIT 1") + "'");
        var limited = org.pangrova.ai.control.assembly.AiInfrastructureStores.jdbc(
                stateStoreDataSource(), ModelAuthorityFixtures.artifacts().builds(stateStoreDataSource())).features()
                .findFeatureView(new org.pangrova.ai.control.feature.FeatureCatalogStore.FeatureViewId(
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
        roles = new JdbcRoleStore(stateStoreDataSource());
        roles.initSchema();
        role = "feature_" + UUID.randomUUID();
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(session.getUser(), role));
        var grants = new JdbcGrantStore(stateStoreDataSource());
        grants.addGrant(PrivilegeGrant.of(role, Privilege.ALL, "SCHEMA", CATALOG, "default", null, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT, "TABLE", CATALOG, sourceSchema, "customers", null));
        policies = new org.pangrova.metadata.service.TableControlRowPolicyStore(
                new RegistryTablePolicyAuthority(metadataBootstrap.catalogRegistry()),
                new JdbcRowPolicyStore(stateStoreDataSource()));
        masks = new org.pangrova.metadata.adapter.security.TableControlColumnMaskStore(
                new RegistryTablePolicyAuthority(metadataBootstrap.catalogRegistry()),
                new JdbcColumnMaskStore(stateStoreDataSource()));
        replacePolicies(minimum, mask);
        configureFeatureSecurity(new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(policies), new ColumnMaskingProcessor(masks)));
    }

    private void replacePolicies(int minimum, String mask) {
        policies.getPolicies(CATALOG, sourceSchema, "customers").forEach(policy -> policies.deletePolicy(policy.policyId()));
        masks.getColumnMasks(CATALOG, sourceSchema, "customers", "secret").forEach(value -> masks.deleteMask(value.maskId()));
        policies.savePolicy(new RowPolicy(0, "feature_rows", CATALOG, sourceSchema, "customers",
                "customer_id >= " + minimum, List.of(role), true, 1, null));
        masks.saveColumnMask(new ColumnMask(0, "feature_secret", CATALOG, sourceSchema, "customers", "secret",
                mask, List.of(role), List.of(), true, null));
    }

    private LanceTableHandle table() {
        var session = authenticatedCatalogSession(CATALOG);
        return (LanceTableHandle) metadataBootstrap.catalogRegistry().getCatalog(CATALOG).getMetadata(session)
                .getTableHandle(session, new SchemaTableName(sourceSchema, "customers"));
    }

    private List<String> readPublished(TypedRow result, long snapshot) throws Exception {
        var store = ModelAuthorityFixtures.artifacts().builds(stateStoreDataSource());
        String job = result.getString("job_id");
        await().atMost(Duration.ofMinutes(2)).untilAsserted(() -> assertThat(store.findPublishedGeneration(
                tenant, result.getString("artifact_id"), result.getString("generation"), Long.toString(snapshot))).isPresent());
        List<GenerationOutput> outputs = store.loadCompleteOutputs(tenant, job);
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

    private void assertPublishedRowCountGatesRelease(
            TypedRow result, long snapshot, long expectedRows, boolean shouldApprove) {
        var manifest = ModelAuthorityFixtures.artifacts().builds(stateStoreDataSource())
                .findPublishedGeneration(tenant, result.getString("artifact_id"),
                        result.getString("generation"), Long.toString(snapshot))
                .orElseThrow();
        String suffix = UUID.randomUUID().toString();
        String datasetId = "feature-export-" + suffix;
        String policyVersion = "minimum-two-rows-" + suffix;
        String runId = "feature-export-" + suffix;
        Instant now = Instant.now();
        var evaluations = org.pangrova.ai.control.assembly.AiInfrastructureStores
                .jdbc(stateStoreDataSource(),
                        ModelAuthorityFixtures.artifacts().builds(stateStoreDataSource())).evaluations();
        evaluations.createDatasetVersion(new EvaluationDatasetVersion(
                new EvaluationDatasetId(tenant, datasetId), 1, "feature-e2e",
                manifest.sourceSnapshot(), manifest.manifestUri(),
                manifest.manifestSha256(), manifest.rowCount(), "{}", now));
        evaluations.createMetricPolicy(new EvaluationMetricPolicy(tenant, policyVersion,
                List.of(new EvaluationMetricRule("materialized_rows", "rows",
                        EvaluationMetricDirection.HIGHER_IS_BETTER, 2, 0))));
        evaluations.createRun(new EvaluationRun(
                tenant, runId, datasetId, 1, EvaluationSubjectType.FEATURE_VIEW,
                manifest.artifactId(), manifest.generation(), null, policyVersion,
                EvaluationRunState.RUNNING, now, null, null, null));
        assertThat(evaluations.completeFeatureViewMaterializationRun(
                tenant, runId, now.plusSeconds(1))).isTrue();
        assertThat(evaluations.listMetrics(tenant, runId)).singleElement()
                .satisfies(metric -> {
                    assertThat(metric.name()).isEqualTo("materialized_rows");
                    assertThat(metric.value()).isEqualTo((double) expectedRows);
                    assertThat(metric.evidenceReference()).isEqualTo(manifest.manifestUri());
                });
        var approval = new EvaluationReleaseDecision(
                tenant, runId, true, policyVersion, "published feature row count",
                "feature-e2e", now.plusSeconds(2));
        if (shouldApprove) {
            evaluations.recordReleaseDecision(approval);
            assertThat(evaluations.findReleaseDecision(tenant, runId)).contains(approval);
        } else {
            assertThatThrownBy(() -> evaluations.recordReleaseDecision(approval))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("violates numerical threshold");
            assertThat(evaluations.findReleaseDecision(tenant, runId)).isEmpty();
            String forgedRunId = runId + "-forged";
            evaluations.createRun(new EvaluationRun(
                    tenant, forgedRunId, datasetId, 1, EvaluationSubjectType.FEATURE_VIEW,
                    manifest.artifactId(), manifest.generation(), null, policyVersion,
                    EvaluationRunState.RUNNING, now, null, null, null));
            assertThat(evaluations.completeRun(tenant, forgedRunId,
                    EvaluationRunState.SUCCEEDED,
                    List.of(new EvaluationMetric("materialized_rows", 2, "rows", true,
                            manifest.manifestUri())), null, null, now.plusSeconds(1)))
                    .isTrue();
            assertThatThrownBy(() -> evaluations.recordReleaseDecision(
                    new EvaluationReleaseDecision(tenant, forgedRunId, true,
                            policyVersion, "forged row count", "feature-e2e",
                            now.plusSeconds(2))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("differs from published export");
            assertThat(evaluations.findReleaseDecision(tenant, forgedRunId)).isEmpty();
            String wrongDatasetId = datasetId + "-wrong-manifest";
            String wrongRunId = runId + "-wrong-manifest";
            String wrongSha = (manifest.manifestSha256().charAt(0) == 'a' ? "b" : "a")
                    .repeat(64);
            evaluations.createDatasetVersion(new EvaluationDatasetVersion(
                    new EvaluationDatasetId(tenant, wrongDatasetId), 1, "feature-e2e",
                    manifest.sourceSnapshot(), manifest.manifestUri(),
                    wrongSha, manifest.rowCount(), "{}", now));
            evaluations.createRun(new EvaluationRun(
                    tenant, wrongRunId, wrongDatasetId, 1,
                    EvaluationSubjectType.FEATURE_VIEW, manifest.artifactId(),
                    manifest.generation(), null, policyVersion,
                    EvaluationRunState.RUNNING, now, null, null, null));
            assertThatThrownBy(() -> evaluations.completeFeatureViewMaterializationRun(
                    tenant, wrongRunId, now.plusSeconds(1)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not pin the published Feature View output");
            assertThat(evaluations.findRun(tenant, wrongRunId).orElseThrow().state())
                    .isEqualTo(EvaluationRunState.RUNNING);
        }
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
