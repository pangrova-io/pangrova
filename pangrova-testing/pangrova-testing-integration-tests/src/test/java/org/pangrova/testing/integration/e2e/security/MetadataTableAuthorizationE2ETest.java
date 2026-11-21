package org.pangrova.testing.integration.e2e.security;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual HTTP SQL and Native metadata reads must use the base table's current JDBC grants. */
class MetadataTableAuthorizationE2ETest extends SqlE2ETestBase {
    private static final String ICEBERG = "metadata_authority";
    private static final String PAIMON = "metadata_authority_paimon";
    private static final String SCHEMA = "default";
    private String catalog;
    private List<String> suffixes;
    private final java.util.Map<String, Long> metadataCounts = new java.util.LinkedHashMap<>();
    private final java.util.Map<String, Integer> showCounts = new java.util.LinkedHashMap<>();
    private JdbcRoleStore roles;
    private JdbcGrantStore grants;
    private JdbcRowPolicyStore policies;
    private JdbcColumnMaskStore masks;
    private String role;
    private String table;
    private String qualified;
    private boolean restricted;
    private boolean rewired;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(ICEBERG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PAIMON, SCHEMA, paimonWarehouse().toString()));
    }

    private void createRealTableAndScopedRole(String selectedCatalog) {
        catalog = selectedCatalog;
        suffixes = catalog.equals(ICEBERG) ? List.of("snapshots", "history", "refs", "files", "manifests", "entries", "partitions")
                : List.of("snapshots", "tags", "options", "schemas", "branches", "files");
        restricted = false;
        rewired = false;
        roles = null;
        grants = null;
        policies = null;
        masks = null;
        metadataCounts.clear();
        showCounts.clear();
        table = "orders_" + UUID.randomUUID().toString().replace("-", "");
        qualified = catalog + "." + SCHEMA + "." + table;
        sqlRunner.executeDdl("CREATE TABLE " + qualified + " (id BIGINT, region VARCHAR"
                + (catalog.equals(PAIMON) ? ", PRIMARY KEY (id, region) NOT ENFORCED" : "")
                + ") PARTITIONED BY (region)");
        sqlRunner.execute("INSERT INTO " + qualified + " VALUES (1, 'private'), (2, 'public')");
        for (String suffix : suffixes) {
            metadataCounts.put(suffix, sqlRunner.execute("SELECT COUNT(*) AS n FROM " + qualified + "$" + suffix)
                    .get(0).getLong("n"));
        }
        assertThat(metadataCounts.get("snapshots")).isPositive();
        for (String kind : showKinds()) {
            showCounts.put(kind, sqlRunner.execute("SHOW " + kind + " FROM " + qualified).size());
        }
        roles = new JdbcRoleStore(stateStoreDataSource());
        role = "metadata_reader_" + UUID.randomUUID();
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(authenticatedCatalogSession(catalog).getUser(), role));
        grants = new JdbcGrantStore(stateStoreDataSource());
        policies = new JdbcRowPolicyStore(stateStoreDataSource());
        masks = new JdbcColumnMaskStore(stateStoreDataSource());
    }

    @AfterEach
    void restoreAuthorityAndDrainQueries() {
        try {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(queryManager.getActiveQueryIds()).isEmpty();
                assertThat(queryManager.getActiveQueryCount()).isZero();
            });
        } finally {
            removePolicies();
            if (rewired) clearFeatureSecurity();
            else if (restricted) restoreDefaultSqlAuthority();
            if (roles != null) {
                if (grants != null) {
                    grants.listGrantsForRole(role).forEach(grant -> grants.revokeGrant(grant.id()));
                }
                roles.dropRole(role);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {ICEBERG, PAIMON})
    void metadataSuffixCannotGrantReadOrSurviveBaseRevocation(String selectedCatalog) {
        createRealTableAndScopedRole(selectedCatalog);
        for (String suffix : suffixes) {
            grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT, "TABLE",
                    catalog, SCHEMA, table + "$" + suffix, null));
        }
        restrictDefaultSqlAuthority();
        restricted = true;
        assertMetadataDenied(qualified);

        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT, "TABLE", catalog, SCHEMA, table, null));
        assertMetadataReadsUnchanged();

        assertThat(grants.revokeGrants(role, Privilege.SELECT, catalog, SCHEMA, table)).isEqualTo(1);
        assertMetadataDenied(qualified);
    }

    @ParameterizedTest
    @ValueSource(strings = {ICEBERG, PAIMON})
    void baseRowPolicyBlocksRawMetadataAndRemovalRestoresReads(String selectedCatalog) {
        createRealTableAndScopedRole(selectedCatalog);
        bindPolicyAuthority();
        assertMetadataReadsUnchanged();
        policies.savePolicy(new RowPolicy(0, "only_private", catalog, SCHEMA, table,
                "id = 1", List.of(role), true, 1, null));
        assertThat(sqlRunner.execute("SELECT id FROM " + qualified)).singleElement()
                .satisfies(row -> assertThat(row.getLong("id")).isEqualTo(1L));
        assertPolicyBlocksMetadata();
        removePolicies();
        assertMetadataReadsUnchanged();
        assertThat(sqlRunner.execute("SELECT id FROM " + qualified)).hasSize(2);
    }

    @ParameterizedTest
    @ValueSource(strings = {ICEBERG, PAIMON})
    void maskOutsideMetadataProjectionBlocksRawReadAndRemovalRestoresReads(String selectedCatalog) {
        createRealTableAndScopedRole(selectedCatalog);
        bindPolicyAuthority();
        assertMetadataReadsUnchanged();
        masks.saveColumnMask(new ColumnMask(0, "redact_region", catalog, SCHEMA, table, "region",
                "'redacted'", List.of(role), List.of(), true, null));
        assertThat(sqlRunner.execute("SELECT region FROM " + qualified)).hasSize(2)
                .allSatisfy(row -> assertThat(row.getString("region")).isEqualTo("redacted"));
        assertPolicyBlocksMetadata();
        removePolicies();
        assertMetadataReadsUnchanged();
        assertThat(sqlRunner.execute("SELECT region FROM " + qualified)).extracting(row -> row.getString("region"))
                .containsExactlyInAnyOrder("private", "public");
    }

    private void bindPolicyAuthority() {
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT, "TABLE", catalog, SCHEMA, table, null));
        rewired = true;
        configureFeatureSecurity(new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(policies), new ColumnMaskingProcessor(masks)));
    }

    private void removePolicies() {
        if (policies != null) {
            policies.getPolicies(catalog, SCHEMA, table).forEach(policy -> policies.deletePolicy(policy.policyId()));
        }
        if (masks != null) {
            masks.getTableMasks(catalog, SCHEMA, table).forEach(mask -> masks.deleteMask(mask.maskId()));
        }
    }

    private void assertMetadataReadsUnchanged() {
        for (String suffix : suffixes) {
            assertThat(sqlRunner.execute("SELECT COUNT(*) AS n FROM " + qualified + "$" + suffix))
                    .singleElement().satisfies(row -> assertThat(row.getLong("n")).isEqualTo(metadataCounts.get(suffix)));
        }
        for (String kind : showKinds()) {
            assertThat(sqlRunner.execute("SHOW " + kind + " FROM " + qualified)).hasSize(showCounts.get(kind));
        }
    }

    private List<String> showKinds() {
        return catalog.equals(ICEBERG) ? List.of("SNAPSHOTS", "FILES", "MANIFESTS", "PARTITIONS", "TAGS", "BRANCHES")
                : List.of("SNAPSHOTS", "TAGS", "BRANCHES", "FILES");
    }

    private void assertPolicyBlocksMetadata() {
        for (String suffix : suffixes) {
            assertThat(sqlRunner.executeExpectingError("SELECT COUNT(*) AS n FROM " + qualified + "$" + suffix)
                    .message()).as("base policy protects metadata suffix %s", suffix)
                    .contains("SECURITY-0002", "Unfiltered table metadata");
        }
        for (String kind : showKinds()) {
            assertThat(sqlRunner.executeExpectingError("SHOW " + kind + " FROM " + qualified).message())
                    .as("base policy protects SHOW %s", kind)
                    .contains("SECURITY-0002", "Unfiltered table metadata");
        }
    }

    private void assertMetadataDenied(String qualified) {
        for (String suffix : suffixes) {
            assertThat(sqlRunner.executeExpectingError("SELECT COUNT(*) AS n FROM " + qualified + "$" + suffix)
                    .message()).as("metadata suffix %s", suffix).contains("SECURITY-0002");
        }
        for (String kind : showKinds()) {
            assertThat(sqlRunner.executeExpectingError("SHOW " + kind + " FROM " + qualified).message())
                    .as("metadata SHOW %s requires current base SELECT", kind).contains("SECURITY-0002");
        }
    }
}
