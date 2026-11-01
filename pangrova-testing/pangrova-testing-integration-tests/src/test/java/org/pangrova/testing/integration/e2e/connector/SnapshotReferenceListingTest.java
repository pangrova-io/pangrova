package org.pangrova.testing.integration.e2e.connector;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real HTTP SQL/JNI listing, connector schemas, mutations and structured table identities. */
class SnapshotReferenceListingTest extends SqlE2ETestBase {
    private static final String ICEBERG = "listing_iceberg";
    private static final String PAIMON = "listing_paimon";
    private String catalog;
    private String name;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(ICEBERG, "default", icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PAIMON, "default", paimonWarehouse().toString()));
    }

    private void create(String selectedCatalog, boolean quoted) {
        catalog = selectedCatalog;
        name = (quoted ? "Orders.中文\"; select_" : "orders_") + UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT"
                + (catalog.equals(PAIMON) ? ", PRIMARY KEY (id) NOT ENFORCED" : "") + ")");
    }

    private String table() { return catalog + ".default.\"" + name.replace("\"", "\"\"") + "\""; }

    @AfterEach
    void drainAndDrop() {
        try {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(queryManager.getActiveQueryIds()).isEmpty();
                assertThat(queryManager.getActiveQueryCount()).isZero();
            });
        } finally {
            if (name != null) sqlRunner.executeDdl("DROP TABLE IF EXISTS " + table());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {ICEBERG, PAIMON})
    void emptyTableReturnsEmptyReferenceLists(String selectedCatalog) {
        create(selectedCatalog, false);
        assertThat(sqlRunner.execute("SHOW TAGS FROM " + table())).isEmpty();
        assertThat(sqlRunner.execute("SHOW BRANCHES ON " + table())).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {ICEBERG, PAIMON})
    void createDropAndMovingMainMatchTheNativeMetadataRelations(String selectedCatalog) {
        create(selectedCatalog, false);
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1)");
        sqlRunner.executeDdl("ALTER TABLE " + table() + " CREATE TAG release_tag");
        sqlRunner.executeDdl("ALTER TABLE " + table() + " CREATE BRANCH audit_branch");
        var tags = sqlRunner.execute("SHOW TAGS FROM " + table());
        assertThat(tags).singleElement().satisfies(row -> {
            assertThat(row.getString(catalog.equals(ICEBERG) ? "name" : "tag_name")).isEqualTo("release_tag");
            assertThat(row.getLong("snapshot_id")).isPositive();
            assertThat(row.columnNames()).doesNotContain("info");
        });
        long tagged = tags.get(0).getLong("snapshot_id");
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (2)");
        assertThat(sqlRunner.execute("SHOW TAGS FROM " + table()).get(0).getLong("snapshot_id")).isEqualTo(tagged);
        var branches = sqlRunner.execute("SHOW BRANCHES FROM " + table());
        assertThat(branches).extracting(row -> row.getString(catalog.equals(ICEBERG) ? "name" : "branch_name"))
                .contains("audit_branch");
        assertSameNativeListing("TAGS");
        assertSameNativeListing("BRANCHES");
        sqlRunner.executeDdl("ALTER TABLE " + table() + " DROP TAG release_tag");
        sqlRunner.executeDdl("ALTER TABLE " + table() + " DROP BRANCH audit_branch");
        assertThat(sqlRunner.execute("SHOW TAGS FROM " + table())).isEmpty();
        assertThat(sqlRunner.execute("SHOW BRANCHES FROM " + table()))
                .extracting(row -> row.getString(catalog.equals(ICEBERG) ? "name" : "branch_name"))
                .doesNotContain("audit_branch");
    }

    @ParameterizedTest
    @ValueSource(strings = {ICEBERG, PAIMON})
    void quotedNamesRemainOneIdentityThroughMetadataPlanning(String selectedCatalog) {
        create(selectedCatalog, true);
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1)");
        sqlRunner.executeDdl("ALTER TABLE " + table() + " CREATE TAG release_tag");
        assertThat(sqlRunner.execute("SHOW TAGS FROM " + table()))
                .singleElement().satisfies(row -> assertThat(row.getString(
                        catalog.equals(ICEBERG) ? "name" : "tag_name")).isEqualTo("release_tag"));
    }

    private void assertSameNativeListing(String kind) {
        String suffix = catalog.equals(ICEBERG) ? "refs" : kind.toLowerCase(java.util.Locale.ROOT);
        String relation = catalog + ".default." + name + "$" + suffix;
        String predicate = catalog.equals(ICEBERG) ? " WHERE type = '" + (kind.equals("TAGS") ? "TAG" : "BRANCH")
                + "'" : "";
        var nativeRows = sqlRunner.execute("SELECT * FROM " + relation + predicate);
        var showRows = sqlRunner.execute("SHOW " + kind + " FROM " + table());
        assertThat(showRows.stream().map(TypedRow::values).toList())
                .containsExactlyInAnyOrderElementsOf(nativeRows.stream().map(TypedRow::values).toList());
    }
}
