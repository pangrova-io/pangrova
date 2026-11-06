package org.pangrova.persistence.indexing.postgresql.metadata;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.lifecycle.IndexState;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.persistence.jdbc.support.JdbcSupport;

class JdbcSupportPostgresTest {

    @Test
    void preservesOneInstantAcrossConnectionsWithDifferentTimeZones() throws Exception {
        DataSource dataSource = SharedPostgresContainer.isolatedDataSource("utc_instant");
        Instant source = Instant.parse("2026-09-19T04:05:06.123456789Z");
        Instant expected = Instant.parse("2026-09-19T04:05:06.123456Z");

        try (Connection utc = dataSource.getConnection()) {
            setTimeZone(utc, "UTC");
            try (var statement = utc.createStatement()) {
                statement.execute("CREATE TABLE absolute_time_test ("
                        + "id INT PRIMARY KEY, recorded_at TIMESTAMPTZ(6))");
            }
            try (var statement = utc.prepareStatement(
                    "INSERT INTO absolute_time_test (id,recorded_at) VALUES (?,?)")) {
                statement.setInt(1, 1);
                JdbcSupport.setInstant(statement, 2, source);
                statement.executeUpdate();
                statement.setInt(1, 2);
                JdbcSupport.setNullableInstant(statement, 2, null);
                statement.executeUpdate();
            }
        }

        try (Connection losAngeles = dataSource.getConnection()) {
            setTimeZone(losAngeles, "America/Los_Angeles");
            try (var statement = losAngeles.prepareStatement(
                    "SELECT recorded_at FROM absolute_time_test WHERE id=1");
                    var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(JdbcSupport.getInstant(result, "recorded_at")).isEqualTo(expected);
                assertThat(JdbcSupport.getInstant(result, 1)).isEqualTo(expected);
            }
            try (var statement = losAngeles.prepareStatement(
                    "SELECT recorded_at FROM absolute_time_test WHERE id=2");
                    var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(JdbcSupport.getInstant(result, "recorded_at")).isNull();
            }
        }
    }

    @Test
    void leaseAndAnchorInstantsSurviveCrossConnectionTimeZones() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("lease_time_zones");
        Instant acquiredAt = Instant.parse("2026-09-19T04:05:06.123456Z");
        Instant expiresAt = Instant.parse("2026-09-19T04:06:06.654321Z");

        try (Connection utc = dataSource.getConnection()) {
            setTimeZone(utc, "UTC");
            try (var statement = utc.prepareStatement(
                    "INSERT INTO distributed_lease "
                            + "(lease_key,holder_id,epoch,acquired_at,expires_at,version) "
                            + "VALUES ('lease-zone','owner',1,?,?,1)")) {
                JdbcSupport.setInstant(statement, 1, acquiredAt);
                JdbcSupport.setInstant(statement, 2, expiresAt);
                statement.executeUpdate();
            }
            try (var statement = utc.prepareStatement(
                    "INSERT INTO index_maintenance_lease "
                            + "(lease_key,holder_id,acquired_at,expires_at) "
                            + "VALUES ('index-zone','owner',?,?)")) {
                JdbcSupport.setInstant(statement, 1, acquiredAt);
                JdbcSupport.setInstant(statement, 2, expiresAt);
                statement.executeUpdate();
            }
            try (var statement = utc.prepareStatement(
                    "INSERT INTO execution_anchor "
                            + "(execution_id,owner_id,owner_incarnation,fencing_token,"
                            + "acquired_at,expires_at,version) "
                            + "VALUES ('execution-zone','owner','owner-1',1,?,?,1)")) {
                JdbcSupport.setInstant(statement, 1, acquiredAt);
                JdbcSupport.setInstant(statement, 2, expiresAt);
                statement.executeUpdate();
            }
        }

        try (Connection losAngeles = dataSource.getConnection()) {
            setTimeZone(losAngeles, "America/Los_Angeles");
            assertStoredInstants(losAngeles, "distributed_lease", "lease_key", "lease-zone",
                    acquiredAt, expiresAt);
            assertStoredInstants(losAngeles, "index_maintenance_lease", "lease_key", "index-zone",
                    acquiredAt, expiresAt);
            assertStoredInstants(losAngeles, "execution_anchor", "execution_id", "execution-zone",
                    acquiredAt, expiresAt);
        }
    }

    @Test
    void indexDescriptorFenceSurvivesCrossConnectionTimeZones() throws Exception {
        DataSource base = SharedPostgresContainer.migratedDataSource("index_fence_zones");
        DataSource utc = inTimeZone(base, "UTC");
        DataSource losAngeles = inTimeZone(base, "America/Los_Angeles");
        Instant initial = Instant.parse("2026-09-19T04:05:06.123456789Z");
        Instant initialExpected = Instant.parse("2026-09-19T04:05:06.123456Z");
        Instant next = Instant.parse("2026-09-19T04:06:06.987654321Z");
        Instant nextExpected = Instant.parse("2026-09-19T04:06:06.987654Z");

        new JdbcIndexMetadataStore(utc, org.pangrova.metadata.service.TableControlDeferredPolicy::requireCurrent).registerIndex(new IndexDescriptor(
                "cross-zone-index", "cross_zone", "lake", "sales", "orders",
                List.of("embedding"), IndexType.VECTOR, Map.of(),
                IndexState.BUILDING, null, 0, "snapshot-1", null,
                initial, initial, "tenant-a"));

        JdbcIndexMetadataStore losAngelesStore = new JdbcIndexMetadataStore(losAngeles, org.pangrova.metadata.service.TableControlDeferredPolicy::requireCurrent);
        IndexDescriptor observed = losAngelesStore.findIndex("cross-zone-index").orElseThrow();
        assertThat(observed.createdAt()).isEqualTo(initialExpected);
        assertThat(observed.updatedAt()).isEqualTo(initialExpected);
        assertThat(losAngelesStore.updateIndexState(
                observed.indexId(), IndexState.READY.name(), next, observed.updatedAt()))
                .isTrue();

        assertThat(new JdbcIndexMetadataStore(utc, org.pangrova.metadata.service.TableControlDeferredPolicy::requireCurrent).findIndex(observed.indexId()).orElseThrow())
                .satisfies(updated -> {
                    assertThat(updated.state()).isEqualTo(IndexState.READY);
                    assertThat(updated.updatedAt()).isEqualTo(nextExpected);
                });
    }

    @Test
    void migrationsUseMicrosecondTimeZoneAwareAbsoluteTimes() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("absolute_columns");
        List<Column> columns = List.of(
                new Column("distributed_lease", "acquired_at"),
                new Column("distributed_lease", "expires_at"),
                new Column("index_maintenance_lease", "acquired_at"),
                new Column("index_maintenance_lease", "expires_at"),
                new Column("index_descriptors", "created_at"),
                new Column("index_descriptors", "updated_at"),
                new Column("index_generation_manifests", "published_at"),
                new Column("index_generation_builds", "started_at"),
                new Column("index_generation_builds", "updated_at"),
                new Column("artifact_build_jobs", "publication_lease_expires_at"),
                new Column("artifact_build_jobs", "created_at"),
                new Column("artifact_build_jobs", "updated_at"),
                new Column("artifact_build_jobs", "publication_expected_updated_at"),
                new Column("artifact_build_jobs", "published_at"),
                new Column("artifact_build_pages", "lease_expires_at"),
                new Column("artifact_build_pages", "next_available_at"),
                new Column("artifact_build_pages", "created_at"),
                new Column("artifact_build_pages", "updated_at"),
                new Column("execution_anchor", "acquired_at"),
                new Column("execution_anchor", "expires_at"),
                new Column("generated_value_artifacts", "created_at"),
                new Column("generated_value_artifacts", "updated_at"),
                new Column("generated_value_generation_manifests", "published_at"),
                new Column("semantic_view_versions", "created_at"),
                new Column("semantic_view_active_versions", "published_at"),
                new Column("evaluation_dataset_versions", "created_at"),
                new Column("evaluation_runs", "started_at"),
                new Column("evaluation_runs", "completed_at"),
                new Column("evaluation_release_decisions", "decided_at"));

        try (Connection connection = dataSource.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT data_type,datetime_precision FROM information_schema.columns "
                                + "WHERE table_schema=current_schema() "
                                + "AND table_name=? AND column_name=?")) {
            for (Column column : columns) {
                statement.setString(1, column.table());
                statement.setString(2, column.name());
                try (var result = statement.executeQuery()) {
                    assertThat(result.next())
                            .as("%s.%s exists", column.table(), column.name())
                            .isTrue();
                    assertThat(result.getString("data_type"))
                            .as("%s.%s type", column.table(), column.name())
                            .isEqualTo("timestamp with time zone");
                    assertThat(result.getInt("datetime_precision"))
                            .as("%s.%s precision", column.table(), column.name())
                            .isEqualTo(6);
                }
            }
        }
    }

    private static void setTimeZone(Connection connection, String timeZone) throws Exception {
        try (var statement = connection.prepareStatement(
                "SELECT set_config('TimeZone', ?, false)")) {
            statement.setString(1, timeZone);
            statement.executeQuery();
        }
    }

    private static void assertStoredInstants(
            Connection connection,
            String table,
            String idColumn,
            String id,
            Instant acquiredAt,
            Instant expiresAt) throws Exception {
        String sql = "SELECT acquired_at,expires_at FROM " + table + " WHERE " + idColumn + "=?";
        try (var statement = connection.prepareStatement(sql)) {
            statement.setString(1, id);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(JdbcSupport.getInstant(result, "acquired_at")).isEqualTo(acquiredAt);
                assertThat(JdbcSupport.getInstant(result, "expires_at")).isEqualTo(expiresAt);
            }
        }
    }

    private static DataSource inTimeZone(DataSource source, String timeZone) {
        PGSimpleDataSource original = (PGSimpleDataSource) source;
        PGSimpleDataSource configured = new PGSimpleDataSource();
        configured.setURL(original.getURL());
        configured.setUser(original.getUser());
        configured.setPassword(original.getPassword());
        configured.setCurrentSchema(original.getCurrentSchema());
        configured.setOptions("-c TimeZone=" + timeZone);
        return configured;
    }

    private record Column(String table, String name) {}
}
