package org.pangrova.persistence.workflow.postgresql.security;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.persistence.table.postgresql.policy.PostgresTablePolicyRepository;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.table.api.enums.GovernanceBindingType;
import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.enums.TableControlProvenance;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.policy.GovernanceBinding;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresReadPolicySnapshotTest {
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void combinedReadKeepsRowIdentityScopeAndTheExactNamedMasks() throws Exception {
        var source = MigratedPostgres.create();
        var rows = new JdbcRowPolicyStore(source);
        var masks = new JdbcColumnMaskStore(source);
        var readPolicies = new PostgresReadPolicySnapshot(source);
        UUID current = register(source);
        rows.savePolicy(policy(current, "old_name", "id > 0", true));
        rows.savePolicy(policy(UUID.randomUUID(), "orders", "id > 10", true));
        rows.savePolicy(policy(null, "orders", "id > 20", true));
        rows.savePolicy(policy(null, "other_table", "id > 30", true));
        rows.savePolicy(new RowPolicy(0, "disabled", "cat", "db", "orders", "id > 40",
                List.of("reader"), false, 50, current));
        masks.saveColumnMask(new ColumnMask(0, "hide_payload", "cat", "db", "orders", "payload",
                "'masked'", List.of("reader"), List.of("admin"), true, current));
        masks.saveColumnMask(new ColumnMask(0, "disabled", "cat", "db", "orders", "id",
                "0", List.of("reader"), List.of(), false, current));
        masks.saveColumnMask(new ColumnMask(0, "other_table", "cat", "db", "other_table", "payload",
                "'hidden'", List.of("reader"), List.of(), true, null));

        var expected = new GovernanceBinding(UUID.randomUUID(), current, GovernanceBindingType.COLUMN_TAG,
                "email", Map.of("column", "email", "tagKey", "pii_type", "value", "PII_EMAIL"),
                "alice", NOW, NOW);
        var bindings = new PostgresTablePolicyRepository(source);
        bindings.save(expected);
        bindings.save(new GovernanceBinding(UUID.randomUUID(), UUID.randomUUID(), GovernanceBindingType.COLUMN_TAG,
                "foreign", Map.of(), "alice", NOW, NOW));

        var managed = readPolicies.read("cat", "db", "orders");
        assertThat(managed.identity()).get().extracting(PostgresPolicyTableIdentity.VerifiedTable::tableId)
                .isEqualTo(current);
        assertThat(managed.bindings()).containsExactly(expected);
        assertThat(managed.rows()).containsExactlyElementsOf(
                rows.getPoliciesForCurrentTable("cat", "db", "orders", current));
        assertThat(managed.rows()).hasSize(2);
        assertThat(managed.masks()).containsExactlyElementsOf(masks.getTableMasks("cat", "db", "orders"));
        assertThat(managed.masks()).hasSize(1);

        execute(source, "DELETE FROM table_control_name_reservation WHERE table_id='" + current + "'");
        var external = readPolicies.read("cat", "db", "orders");
        assertThat(external.identity()).isEmpty();
        assertThat(external.bindings()).isEmpty();
        assertThat(external.rows()).extracting(RowPolicy::filterExpression).containsExactly("id > 20");
        assertThat(external.masks()).isEqualTo(managed.masks());
        var absent = readPolicies.read("cat", "db", "absent");
        assertThat(absent.rows()).isEmpty();
        assertThat(absent.masks()).isEmpty();
    }

    @Test
    void aChangedCatalogRevisionFailsClosed() throws Exception {
        var source = MigratedPostgres.create();
        register(source);
        execute(source, "UPDATE catalog_definitions SET revision=8 WHERE name='cat'");

        assertThatThrownBy(() -> new PostgresReadPolicySnapshot(source).read("cat", "db", "orders"))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    private static UUID register(DataSource source) throws SQLException {
        String incarnation = UUID.randomUUID().toString();
        try (var connection = source.getConnection(); var statement = connection.prepareStatement("""
                INSERT INTO catalog_definitions (name,type,provider,state,properties,incarnation_id,revision)
                VALUES ('cat','CONNECTOR','iceberg','ACTIVE','{}',?,7)
                """)) {
            statement.setString(1, incarnation);
            assertThat(statement.executeUpdate()).isOne();
        }
        UUID id = UUID.randomUUID();
        new JdbcTableControlStore(source).save(TableControlRecord.builder()
                .tableId(id).name("cat", "db", "orders").format("ICEBERG")
                .formatTableUuid("uuid-orders").formatLocation("s3://bucket/orders")
                .ownershipMode(OwnershipMode.MANAGED).state(TableControlState.ACTIVE)
                .provenance(TableControlProvenance.CREATED).catalogIncarnationId(incarnation).catalogRevision(7L)
                .version("ICEBERG:uuid-orders:7", NOW).createdAt(NOW).updatedAt(NOW).build());
        return id;
    }

    private static void execute(DataSource source, String sql) throws SQLException {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private static RowPolicy policy(UUID tableId, String table, String expression, boolean enabled) {
        return new RowPolicy(0, "rows", "cat", "db", table, expression, List.of("reader"), enabled, 1, tableId);
    }
}
