package org.pangrova.persistence.security.postgresql.schema;

import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.postgresql.audit.JdbcAuditEventWriter;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.postgres.SharedPostgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL initialization, direct-SQL authority and borrowed transaction checks. */
class CanonicalSecuritySchemaTest {
    @Test
    void freshBundleInstallsEveryAuthorityGateBeforeAnyStoreIsConstructed() throws Exception {
        var source = MigratedPostgres.create();
        try (var connection = source.getConnection(); var query = connection.prepareStatement(
                "SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                        + "JOIN pg_namespace n ON n.oid=c.relnamespace "
                        + "WHERE n.nspname=current_schema() AND t.tgname='pangrova_authority_gate'");
                var rows = query.executeQuery()) {
            var names = new java.util.ArrayList<String>();
            while (rows.next()) names.add(rows.getString(1));
            assertThat(names).containsExactlyInAnyOrderElementsOf(List.of(
                    "pangrova_roles", "pangrova_user_roles", "pangrova_group_roles", "security_grants",
                    "row_policy", "column_mask", "tenants", "tenant_user_memberships",
                    "pangrova_embedding_models", "storage_profile_definitions", "file_space_registry",
                    "table_control_governance_binding"));
        }
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("INSERT INTO pangrova_roles(role_name) VALUES ('reader')");
            statement.execute("INSERT INTO pangrova_user_roles(user_id,role_name) VALUES ('alice','reader')");
            statement.execute("INSERT INTO pangrova_group_roles(group_name,role_name) VALUES ('team','reader')");
            statement.execute("INSERT INTO security_grants(role_name,privilege,object_type) VALUES ('reader','SELECT','TABLE')");
            statement.execute("DELETE FROM pangrova_roles WHERE role_name='reader'");
            for (String table : List.of("pangrova_user_roles", "pangrova_group_roles", "security_grants")) {
                try (var rows = statement.executeQuery("SELECT count(*) FROM " + table)) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getLong(1)).isZero();
                }
            }
            assertThatThrownBy(() -> statement.execute("INSERT INTO security_grants "
                    + "(role_name,privilege,object_type,catalog_name) VALUES "
                    + "('reader','READ_STORAGE_PROFILE','STORAGE_PROFILE','missing')"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("existing exact profile");
            assertThatThrownBy(() -> statement.execute("TRUNCATE row_policy"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cannot be truncated");
        }
    }

    @Test
    void storesCannotCreateTablesInAnUninitializedDatabase() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        assertThatThrownBy(() -> new JdbcRoleStore(source).listRoles())
                .isInstanceOf(IllegalStateException.class).hasCauseInstanceOf(SQLException.class);
        assertThatThrownBy(() -> new JdbcAuditEventWriter(source))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("canonical migration bundle");
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT to_regclass('pangrova_roles'),to_regclass('audit_logs')")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isNull();
            assertThat(rows.getString(2)).isNull();
        }
    }

    @Test
    void publicationFenceBlocksDirectSqlMutationUntilCallerEndsTransaction() throws Exception {
        var source = MigratedPostgres.create();
        try (var publisher = source.getConnection(); var mutation = source.getConnection()) {
            publisher.setAutoCommit(false);
            mutation.setAutoCommit(false);
            AuthorizationTransactionGate.lockPublication(publisher);
            try (var statement = mutation.createStatement()) {
                statement.execute("SET LOCAL lock_timeout='100ms'");
                assertThatThrownBy(() -> statement.execute("INSERT INTO pangrova_roles(role_name) VALUES ('blocked')"))
                        .isInstanceOfSatisfying(SQLException.class, failure -> assertThat(failure.getSQLState()).isEqualTo("55P03"));
            }
            mutation.rollback();
            publisher.commit();
            try (var statement = mutation.createStatement()) {
                statement.execute("INSERT INTO pangrova_roles(role_name) VALUES ('allowed')");
            }
            mutation.commit();
            assertThat(new JdbcRoleStore(source).listRoles()).extracting(role -> role.name()).containsExactly("allowed");
        }
    }
}
