package org.pangrova.persistence.coordination.postgresql.admission;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.postgres.SharedPostgres;

import static org.assertj.core.api.Assertions.*;

class JdbcClusterConcurrencySlotsInitializationTest extends ContainerTestBase {
    @Test
    void providerCannotCreateAnAlternativeSchemaWhenInitializationIsMissing() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        assertThatThrownBy(() -> new JdbcClusterConcurrencySlots(source))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("canonical schema");
        try (var connection = source.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT COUNT(*) FROM information_schema.tables "
                     + "WHERE table_schema=current_schema() AND table_name LIKE 'tenant_concurrency_%'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isZero();
        }
    }

    @Test
    void canonicalInitializationAndReopeningPreserveExactLiveReservations() throws Exception {
        var source = MigratedPostgres.create();
        var first = new JdbcClusterConcurrencySlots(source);
        assertThat(first.tryReserve("tenant", "query", 1)).isTrue();
        var reopened = new JdbcClusterConcurrencySlots(source);
        assertThat(reopened.active("tenant")).isEqualTo(1);
        assertThat(reopened.tryReserve("tenant", "query", 1)).isTrue();
        assertThat(reopened.tryReserve("tenant", "another-query", 1)).isFalse();
        reopened.release("wrong-tenant", "query");
        assertThat(first.active("tenant")).isEqualTo(1);
        reopened.release("tenant", "query");
        assertThat(first.active("tenant")).isZero();
    }
}
