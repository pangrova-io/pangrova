package org.pangrova.persistence.coordination.postgresql.membership;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;
import org.pangrova.persistence.coordination.api.error.CoordinationStorageException;
import java.time.Duration;
import java.sql.SQLException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcCoordinatorRegistryFailureTest extends ContainerTestBase {
    @Test void missingRequiredProgressColumnIsNotLegacyUnknownProgress() throws Exception {
        var source = MigratedPostgres.create();
        var registry = new JdbcCoordinatorRegistry(source);
        long now = System.currentTimeMillis();
        registry.register(new CoordinatorSnapshot("node", "host", 8080, "LEADER", 1, now, now, 0));
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE coordinator_registry DROP COLUMN last_processed_invalidation_id");
        }
        for (Runnable read : java.util.List.<Runnable>of(() -> registry.getCoordinator("node"),
                registry::getLeader, () -> registry.getAllCoordinators(Duration.ofSeconds(30)))) {
            assertThatThrownBy(read::run).isInstanceOf(CoordinationStorageException.class)
                    .hasCauseInstanceOf(SQLException.class);
        }
    }
}
