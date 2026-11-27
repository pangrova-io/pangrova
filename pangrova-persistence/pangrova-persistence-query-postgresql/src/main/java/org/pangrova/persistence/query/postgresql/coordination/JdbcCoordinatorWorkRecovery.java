package org.pangrova.persistence.query.postgresql.coordination;

import org.pangrova.common.lifecycle.ProcessIncarnationStore;

import javax.sql.DataSource;
import java.sql.SQLException;

/** Publishes local process-exit proof before a replacement Coordinator admits work. */
public final class JdbcCoordinatorWorkRecovery {
    // Lock all still-present anchors of the retired process. A PostgreSQL backend can finish an
    // in-flight reservation after its JVM died. Its UPDATE either precedes this proof or sees an
    // expired anchor after taking its row lock. A lookup of visible work markers would miss that race.
    private static final String PUBLISH_EXIT = """
            UPDATE execution_anchor SET expires_at = LEAST(expires_at, clock_timestamp()),
                version = version + 1,
                coordinator_work_exited = CASE
                    WHEN coordinator_work_domain = ? AND coordinator_work_incarnation = owner_incarnation
                         AND coordinator_work_token = fencing_token THEN TRUE
                    ELSE coordinator_work_exited
                END
            WHERE owner_incarnation = ANY (?)
            """;

    private JdbcCoordinatorWorkRecovery() {}

    /** Makes work from provably retired local incarnations eligible for ordinary fenced recovery. */
    public static void publishRetired(DataSource source, ProcessIncarnationStore processStore) {
        String domain = processStore.identity().ownershipDomainId();
        var retired = processStore.retiredIncarnations();
        if (retired.isEmpty()) return;
        try (var connection = source.getConnection()) {
            var incarnations = connection.createArrayOf("text", retired.toArray(String[]::new));
            try (var statement = connection.prepareStatement(PUBLISH_EXIT)) {
                statement.setString(1, domain);
                statement.setArray(2, incarnations);
                statement.executeUpdate();
            } finally {
                incarnations.free();
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot publish retired Coordinator work authority", failure);
        }
    }
}
