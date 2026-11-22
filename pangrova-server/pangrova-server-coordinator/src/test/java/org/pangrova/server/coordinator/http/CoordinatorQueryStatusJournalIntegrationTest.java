package org.pangrova.server.coordinator.http;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.persistence.query.postgresql.state.JdbcQueryStateJournal;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.server.coordinator.http.CoordinatorQueryStatusWaitHttpTest.status;

class CoordinatorQueryStatusJournalIntegrationTest extends ContainerTestBase {
    @Test
    void versionedOwnerReadsAndWrongPeerMissesNeverBorrowAJournalConnection() throws Exception {
        var source = journalDataSource();
        var config = new StateJournalConfig("JDBC", source.getURL(), 20, 20, 24, 1000);
        try (var journal = new JdbcQueryStateJournal(source, config);
             var owner = new CoordinatorQueryStatusWaitHttpTest.Peer(journal);
             var peerJournal = new JdbcQueryStateJournal(source, config);
             var peer = new CoordinatorQueryStatusWaitHttpTest.Peer(peerJournal)) {
            String query = UUID.randomUUID().toString();
            String observer = UUID.randomUUID().toString();
            journal.appendQueryEventSync(new QueryStateEvent(
                    JournalEventContext.create(System.currentTimeMillis(), "owner"), query, null, null, "RUNNING"));
            var state = owner.running(query);
            int baseline = source.borrows.get();
            for (int i = 0; i < 16; i++) {
                assertThat(status(peer.get("/v1/query/" + query + "/state")).state()).isEqualTo("RUNNING");
            }
            assertThat(source.borrows).hasValue(baseline + 16);
            for (int i = 0; i < 16; i++) {
                var observed = status(owner.waitFor(query, state.status().version(), 10, observer)
                        .get(3, TimeUnit.SECONDS));
                assertThat(observed.version()).isEqualTo(state.status().version());
                assertThat(peer.waitFor(query, observed.version(), 10, observer)
                        .get(3, TimeUnit.SECONDS).statusCode()).isEqualTo(404);
            }
            assertThat(source.borrows).hasValue(baseline + 16);
        }
    }

    private static CountingDataSource journalDataSource() throws SQLException {
        var schemaOwner = SharedPostgresContainer.migratedDataSource("coordinator_status_reads");
        var postgres = SharedPostgresContainer.instance();
        var source = new CountingDataSource();
        source.setURL(postgres.getJdbcUrl());
        source.setUser(postgres.getUsername());
        source.setPassword(postgres.getPassword());
        try (var connection = schemaOwner.getConnection()) {
            source.setCurrentSchema(connection.getSchema());
        }
        return source;
    }

    private static final class CountingDataSource extends PGSimpleDataSource {
        private final AtomicInteger borrows = new AtomicInteger();

        @Override
        public Connection getConnection() throws SQLException {
            borrows.incrementAndGet();
            return super.getConnection();
        }
    }
}
