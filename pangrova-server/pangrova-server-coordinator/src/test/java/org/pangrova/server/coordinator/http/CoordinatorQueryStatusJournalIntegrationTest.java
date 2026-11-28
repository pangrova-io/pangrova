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
            recordAdmission(journal, query, "anonymous");
            journal.appendQueryEventSync(new QueryStateEvent(
                    JournalEventContext.create(System.currentTimeMillis(), "owner"), query, null, null, "RUNNING"));
            var state = owner.running(query);
            String foreignQuery = UUID.randomUUID().toString();
            recordAdmission(journal, foreignQuery, "other-principal");
            int baseline = source.borrows.get();
            assertThat(peer.get("/v1/query/" + foreignQuery + "/state").statusCode()).isEqualTo(404);
            assertThat(source.borrows).hasValue(baseline);
            for (int i = 0; i < 16; i++) {
                assertThat(peer.get("/v1/query/" + query + "/state").statusCode()).isEqualTo(404);
                assertThat(status(owner.get("/v1/query/" + query + "/state")).state()).isEqualTo("RUNNING");
            }
            assertThat(source.borrows).hasValue(baseline);
            for (int i = 0; i < 16; i++) {
                var observed = status(owner.waitFor(query, state.status().version(), 10, observer)
                        .get(3, TimeUnit.SECONDS));
                assertThat(observed.version()).isEqualTo(state.status().version());
                assertThat(peer.waitFor(query, observed.version(), 10, observer)
                        .get(3, TimeUnit.SECONDS).statusCode()).isEqualTo(404);
            }
            assertThat(source.borrows).hasValue(baseline);

            assertThat(peer.get("/v1/query/" + foreignQuery + "/state?history=true").statusCode()).isEqualTo(404);
            assertThat(source.borrows).hasValue(baseline + 1);
            assertThat(status(peer.get("/v1/query/" + query + "/state?history=true")).state()).isEqualTo("RUNNING");
            assertThat(source.borrows).hasValue(baseline + 2);
        }
    }

    private static void recordAdmission(JdbcQueryStateJournal journal, String queryId, String principal)
            throws Exception {
        var payload = new org.pangrova.query.api.session.JournalSessionPayload(queryId, "", principal);
        String encoded = org.pangrova.common.json.JsonMapperFactory.writer().writeValueAsString(payload);
        journal.appendQueryEventSync(new QueryStateEvent(
                JournalEventContext.create(System.currentTimeMillis(), "owner"), queryId, null, encoded, "QUEUED"));
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
