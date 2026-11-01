package org.pangrova.query.assembly.state;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal;
import org.pangrova.persistence.jdbc.query.state.JdbcQueryStateJournal;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.pangrova.spi.query.state.JournalEventContext;
import org.pangrova.spi.connector.coordination.JournalWriteException;
import org.pangrova.spi.query.state.QueryStateEvent;
import org.pangrova.persistence.api.query.state.QueryStateJournal;
import org.pangrova.spi.query.state.StateJournalConfig;

import static org.junit.jupiter.api.Assertions.*;

/** Factory routing uses actual instance-owned NONE and production-migrated PostgreSQL owners. */
@Testcontainers
class QueryStateJournalFactoryTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine").withDatabaseName("pangrova_test");
    @Test void noneTypeProducesIndependentInMemoryJournal() {
        try (QueryStateJournal journal = QueryStateJournalFactory.create(StateJournalConfig.NONE, null);
                QueryStateJournal other = QueryStateJournalFactory.create(StateJournalConfig.NONE, null)) {
            assertInstanceOf(InMemoryQueryStateJournal.class, journal);
            assertNotSame(other, journal);
            QueryStateEvent event = event("q-1");
            journal.appendQueryEventSync(event);
            assertEquals(event, journal.latestQueryEvent("q-1").orElseThrow());
            assertTrue(other.replayActiveQueries().isEmpty());
        }
    }

    @Test @Tag("testcontainers") void jdbcTypeProducesActualJdbcJournal() throws Exception {
        var source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        var dataSource = (javax.sql.DataSource) source;
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS query_state_journal (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        event_id UUID NOT NULL UNIQUE,
                        query_id VARCHAR(64) NOT NULL,
                        sql_text TEXT,
                        session_json TEXT,
                        state VARCHAR(32) NOT NULL,
                        timestamp_ms BIGINT NOT NULL,
                        coordinator_id VARCHAR(128),
                        epoch BIGINT NOT NULL DEFAULT 0,
                        lineage_payload JSONB
                    )""");
        }
        StateJournalConfig config = new StateJournalConfig("JDBC", "postgresql-production-schema", 10, 2, 24, 4);
        try (QueryStateJournal journal = QueryStateJournalFactory.create(config, dataSource)) {
            assertInstanceOf(JdbcQueryStateJournal.class, journal);
            QueryStateEvent event = event("q-1");
            journal.appendQueryEventSync(event);
            assertEquals(event, journal.latestQueryEvent("q-1").orElseThrow());
            try (var connection = dataSource.getConnection();
                    var statement = connection.prepareStatement("SELECT event_id FROM query_state_journal")) {
                try (var rows = statement.executeQuery()) {
                    assertTrue(rows.next());
                    assertEquals(event.context().eventId(), rows.getObject(1, java.util.UUID.class));
                    assertFalse(rows.next());
                }
            }
        }
    }

    @Test void jdbcTypeWithoutDataSourceThrows() {
        StateJournalConfig config = new StateJournalConfig("JDBC", "jdbc:postgresql://localhost/pangrova", 100, 50, 24, 50_000);
        assertThrows(IllegalArgumentException.class, () -> QueryStateJournalFactory.create(config, null));
    }

    @Test void unknownTypeThrows() {
        StateJournalConfig config = new StateJournalConfig("REDIS", null, 100, 50, 24, 50_000);
        assertThrows(IllegalArgumentException.class, () -> QueryStateJournalFactory.create(config, null));
    }

    @Test void nullConfigThrows() {
        assertThrows(NullPointerException.class, () -> QueryStateJournalFactory.create(null, null));
    }

    @Test void noneFactoryConsumesConfiguredResidentEventCapacity() {
        StateJournalConfig config = new StateJournalConfig("NONE", null, 100, 50, 24, 1);
        try (QueryStateJournal journal = QueryStateJournalFactory.create(config, null)) {
            journal.appendQueryEventSync(event("q-1"));
            assertThrows(JournalWriteException.class, () -> journal.appendQueryEventSync(event("q-2")));
            assertEquals(1, journal.replayActiveQueries().size());
        }
    }

    private static QueryStateEvent event(String queryId) {
        return new QueryStateEvent(JournalEventContext.create(1000, "coord-1"), queryId, "SELECT 1", "{}", "QUEUED");
    }
}
