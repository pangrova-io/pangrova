package org.pangrova.coordinator.query.state;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.query.core.JdbcQueryStateJournal;
import org.pangrova.spi.coordination.JournalEventContext;
import org.pangrova.spi.coordination.JournalWriteException;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.StateJournalConfig;

import static org.junit.jupiter.api.Assertions.*;

/** Factory routing uses actual instance-owned NONE and production-migrated PostgreSQL owners. */
class QueryStateJournalFactoryTest {
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
        var dataSource = SharedPostgresContainer.migratedDataSource("journal_factory");
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
