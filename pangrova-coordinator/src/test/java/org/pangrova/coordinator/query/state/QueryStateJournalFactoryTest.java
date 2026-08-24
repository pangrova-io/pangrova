package org.pangrova.coordinator.query.state;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.pangrova.metadata.coordination.JdbcQueryStateJournal;
import org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.StateJournalConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that StateJournalConfig type maps to the correct journal implementation.
 * Covers REQ-15: NONE → InMemoryQueryStateJournal, JDBC → JdbcQueryStateJournal.
 */
class QueryStateJournalFactoryTest {

    @Test
    void noneTypeProducesInMemoryJournal() {
        QueryStateJournal journal = QueryStateJournalFactory.create(
                StateJournalConfig.NONE, null);
        assertInstanceOf(InMemoryQueryStateJournal.class, journal);
        assertSame(InMemoryQueryStateJournal.INSTANCE, journal);
    }

    @Test
    void jdbcTypeProducesJdbcJournal() throws Exception {
        StateJournalConfig config = new StateJournalConfig(
                "JDBC", "jdbc:postgresql://localhost:5432/pangrova", 100, 50, 24);
        QueryStateJournal journal = QueryStateJournalFactory.create(config, DUMMY_DS);
        assertInstanceOf(JdbcQueryStateJournal.class, journal);
        // Clean up daemon flush thread
        ((JdbcQueryStateJournal) journal).close();
    }

    @Test
    void jdbcTypeWithoutDataSourceThrows() {
        StateJournalConfig config = new StateJournalConfig(
                "JDBC", "jdbc:postgresql://localhost:5432/pangrova", 100, 50, 24);
        assertThrows(IllegalArgumentException.class, () ->
                QueryStateJournalFactory.create(config, null));
    }

    @Test
    void unknownTypeThrows() {
        StateJournalConfig config = new StateJournalConfig(
                "REDIS", null, 100, 50, 24);
        assertThrows(IllegalArgumentException.class, () ->
                QueryStateJournalFactory.create(config, null));
    }

    @Test
    void nullConfigThrows() {
        assertThrows(NullPointerException.class, () ->
                QueryStateJournalFactory.create(null, null));
    }

    /**
     * Minimal DataSource that satisfies the non-null check in JdbcQueryStateJournal
     * constructor. No actual JDBC connection is established during factory routing test.
     */
    private static final DataSource DUMMY_DS = new DataSource() {
        @Override public Connection getConnection() throws SQLException {
            throw new SQLException("dummy — not connected");
        }
        @Override public Connection getConnection(String u, String p) throws SQLException {
            throw new SQLException("dummy — not connected");
        }
        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public java.util.logging.Logger getParentLogger() {
            return java.util.logging.Logger.getLogger("dummy");
        }
        @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    };
}
