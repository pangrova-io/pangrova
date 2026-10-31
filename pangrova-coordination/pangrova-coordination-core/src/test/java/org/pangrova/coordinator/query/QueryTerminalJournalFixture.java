package org.pangrova.coordinator.query;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import javax.sql.DataSource;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.query.store.jdbc.journal.JdbcQueryStateJournal;
import org.pangrova.query.api.state.StateJournalConfig;
import org.postgresql.ds.PGSimpleDataSource;

/** Real migrated PostgreSQL journal, permission faults and committed-write observations. */
final class QueryTerminalJournalFixture implements AutoCloseable {
    private final DataSource administrator;
    private final String schema;
    private final String role = "journal_owner_" + UUID.randomUUID().toString().replace("-", "");
    private final JdbcQueryStateJournal journal;

    QueryTerminalJournalFixture() throws SQLException {
        administrator = SharedPostgresContainer.migratedDataSource("terminal_owner");
        try (var connection = administrator.getConnection()) { schema = connection.getSchema(); }
        String password = UUID.randomUUID().toString();
        execute("CREATE ROLE " + role + " LOGIN PASSWORD '" + password + "'");
        try {
            installCommittedWriteObserver();
            execute("GRANT USAGE ON SCHEMA " + schema + " TO " + role);
            execute("GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA " + schema + " TO " + role);
            execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA " + schema + " TO " + role);
            var source = new PGSimpleDataSource();
            source.setURL(SharedPostgresContainer.instance().getJdbcUrl());
            source.setCurrentSchema(schema);
            source.setUser(role);
            source.setPassword(password);
            source.setApplicationName(role);
            journal = new JdbcQueryStateJournal(source,
                    new StateJournalConfig("JDBC", source.getURL(), 20, 20, 24, 50_000));
        } catch (SQLException | RuntimeException | Error failure) {
            try { removeRole(); } catch (SQLException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void installCommittedWriteObserver() throws SQLException {
        execute("CREATE TABLE terminal_writes (LIKE query_state_journal)");
        execute("CREATE FUNCTION record_terminal_write() RETURNS trigger LANGUAGE plpgsql AS $$"
                + "BEGIN INSERT INTO " + schema + ".terminal_writes SELECT NEW.*; RETURN NEW; END $$");
        execute("CREATE TRIGGER terminal_write_observation AFTER INSERT ON query_state_journal "
                + "FOR EACH ROW EXECUTE FUNCTION record_terminal_write()");
    }

    JdbcQueryStateJournal journal() { return journal; }
    DataSource administrator() { return administrator; }

    java.util.List<org.pangrova.query.api.state.QueryStateEvent> committedEvents(String queryId) throws SQLException {
        try (var connection = administrator.getConnection(); var statement = connection.prepareStatement(
                "SELECT event_id, timestamp_ms, coordinator_id, query_id, sql_text, session_json, state "
                        + "FROM terminal_writes WHERE query_id = ? ORDER BY id ASC")) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                var result = new java.util.ArrayList<org.pangrova.query.api.state.QueryStateEvent>();
                while (rows.next()) result.add(new org.pangrova.query.api.state.QueryStateEvent(
                        new org.pangrova.query.api.state.JournalEventContext(rows.getObject(1, UUID.class),
                                rows.getLong(2), rows.getString(3)), rows.getString(4), rows.getString(5),
                        rows.getString(6), rows.getString(7)));
                return java.util.List.copyOf(result);
            }
        }
    }

    Connection lockQueryInsert() throws SQLException {
        var connection = administrator.getConnection();
        try {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("LOCK TABLE query_state_journal IN ACCESS EXCLUSIVE MODE");
            }
            return connection;
        } catch (SQLException | RuntimeException | Error failure) {
            try { connection.close(); } catch (SQLException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    boolean insertWaitingOnActualLock() throws SQLException {
        try (var connection = administrator.getConnection(); var statement = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = ? "
                        + "AND wait_event_type = 'Lock' AND query LIKE 'INSERT INTO query_state_journal%')")) {
            statement.setString(1, role);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    void denyQueryInsert() throws SQLException { execute("REVOKE INSERT ON query_state_journal FROM " + role); }
    void allowQueryInsert() throws SQLException { execute("GRANT INSERT ON query_state_journal TO " + role); }
    void denyFinishedQueryInsert() throws SQLException {
        execute("CREATE OR REPLACE FUNCTION reject_finished_query_insert() RETURNS trigger LANGUAGE plpgsql AS $$"
                + "BEGIN IF NEW.state = 'FINISHED' THEN RAISE EXCEPTION 'Actual FINISHED journal insert denied' "
                + "USING ERRCODE = '42501'; END IF; RETURN NEW; END $$");
        execute("CREATE TRIGGER reject_finished_query_insert BEFORE INSERT ON query_state_journal "
                + "FOR EACH ROW EXECUTE FUNCTION reject_finished_query_insert()");
    }
    void allowFinishedQueryInsert() throws SQLException {
        execute("DROP TRIGGER IF EXISTS reject_finished_query_insert ON query_state_journal");
        execute("DROP FUNCTION IF EXISTS reject_finished_query_insert()");
    }
    void denyQueryDelete() throws SQLException { execute("REVOKE DELETE ON query_state_journal FROM " + role); }
    void allowQueryDelete() throws SQLException { execute("GRANT DELETE ON query_state_journal TO " + role); }

    int terminalWrites(String queryId, String state) throws SQLException {
        try (var connection = administrator.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT count(*) FROM terminal_writes WHERE query_id = ? AND state = ?")) {
            statement.setString(1, queryId);
            statement.setString(2, state);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getInt(1); }
        }
    }

    int queryRows(String queryId) throws SQLException {
        try (var connection = administrator.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT count(*) FROM query_state_journal WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getInt(1); }
        }
    }

    Connection lockQueryRows(String queryId) throws SQLException {
        var connection = administrator.getConnection();
        try {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement(
                    "SELECT id FROM query_state_journal WHERE query_id = ? FOR UPDATE")) {
                statement.setString(1, queryId);
                try (var rows = statement.executeQuery()) { while (rows.next()) rows.getLong(1); }
            }
            return connection;
        } catch (SQLException | RuntimeException | Error failure) {
            try { connection.close(); } catch (SQLException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    boolean purgeWaitingOnActualLock() throws SQLException {
        try (var connection = administrator.getConnection();
                var statement = connection.prepareStatement("SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        + "WHERE application_name = ? AND wait_event_type = 'Lock' "
                        + "AND query LIKE 'DELETE FROM query_state_journal%')")) {
            statement.setString(1, role);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }

    private void execute(String sql) throws SQLException {
        try (var connection = administrator.getConnection(); var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void close() throws SQLException {
        allowFinishedQueryInsert();
        journal.close();
        removeRole();
    }

    private void removeRole() throws SQLException {
        execute("DROP OWNED BY " + role);
        execute("DROP ROLE " + role);
    }
}
